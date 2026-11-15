"""R (tidyverse / dplyr pipelines, base R basics) -> PySpark.

A small tokenizer + Pratt parser for R expressions, then emitters for dplyr verbs.
"""
from __future__ import annotations

import ast
import re

from .core import Report, Unsupported, format_chain, strip_parens, cm

# ------------------------------------------------------------------ lexer
OPS3 = ["%/%", "%>%", "%in%", "%%", "|>", "<-", "->", "<=", ">=", "==", "!=", "&&", "||", "::"]
TOK = re.compile(r"""
    (?P<ws>[ \t\r]+) | (?P<comment>\#[^\n]*) | (?P<nl>\n)
  | (?P<num>\d+\.?\d*(?:[eE][+-]?\d+)?L?)
  | (?P<str>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')
  | (?P<bt>`[^`]*`)
  | (?P<id>[A-Za-z_.][A-Za-z0-9_.]*)
  | (?P<pct>%[^%\s]*%)
  | (?P<op><-|->|<=|>=|==|!=|&&|\|\||\|>|::|[-+*/^<>!&|~(),=$\[\]{}:;@])
""", re.X)
INFIX_END = {"%>%", "|>", "+", "-", "*", "/", ",", "<-", "=", "&", "|", "&&", "||", "~", "==", "!=", "<", ">", "<=", ">=", "(", "$", "^"}


def lex(src):
    toks, depth = [], 0
    for m in TOK.finditer(src):
        k, v = m.lastgroup, m.group()
        if k in ("ws", "comment"):
            continue
        if k == "bt":
            k, v = "id", v[1:-1]
        if k == "op" and v in "([{":
            depth += 1
        elif k == "op" and v in ")]}":
            depth -= 1
        if k == "nl":
            if depth > 0 or (toks and (toks[-1][1] in INFIX_END or toks[-1][0] == "pct") and toks[-1][0] != "str"):
                continue
            toks.append(("semi", "\n", m.start()))
            continue
        toks.append((k, v, m.start()))
    return toks


# ------------------------------------------------------------------ parser
BP = {"~": 5, "||": 10, "|": 10, "&&": 12, "&": 12, "==": 20, "!=": 20, "<": 20, ">": 20, "<=": 20, ">=": 20,
      "+": 30, "-": 30, "*": 40, "/": 40, "%in%": 45, "%%": 45, "%/%": 45, "%>%": 50, "|>": 50, ":": 55, "^": 60}


class P:
    def __init__(self, toks):
        self.t, self.i = toks, 0

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else ("eof", "", -1)

    def next(self):
        t = self.peek()
        self.i += 1
        return t

    def expect(self, v):
        t = self.next()
        if t[1] != v:
            raise Unsupported(f"R parse error: expected {v!r}, got {t[1]!r}")

    def expr(self, rbp=0):
        left = self.prefix()
        while True:
            k, v, _ = self.peek()
            op = v if k in ("op", "pct") else None
            if op in ("(",):  # call handled in prefix
                break
            if op not in BP or BP[op] <= rbp:
                break
            self.next()
            right = self.expr(BP[op] - (1 if op == "^" else 0))
            left = ("pipe", left, right) if op in ("%>%", "|>") else ("bin", op, left, right)
        return left

    def prefix(self):
        k, v, _ = self.next()
        if k == "num":
            return ("num", v.rstrip("L"))
        if k == "str":
            return ("str", v[1:-1].encode().decode("unicode_escape"))
        if k == "op" and v == "(":
            e = self.expr()
            self.expect(")")
            return ("paren", e)
        if k == "op" and v in ("-", "+", "!"):
            return ("un", v, self.expr(55 if v != "!" else 15))
        if k == "op" and v == "~":
            return ("formula", None, self.expr(5))
        if k == "id":
            if self.peek()[1] == "::":
                self.next()
                k, v, _ = self.next()
            node = ("id", v)
            while True:
                nk, nv, _ = self.peek()
                if nv == "(" and nk == "op":
                    self.next()
                    node = ("call", node[1] if node[0] == "id" else None, self.args())
                elif nv == "$":
                    self.next()
                    node = ("dollar", node, self.next()[1])
                else:
                    break
            return node
        raise Unsupported(f"R parse error near {v!r}")

    def args(self):
        out = []
        while self.peek()[1] != ")":
            if self.peek()[0] == "id" and self.t[self.i + 1][1] == "=" if self.i + 1 < len(self.t) else False:
                name = self.next()[1]
                self.next()
                out.append((name, self.expr(0)))
            elif self.peek()[0] == "str" and self.i + 1 < len(self.t) and self.t[self.i + 1][1] == "=":
                name = self.next()[1][1:-1]
                self.next()
                out.append((name, self.expr(0)))
            else:
                out.append((None, self.expr(0)))
            if self.peek()[1] == ",":
                self.next()
        self.expect(")")
        return out


# ------------------------------------------------------------------ emitter
AGG = {"sum": "sum", "mean": "avg", "median": "median", "sd": "stddev", "var": "variance", "n_distinct": "countDistinct",
       "first": "first", "last": "last"}
SIMPLE = {"abs": "abs", "sqrt": "sqrt", "exp": "exp", "log": "log", "log10": "log10", "log2": "log2", "floor": "floor",
          "ceiling": "ceil", "toupper": "upper", "tolower": "lower", "str_to_upper": "upper", "str_to_lower": "lower",
          "trimws": "trim", "str_trim": "trim", "nchar": "length", "str_length": "length", "year": "year",
          "month": "month", "day": "dayofmonth", "hour": "hour", "minute": "minute", "second": "second",
          "coalesce": "coalesce", "str_to_title": "initcap"}
CAST = {"as.numeric": "double", "as.double": "double", "as.integer": "int", "as.character": "string",
        "as.logical": "boolean", "as.factor": "string"}
JOIN = {"left_join": "left", "inner_join": "inner", "right_join": "right", "full_join": "full",
        "semi_join": "left_semi", "anti_join": "left_anti"}
CMP = {"==": "==", "!=": "!=", "<": "<", ">": ">", "<=": "<=", ">=": ">="}


class Emitter:
    def __init__(self, report: Report):
        self.r = report
        self.uses_window = False
        self.dfs: set[str] = set()
        self.line = 0
        self.cur_order: str | None = None        # Spark sort expressions of the last arrange() in the current pipeline
        self.var_order: dict[str, str] = {}      # ... remembered per assigned data frame
        self.warned_order = False
        self.warned_part = False

    def warn(self, m):
        self.r.warn(self.line, m)

    # ---- column expressions
    def ex(self, n, mode="plain", keys=None):
        """mode: plain | win (aggregates -> window over keys) | agg (aggregates as-is)"""
        t = n[0]
        e = lambda x: self.ex(x, mode, keys)
        if t == "num":
            return n[1]
        if t == "str":
            return repr(n[1])
        if t == "paren":
            return f"({e(n[1])})"
        if t == "id":
            if n[1] in ("TRUE", "T"):
                return "F.lit(True)"
            if n[1] in ("FALSE", "F"):
                return "F.lit(False)"
            if n[1] in ("NA", "NULL", "NA_character_", "NA_real_", "NA_integer_"):
                return "F.lit(None)"
            if n[1] in ("Inf",):
                return "F.lit(float('inf'))"
            return f"F.col({n[1]!r})"
        if t == "dollar":
            return f"F.col({n[2]!r})"
        if t == "un":
            return {"-": f"(-{e(n[2])})", "+": e(n[2]), "!": f"(~{e(n[2])})"}[n[1]]
        if t == "bin":
            op, l, r = n[1], n[2], n[3]
            if op in CMP:
                return f"({e(l)} {op} {e(r)})"
            if op == "%in%":
                return f"{e(l)}.isin({self.pylist(r)})"
            if op in ("&", "&&"):
                return f"({e(l)} & {e(r)})"
            if op in ("|", "||"):
                return f"({e(l)} | {e(r)})"
            if op == "^":
                return f"F.pow({e(l)}, {e(r)})"
            if op == "%/%":
                return f"F.floor({e(l)} / {e(r)})"
            if op in ("+", "-", "*", "/", "%%"):
                return f"({e(l)} {'%' if op == '%%' else op} {e(r)})"
            raise Unsupported(f"operator {op}")
        if t == "call":
            return self.call(n, mode, keys)
        raise Unsupported("unsupported R expression")

    def pylist(self, n):
        if n[0] == "call" and n[1] == "c":
            return "[" + ", ".join(self.lit(a[1]) for a in n[2]) + "]"
        raise Unsupported("%in% needs a literal c(...) vector")

    def lit(self, n):
        if n[0] == "num":
            return n[1]
        if n[0] == "str":
            return repr(n[1])
        if n[0] == "id":
            return {"TRUE": "True", "FALSE": "False", "NA": "None", "NULL": "None"}.get(n[1], n[1])
        if n[0] == "un" and n[1] == "-":
            return "-" + self.lit(n[2])
        raise Unsupported("expected a literal")

    def over(self, code, mode, keys):
        if mode == "win":
            self.uses_window = True
            if not keys:
                self.warn("aggregate over the whole table in mutate/filter: uses Window.partitionBy() (single partition)")
            return f"{code}.over(Window.partitionBy({', '.join(map(repr, keys or []))}))"
        return code

    def win(self, keys, rows=None, order=None):
        self.uses_window = True
        if order is None:
            order = self.cur_order
        if order is None:
            order = "F.monotonically_increasing_id()"
            if not self.warned_order:
                self.warned_order = True
                self.warn("lag/lead/cumsum/slice depend on row order; no arrange() was seen, so the window uses the current row order "
                          "(monotonically_increasing_id). Add arrange(...) or order_by= if the result must be reproducible")
        if keys:
            w = f"Window.partitionBy({', '.join(map(repr, keys))}).orderBy({order})"
        else:
            w = f"Window.orderBy({order})"
            if not self.warned_part:
                self.warned_part = True
                self.warn("window without group_by: Spark moves all rows to a single partition (slow on big data)")
        if rows:
            w += f".rowsBetween({rows[0]}, {rows[1]})"
        return w

    def call(self, n, mode, keys):
        name, args = n[1], n[2]
        a = [x[1] for x in args]
        kw = {x[0]: x[1] for x in args if x[0]}
        pos = [x[1] for x in args if not x[0]]
        e = lambda x: self.ex(x, mode, keys)
        if name in ("lag", "lead"):
            if mode == "agg":
                raise Unsupported(f"{name}() inside summarise() has no automatic conversion")
            bad = set(kw) - {"n", "default", "order_by"}
            if bad or not pos:
                raise Unsupported(f"{name}({', '.join(sorted(bad))}=...) has no automatic conversion")
            rest = pos[1:]
            k = kw["n"] if "n" in kw else (rest.pop(0) if rest else ("num", "1"))
            d = kw["default"] if "default" in kw else (rest.pop(0) if rest else None)
            if rest or k[0] != "num" or not k[1].isdigit():
                raise Unsupported(f"{name}(): n must be a non-negative integer literal")
            dflt = ""
            if d is not None and d != ("id", "NA"):
                dflt = f", {self.lit(d)}"
            order = None
            if "order_by" in kw:
                ob = kw["order_by"]
                order = f"{self.ex(ob[2][0][1])}.desc()" if ob[0] == "call" and ob[1] == "desc" else f"{self.ex(ob)}.asc()"
            w = self.win(keys, order=order)
            return f"F.{name}({e(pos[0])}, {int(k[1])}{dflt}).over({w})"
        if name in ("cumsum", "cummax", "cummin") and len(pos) == 1 and not kw:
            if mode == "agg":
                raise Unsupported(f"{name}() inside summarise() has no automatic conversion")
            w = self.win(keys, ("Window.unboundedPreceding", "Window.currentRow"))
            x = e(pos[0])
            fn = {"cumsum": "sum", "cummax": "max", "cummin": "min"}[name]
            # R propagates NA: once an NA is seen every later value is NA
            return f"F.when(F.sum({x}.isNull().cast('int')).over({w}) == 0, F.{fn}({x}).over({w}))"
        if name == "row_number" and not args:
            if mode == "agg":
                raise Unsupported("row_number() inside summarise() has no automatic conversion")
            return f"F.row_number().over({self.win(keys)})"
        if name in AGG:
            na = kw.get("na.rm")
            return self.over(f"F.{AGG[name]}({e(pos[0])})", mode, keys)
        if name in ("min", "max") and len(pos) == 1:
            return self.over(f"F.{name}({e(pos[0])})", mode, keys)
        if name in ("pmin", "pmax", "min", "max"):
            return f"F.{'least' if name.endswith('min') else 'greatest'}({', '.join(e(x) for x in pos)})"
        if name == "n" and not args:
            return self.over("F.count(F.lit(1))", mode, keys)
        if name == "length" and pos:
            return self.over(f"F.count({e(pos[0])})", mode, keys)
        if name in SIMPLE:
            return f"F.{SIMPLE[name]}({', '.join(e(x) for x in pos)})"
        if name == "round":
            return f"F.round({e(pos[0])}, {self.lit(pos[1]) if len(pos) > 1 else 0})"
        if name in CAST:
            return f"{e(pos[0])}.cast({CAST[name]!r})"
        if name == "as.Date":
            return f"F.to_date({e(pos[0])})"
        if name == "is.na":
            return f"{e(pos[0])}.isNull()"
        if name == "is.null":
            return f"{e(pos[0])}.isNull()"
        if name in ("ifelse", "if_else") and len(pos) == 3:
            return f"F.when({strip_parens(e(pos[0]))}, {e(pos[1])}).otherwise({e(pos[2])})"
        if name == "case_when":
            code = None
            for x in pos:
                if x[0] != "formula" and not (x[0] == "bin" and x[1] == "~"):
                    raise Unsupported("case_when arguments must be `cond ~ value`")
            for x in pos:
                cond, val = (x[2], x[3])
                if cond is None:
                    raise Unsupported("case_when formula without condition")
                if cond == ("id", "TRUE"):
                    code = f"{code}.otherwise({e(val)})" if code else e(val)
                    break
                c = strip_parens(e(cond))
                code = f"F.when({c}, {e(val)})" if code is None else f"{code}.when({c}, {e(val)})"
            return code
        if name == "paste0":
            return f"F.concat({', '.join(self.strcol(x, mode, keys) for x in pos)})"
        if name == "paste":
            sep = self.lit(kw["sep"]) if "sep" in kw else "' '"
            return f"F.concat_ws({sep}, {', '.join(e(x) for x in pos)})"
        if name in ("str_detect", "grepl"):
            if name == "grepl":
                pos = [pos[1], pos[0]]
            return f"{e(pos[0])}.rlike({self.lit(pos[1])})"
        if name == "startsWith" or name == "str_starts":
            return f"{e(pos[0])}.startswith({self.lit(pos[1])})"
        if name in ("substr", "substring", "str_sub"):
            s, en = self.lit(pos[1]), self.lit(pos[2])
            return f"F.substring({e(pos[0])}, {s}, {int(en) - int(s) + 1})"
        if name in ("str_replace_all", "gsub", "str_replace"):
            if name == "gsub":
                pat, rep, x = pos[0], pos[1], pos[2]
            else:
                x, pat, rep = pos[0], pos[1], pos[2]
            if name == "str_replace":
                self.warn("str_replace replaces only the first match; regexp_replace replaces all")
            return f"F.regexp_replace({e(x)}, {self.lit(pat)}, {self.lit(rep)})"
        if name == "between":
            return f"{e(pos[0])}.between({e(pos[1])}, {e(pos[2])})"
        if name == "c":
            raise Unsupported("c() outside %in%")
        raise Unsupported(f"function {name}() has no automatic conversion")

    def strcol(self, n, mode, keys):
        c = self.ex(n, mode, keys)
        return f"F.lit({c})" if n[0] in ("str", "num") else c

    # ---- pipelines
    def names(self, args):
        out = []
        for nm, x in args:
            if nm:
                raise Unsupported("named argument where column names expected")
            if x[0] == "id":
                out.append(repr(x[1]))
            elif x[0] == "str":
                out.append(repr(x[1]))
            else:
                raise Unsupported("expected plain column names")
        return out

    def pipeline(self, n):
        chain = []
        while n[0] == "pipe":
            chain.append(n[2])
            n = n[1]
        chain.reverse()
        code = self.base(n)
        keys: list = []
        self.cur_order = self.var_order.get(n[1]) if n[0] == "id" else None
        for step in chain:
            if step[0] != "call":
                raise Unsupported("pipe into a non-call")
            code, keys = self.verb(code, keys, step[1], step[2])
        return code

    def base(self, n):
        if n[0] == "id":
            return n[1]
        if n[0] == "call":
            return self.source(n)
        raise Unsupported("unsupported pipeline source")

    def source(self, n):
        name, args = n[1], n[2]
        pos = [x[1] for x in args if not x[0]]
        if name in ("read_csv", "read.csv", "read_delim", "read.delim", "fread", "read_tsv"):
            sep = {"read_tsv": "\\t"}.get(name)
            extra = f", sep={sep!r}" if sep else ""
            return f"spark.read.csv({self.lit(pos[0])}, header=True, inferSchema=True{extra})"
        if name in ("read_parquet", "read_feather"):
            return f"spark.read.parquet({self.lit(pos[0])})"
        if name in ("read_json", "fromJSON", "stream_in"):
            return f"spark.read.json({self.lit(pos[0])})"
        if name in ("as_tibble", "tibble", "data.frame", "as.data.frame"):
            raise Unsupported("in-memory data frame construction: build with spark.createDataFrame")
        raise Unsupported(f"source {name}() has no automatic conversion")

    # ---- slice_* verbs -> row_number()/rank() windows
    def _count(self, pos, named, what):
        node = dict(named).get("n")
        if node is None and pos:
            node = pos[0]
        if "prop" in dict(named):
            raise Unsupported(f"{what}(prop=...) has no automatic conversion; use n=")
        if node is None:
            return 1
        if node[0] != "num" or not node[1].isdigit():
            raise Unsupported(f"{what}(): n must be a non-negative integer literal")
        return int(node[1])

    def slice_verb(self, code, keys, name, pos, named):
        nd = dict(named)
        if name in ("slice_head", "slice_tail"):
            k = self._count(pos, named, name)
            if name == "slice_head":
                w = self.win(keys)
            else:                                   # last rows = first rows of the reversed order
                if self.cur_order:
                    rev = ", ".join(c[:-7] + ".asc()" if c.endswith(".desc()") else c[:-6] + ".desc()" for c in self._split_order(self.cur_order))
                else:
                    rev = "F.monotonically_increasing_id().desc()"
                w = self.win(keys, order=rev)
            return f"{code}.withColumn('_rn', F.row_number().over({w})).filter(F.col('_rn') <= {k}).drop('_rn')"
        if name in ("slice_max", "slice_min"):
            ob = nd.get("order_by") or (pos[0] if pos else None)
            if ob is None:
                raise Unsupported(f"{name}() needs order_by")
            rest = [x for x in pos if x is not ob]
            k = self._count(rest, [(a, b) for a, b in named if a != "order_by"], name)
            ties = nd.get("with_ties")
            if ties is not None and ties not in (("id", "TRUE"), ("id", "FALSE")):
                raise Unsupported(f"{name}(with_ties=...) must be TRUE or FALSE")
            with_ties = ties != ("id", "FALSE")          # dplyr default is with_ties = TRUE
            direction = "desc" if name == "slice_max" else "asc"
            w = self.win(keys, order=f"{self.ex(ob)}.{direction}()")
            fn = "rank" if with_ties else "row_number"
            return f"{code}.withColumn('_rn', F.{fn}().over({w})).filter(F.col('_rn') <= {k}).drop('_rn')"
        if name == "slice_sample":
            k = self._count(pos, named, name)
            if "replace" in nd or "weight_by" in nd:
                raise Unsupported("slice_sample(replace=/weight_by=) has no automatic conversion")
            w = self.win(keys, order="F.rand()")
            return f"{code}.withColumn('_rn', F.row_number().over({w})).filter(F.col('_rn') <= {k}).drop('_rn')"
        # slice(): positive literal row numbers
        idx = []
        for x in pos:
            if x[0] == "num" and x[1].isdigit():
                idx.append(int(x[1]))
            elif x[0] == "bin" and x[1] == ":" and x[2][0] == "num" and x[3][0] == "num" and x[2][1].isdigit() and x[3][1].isdigit():
                lo, hi = int(x[2][1]), int(x[3][1])
                if hi - lo > 10000:
                    raise Unsupported("slice(): range too large")
                idx += list(range(lo, hi + 1))
            elif x[0] == "call" and x[1] == "c" and all(a[0] is None and a[1][0] == "num" and a[1][1].isdigit() for a in x[2]):
                idx += [int(a[1][1]) for a in x[2]]
            else:
                raise Unsupported("slice() needs positive literal row numbers (e.g. slice(1:3), slice(c(1, 5)))")
        if not idx or min(idx) < 1 or named:
            raise Unsupported("slice() needs positive literal row numbers (e.g. slice(1:3), slice(c(1, 5)))")
        w = self.win(keys)
        if idx == list(range(min(idx), max(idx) + 1)):
            cond = f"F.col('_rn').between({min(idx)}, {max(idx)})"
        else:
            cond = f"F.col('_rn').isin({sorted(set(idx))})"
        return f"{code}.withColumn('_rn', F.row_number().over({w})).filter({cond}).drop('_rn')"

    @staticmethod
    def _split_order(order):
        parts, depth, cur = [], 0, ""
        for ch in order:
            depth += ch in "(["
            depth -= ch in ")]"
            if ch == "," and depth == 0:
                parts.append(cur.strip())
                cur = ""
            else:
                cur += ch
        parts.append(cur.strip())
        return parts

    # ---- tidyr pivots
    def _colnames(self, node, what):
        if node[0] in ("id", "str"):
            return [node[1]]
        if node[0] == "call" and node[1] == "c" and all(a[0] is None and a[1][0] in ("id", "str") for a in node[2]):
            return [a[1][1] for a in node[2]]
        raise Unsupported(f"{what} must be plain column names (c(a, b)); selection helpers, ranges and negation need the schema")

    def pivot_longer(self, code, pos, named):
        nd = dict(named)
        extra = set(nd) - {"cols", "names_to", "values_to", "values_drop_na"}
        if extra:
            raise Unsupported(f"pivot_longer({', '.join(sorted(extra))}=...) has no automatic conversion")
        cols_node = nd.get("cols") or (pos[0] if pos else None)
        if cols_node is None:
            raise Unsupported("pivot_longer() without cols")
        cols = self._colnames(cols_node, "pivot_longer cols")
        to = nd["names_to"][1] if "names_to" in nd and nd["names_to"][0] == "str" else ("name" if "names_to" not in nd else None)
        val = nd["values_to"][1] if "values_to" in nd and nd["values_to"][0] == "str" else ("value" if "values_to" not in nd else None)
        if to is None or val is None:
            raise Unsupported("names_to / values_to must be string literals")
        if len(set(cols)) != len(cols) or to == val:
            raise Unsupported("pivot_longer: duplicate column names")
        structs = ", ".join(f"F.struct(F.lit({c!r}).alias('k'), F.col({c!r}).alias('v'))" for c in cols)
        drop = ", ".join(repr(c) for c in cols)
        out = (f"{code}.select('*', F.explode(F.array({structs})).alias('_pl')).drop({drop})"
               f".select('*', F.col('_pl.k').alias({to!r}), F.col('_pl.v').alias({val!r})).drop('_pl')")
        if nd.get("values_drop_na") == ("id", "TRUE"):
            out += f".filter(F.col({val!r}).isNotNull())"
        self.warn("pivot_longer: the value columns must have compatible types (Spark casts them to a common type; mixed number/text becomes text)")
        return out

    def pivot_wider(self, code, pos, named):
        nd = dict(named)
        extra = set(nd) - {"names_from", "values_from", "id_cols", "values_fill", "values_fn"}
        if extra or pos:
            raise Unsupported(f"pivot_wider({', '.join(sorted(extra)) or 'positional arguments'}) has no automatic conversion")
        if "names_from" not in nd or "values_from" not in nd:
            raise Unsupported("pivot_wider() needs names_from and values_from")
        nf = self._colnames(nd["names_from"], "names_from")
        vf = self._colnames(nd["values_from"], "values_from")
        if len(nf) != 1 or len(vf) != 1:
            raise Unsupported("pivot_wider with several names_from/values_from columns has no automatic conversion (Spark names the result columns differently)")
        fn = "first"
        if "values_fn" in nd:
            f = nd["values_fn"]
            fname = f[1] if f[0] == "id" else None
            if fname not in AGG or fname in ("first", "last"):
                if fname not in ("first", "last"):
                    raise Unsupported("values_fn must be one of sum, mean, median, first, last")
            fn = AGG[fname]
        else:
            self.warn("pivot_wider: duplicate id/name combinations keep an arbitrary value (tidyr would return list-columns); pass values_fn = sum/mean/... to aggregate")
        agg = f"F.{fn}({vf[0]!r})"
        if "id_cols" in nd:
            ids = ", ".join(repr(c) for c in self._colnames(nd["id_cols"], "id_cols"))
            out = f"{code}.groupBy({ids}).pivot({nf[0]!r}).agg({agg})"
        else:
            skip = f"({nf[0]!r}, {vf[0]!r})"
            out = f"(lambda d: d.groupBy(*[c for c in d.columns if c not in {skip}]).pivot({nf[0]!r}).agg({agg}))({code})"
        if "values_fill" in nd:
            out += f".fillna({self.lit(nd['values_fill'])})"
        self.warn("pivot_wider: Spark pivot makes an extra pass to find the distinct values (sorted); column order can differ from tidyr")
        return out

    def verb(self, code, keys, name, args):
        pos = [x[1] for x in args if not x[0]]
        named = [(x[0], x[1]) for x in args if x[0]]
        if name in ("select", "rename", "transmute", "summarise", "summarize", "count", "distinct", "bind_rows", "drop_na") or name in JOIN:
            self.cur_order = None                  # these can drop the sort columns or reorder rows
        if name == "filter":
            conds = [self.ex(x, "win" if keys else "plain", keys) for x in pos]
            if keys:
                self.warn("filter() on grouped data: aggregates evaluated as windows")
            if any(".over(" in c for c in conds):   # Spark does not allow window functions inside WHERE: compute them as columns first
                pre, use = code, []
                for i, c in enumerate(conds):
                    if ".over(" in c:
                        pre += f".withColumn('_f{i}', {strip_parens(c)})"
                        use.append(f"F.col('_f{i}')")
                    else:
                        use.append(c)
                drops = ", ".join(repr(f"_f{i}") for i, c in enumerate(conds) if ".over(" in c)
                return f"{pre}.filter({' & '.join(strip_parens(c) if len(use) == 1 else c for c in use)}).drop({drops})", keys
            return f"{code}.filter({' & '.join(strip_parens(c) if len(conds) == 1 else c for c in conds)})", keys
        if name in ("mutate", "transmute"):
            mode = "win"
            out = code
            for nm, x in named:
                out += f".withColumn({nm!r}, {strip_parens(self.ex(x, mode, keys))})"
            if name == "transmute":
                out += f".select({', '.join(repr(nm) for nm, _ in named)})"
            if pos:
                raise Unsupported("mutate(across(...)) / unnamed mutate args")
            return out, keys
        if name == "select":
            cols = []
            drop = []
            for nm, x in args:
                if nm:
                    cols.append(f"F.col({x[1]!r}).alias({nm!r})")
                elif x[0] == "un" and x[1] == "-" and x[2][0] == "id":
                    drop.append(repr(x[2][1]))
                elif x[0] in ("id", "str"):
                    cols.append(repr(x[1]))
                else:
                    raise Unsupported("select helper (starts_with/everything/...) - list columns explicitly")
            if drop and not cols:
                return f"{code}.drop({', '.join(drop)})", keys
            return f"{code}.select({', '.join(cols)})", keys
        if name == "rename":
            return code + "".join(f".withColumnRenamed({x[1]!r}, {nm!r})" for nm, x in named), keys
        if name == "arrange":
            cols = []
            for x in pos:
                if x[0] == "call" and x[1] == "desc":
                    cols.append(f"{self.ex(x[2][0][1])}.desc()")
                else:
                    cols.append(f"{self.ex(x)}.asc()")
            self.cur_order = ", ".join(cols)
            return f"{code}.orderBy({', '.join(cols)})", keys
        if name == "group_by":
            return code, [x[1:-1] for x in self.names(args)]
        if name == "ungroup":
            return code, []
        if name in ("summarise", "summarize"):
            if not keys:
                aggs = ", ".join(f"{self.ex(x, 'agg')}.alias({nm!r})" for nm, x in named)
                return f"{code}.agg({aggs})", []
            aggs = ", ".join(f"{self.ex(x, 'agg')}.alias({nm!r})" for nm, x in named)
            return f"{code}.groupBy({', '.join(map(repr, keys))}).agg({aggs})", keys[:-1]
        if name == "count":
            cols = self.names(args)
            sort = any(nm == "sort" for nm, _ in named)
            out = f"{code}.groupBy({', '.join(cols)}).count()"
            return (out + ".orderBy(F.col('count').desc())" if sort else out), keys
        if name == "distinct":
            return (f"{code}.dropDuplicates([{', '.join(self.names(args))}])" if args else f"{code}.distinct()"), keys
        if name == "head" or (name == "slice_head" and not keys and "prop" not in dict(named)):
            n = next((self.lit(x) for nm, x in named if nm == "n"), self.lit(pos[0]) if pos else "6")
            return f"{code}.limit({n})", keys
        if name in ("slice_head", "slice_tail", "slice_max", "slice_min", "slice_sample", "slice"):
            return self.slice_verb(code, keys, name, pos, named), keys
        if name == "pivot_longer":
            return self.pivot_longer(code, pos, named), keys
        if name == "pivot_wider":
            return self.pivot_wider(code, pos, named), keys
        if name in JOIN:
            right = self.base(pos[0])
            by = dict(named).get("by")
            how = JOIN[name]
            if by is None and len(pos) > 1:
                by = pos[1]
            if by is None:
                raise Unsupported("join without by= (natural join) - specify by explicitly")
            if by[0] == "str":
                return f"{code}.join({right}, on={by[1]!r}, how={how!r})", keys
            if by[0] == "call" and by[1] == "c":
                items = by[2]
                if all(not nm for nm, _ in items):
                    return f"{code}.join({right}, on=[{', '.join(self.lit(x) for _, x in items)}], how={how!r})", keys
                conds = " & ".join(f"(F.col({'l.' + nm!r}) == F.col({'r.' + str(ast.literal_eval(self.lit(x)))!r}))" for nm, x in items)
                conds = strip_parens(conds) if len(items) == 1 else conds
                return f"{code}.alias('l').join({right}.alias('r'), {conds}, {how!r})", keys
            raise Unsupported("unsupported by= specification")
        if name == "bind_rows":
            return code + "".join(f".unionByName({self.base(x)}, allowMissingColumns=True)" for x in pos), keys
        if name == "drop_na":
            return (f"{code}.dropna(subset=[{', '.join(self.names(args))}])" if args else f"{code}.dropna()"), keys
        if name == "replace_na":
            d = pos[0]
            if d[0] == "call" and d[1] == "list":
                items = ", ".join(f"{nm!r}: {self.lit(x)}" for nm, x in d[2])
                return f"{code}.fillna({{{items}}})", keys
            raise Unsupported("replace_na needs list(col = value)")
        if name in ("write_csv", "write.csv", "write_parquet"):
            fmt = "parquet" if "parquet" in name else "csv"
            self.r.note("Spark writes a *directory* of part files, not a single file")
            return f"{code}.write.mode('overwrite').{fmt}({self.lit(pos[0])}{', header=True' if fmt == 'csv' else ''})", keys
        if name in ("print", "glimpse", "View"):
            return f"{code}.show()", keys
        if name in ("pull", "collect", "as_tibble"):
            return code, keys
        if name in ("rowwise", "nest", "unnest"):
            raise Unsupported(f"{name}() has no automatic conversion")
        raise Unsupported(f"dplyr verb {name}() has no automatic conversion")


def split_statements(toks):
    out, cur = [], []
    for t in toks:
        if t[0] == "semi" or t[1] == ";":
            if cur:
                out.append(cur)
            cur = []
        else:
            cur.append(t)
    if cur:
        out.append(cur)
    return out


def convert_r(source: str, report: Report) -> str:
    em = Emitter(report)
    lines = []
    toks = lex(source)
    stmts = split_statements(toks)
    for st in stmts:
        start = st[0][2]
        end = st[-1][2] + len(st[-1][1]) + 1
        text = source[start:end + 1].rstrip()
        # strip trailing partial token text
        em.line = source.count("\n", 0, start) + 1
        try:
            lines += stmt(em, st)
        except Unsupported as e:
            report.todo(em.line, str(e))
            lines.append(f"# TODO(sparqix): {cm(e)}")
            lines += [f"# {cm(l)}" for l in text.splitlines()]
    head = ["from pyspark.sql import SparkSession, functions as F" + (", Window" if em.uses_window else ""),
            "", 'spark = SparkSession.builder.appName("converted").getOrCreate()', ""]
    return "\n".join(head + lines) + "\n"


def stmt(em: Emitter, st):
    # library(...) / require(...)
    if st[0][1] in ("library", "require", "suppressPackageStartupMessages", "setwd", "options", "set.seed"):
        return []
    # assignment forms
    depth, eq = 0, None
    for i, t in enumerate(st):                    # only a top-level `<-`, `=` or `->` is an assignment (not `row.names = FALSE` inside a call)
        if t[0] == "op" and t[1] in ("(", "[", "{"):
            depth += 1
        elif t[0] == "op" and t[1] in (")", "]", "}"):
            depth -= 1
        elif depth == 0 and t[0] == "op" and t[1] in ("<-", "=", "->"):
            eq = i
            break
    target, rest = None, st
    if eq is not None:
        if st[eq][1] == "->":
            target, rest = st[eq + 1][1], st[:eq]
        else:
            lhs = st[:eq]
            if len(lhs) == 1:
                target = lhs[0][1]
            elif len(lhs) == 3 and lhs[1][1] == "$":
                target, col = lhs[0][1], lhs[2][1]
                expr = P(st[eq + 1:]).expr()
                em.cur_order = em.var_order.get(target)
                code = em.ex(expr, "win", [])
                return [f"{target} = {target}.withColumn({col!r}, {strip_parens(code)})"]
            else:
                raise Unsupported("unsupported assignment target")
            rest = st[eq + 1:]
    node = P(rest).expr()
    if node[0] == "call" and node[1] in ("write.csv", "write_csv", "write.csv2", "write_parquet", "write_tsv") and node[2] and not target:
        data = node[2][0][1]
        paths = [a for a in node[2][1:] if a[0] in (None, "file", "path")]
        if paths and data[0] in ("id", "pipe"):
            base = em.pipeline(data) if data[0] == "pipe" else em.base(data)
            fmt = "parquet" if "parquet" in node[1] else "csv"
            sep = ", sep='\\t'" if node[1] == "write_tsv" else ""
            em.r.note("Spark writes a *directory* of part files, not a single file")
            return [f"{base}.write.mode('overwrite').{fmt}({em.lit(paths[0][1])}{', header=True' if fmt == 'csv' else ''}{sep})"]
    if node[0] == "call" and node[1] in ("print", "head", "glimpse", "View", "nrow", "ncol", "colnames", "names", "dim") and node[2]:
        inner = node[2][0][1]
        base = em.pipeline(inner) if inner[0] == "pipe" else em.base(inner)
        if node[1] == "nrow":
            return [f"{target + ' = ' if target else ''}{base}.count()"]
        if node[1] in ("colnames", "names"):
            return [f"{target + ' = ' if target else ''}{base}.columns"]
        if node[1] == "head":
            n = em.lit(node[2][1][1]) if len(node[2]) > 1 else "6"
            return [f"{target + ' = ' if target else ''}{base}.limit({n})" if target else f"{base}.show({n})"]
        return [f"{base}.show()"]
    if node[0] == "pipe":
        code = em.pipeline(node)
    elif node[0] == "call":
        code = em.source(node)
    elif node[0] == "id":
        code = node[1]
    else:
        raise Unsupported("unsupported R statement")
    code = format_chain(code)
    if target:
        em.dfs.add(target)
        if node[0] == "pipe":
            em.var_order[target] = em.cur_order
        elif node[0] == "id":
            em.var_order[target] = em.var_order.get(node[1])
        else:
            em.var_order.pop(target, None)
        if em.var_order.get(target) is None:
            em.var_order.pop(target, None)
        return [f"{target} = {code}"]
    return [code if code.endswith(")") and ".write." in code else f"{code}.show()" if code.endswith(")") else code]
