"""Excel formulas -> PySpark column expressions.

Input: one formula per line, either `=A2*B2`  or  `total = A2*B2` (name = formula).
A cell reference like B2 maps to column 'B' (row-relative); ranges (B2:B100) are whole-column aggregates.
Use --excel-columns "A=price,B=qty" to map letters to real column names.
"""
from __future__ import annotations

import re

from .core import Report, Unsupported, strip_parens, cm
from .pratt import Pratt, tokenize

TOK = re.compile(r"""(?P<ws>\s+)|(?P<num>\d+\.?\d*(?:[eE][+-]?\d+)?)|(?P<str>"(?:[^"]|"")*")
 |(?P<sref>(?:'[^']{1,64}'|[A-Za-z_]\w{0,63})!\$?[A-Za-z]{1,3}\$?\d*(?::\$?[A-Za-z]{1,3}\$?\d*)?)
 |(?P<ref>\$?[A-Za-z]{1,3}\$?\d*(?::\$?[A-Za-z]{1,3}\$?\d*)?(?![\w(]))|(?P<id>[A-Za-z_][\w.]*)
 |(?P<op><=|>=|<>|[-+*/^&=<>(),%:])""", re.X)
BP = {"=": 20, "<>": 20, "<": 20, ">": 20, "<=": 20, ">=": 20, "&": 25, "+": 30, "-": 30, "*": 40, "/": 40, "^": 60}
UN = {"-": 50, "+": 50}
AGG = {"SUM": "sum", "AVERAGE": "avg", "MIN": "min", "MAX": "max", "COUNT": "count", "MEDIAN": "median",
       "STDEV": "stddev", "STDEV.S": "stddev", "VAR": "variance"}
SIMPLE = {"ABS": "abs", "SQRT": "sqrt", "EXP": "exp", "LN": "log", "LOG10": "log10", "UPPER": "upper",
          "LOWER": "lower", "TRIM": "trim", "LEN": "length", "YEAR": "year", "MONTH": "month", "DAY": "dayofmonth",
          "HOUR": "hour", "MINUTE": "minute", "SECOND": "second", "PROPER": "initcap", "FLOOR": "floor",
          "CEILING": "ceil", "INT": "floor", "SIGN": "signum"}


_SREF = re.compile(r"^(?:'([^']{1,64})'|([A-Za-z_]\w{0,63}))!\$?([A-Za-z]{1,3})\$?(\d*)(?::\$?([A-Za-z]{1,3})\$?(\d*))?$")

_LOOKUP_HELPERS = """def _xl_lookup(df, key, table, key_col, val_col, out):
    \"\"\"exact-match lookup: the first matching row wins (like VLOOKUP(..., FALSE) / XLOOKUP); no match -> NULL (Excel's #N/A)\"\"\"
    t = table.select(F.col(key_col).alias('_k'), F.col(val_col).alias(out)).withColumn('_r', F.monotonically_increasing_id())
    first = t.withColumn('_n', F.row_number().over(Window.partitionBy('_k').orderBy('_r'))).filter(F.col('_n') == 1).drop('_n', '_r')
    return df.join(first, key == F.col('_k'), 'left').drop('_k')


def _xl_approx(df, key, table, key_col, val_col, out):
    \"\"\"approximate-match lookup (VLOOKUP's default): the row with the largest key <= the lookup value; none -> NULL\"\"\"
    t = table.select(F.col(key_col).alias('_k'), F.col(val_col).alias(out))
    j = df.withColumn('_rid', F.monotonically_increasing_id()).join(t, F.col('_k') <= key, 'left')
    w = Window.partitionBy('_rid').orderBy(F.col('_k').desc_nulls_last())
    return j.withColumn('_n', F.row_number().over(w)).filter(F.col('_n') == 1).drop('_n', '_k', '_rid')
"""


def _letters_to_num(L):
    n = 0
    for ch in L.upper():
        n = n * 26 + ord(ch) - 64
    return n


def _num_to_letters(n):
    out = ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


class Excel:
    def __init__(self, cols, report):
        self.cols, self.r = cols, report
        self.pre: list[str] = []          # statements that must run before the current formula's withColumn (lookup joins)
        self.post: list[str] = []         # ... and cleanup after it
        self.sheets: dict[str, str] = {}  # sheet name -> DataFrame variable
        self.used_helpers = False
        self.n_lookup = 0

    def sheet(self, ref_text, what):
        """'Sheet2'!A2:C9 -> (variable, first column number, last column number or None, sheet)"""
        m = _SREF.match(ref_text)
        if not m:
            raise Unsupported(f"{what}: the table must be a range on another sheet, e.g. Sheet2!A:C (mapped to a DataFrame)")
        sheet = m.group(1) or m.group(2)
        var = "lk_" + (re.sub(r"\W", "_", sheet).strip("_").lower() or "sheet")
        self.sheets.setdefault(sheet, var)
        c1 = _letters_to_num(m.group(3))
        c2 = _letters_to_num(m.group(5)) if m.group(5) else None
        return self.sheets[sheet], c1, c2, sheet

    def lcol(self, sheet, num):
        L = _num_to_letters(num)
        return self.cols.get(f"{sheet}.{L}", L)

    def lookup(self, key, tbl, kc, vc, exact):
        self.n_lookup += 1
        out = f"_lk{self.n_lookup}"
        self.used_helpers = True
        fn = "_xl_lookup" if exact else "_xl_approx"
        self.pre.append(f"df = {fn}(df, {key}, {tbl}, {kc!r}, {vc!r}, {out!r})")
        self.post.append(out)
        if not exact:
            self.r.warn("excel", "approximate-match lookup: assumes the lookup table is sorted ascending, as Excel requires; "
                                 "Spark joins on key <= value and keeps the largest key (a non-equi join: can be slow on big tables)")
        return f"F.col({out!r})"

    @staticmethod
    def exact_flag(node, what):
        if node is None:
            return False
        if node in (("id", "FALSE"), ("num", "0")) or (node[0] == "id" and node[1].upper() == "FALSE"):
            return True
        if (node[0] == "id" and node[1].upper() == "TRUE") or node == ("num", "1"):
            return False
        raise Unsupported(f"{what}: the match type must be a literal TRUE/FALSE (or 0/1)")

    def col(self, ref):
        if "!" in ref:
            raise Unsupported("a reference to another sheet is only supported inside VLOOKUP / XLOOKUP / INDEX+MATCH")
        letters = re.match(r"\$?([A-Za-z]+)", ref).group(1).upper()
        name = self.cols.get(letters, letters)
        return f"F.col({name!r})"

    def is_range(self, n):
        return n[0] == "ref" and ":" in n[1]

    def ex(self, n):
        t = n[0]
        if t == "num":
            return n[1]
        if t == "str":
            return repr(n[1][1:-1].replace('""', '"'))
        if t == "paren":
            return f"({self.ex(n[1])})"
        if t == "ref":
            if ":" in n[1]:
                raise Unsupported("range used outside an aggregate/lookup function")
            return self.col(n[1])
        if t == "id":
            u = n[1].upper()
            if u in ("TRUE", "FALSE"):
                return u.capitalize()
            raise Unsupported(f"unknown name {n[1]}")
        if t == "un":
            return f"(-{self.ex(n[2])})" if n[1] == "-" else self.ex(n[2])
        if t == "bin":
            op, l, r = n[1], self.ex(n[2]), self.ex(n[3])
            if op == "&":
                return f"F.concat({self.s(n[2])}, {self.s(n[3])})"
            if op == "=":
                return f"({l} == {r})"
            if op == "<>":
                return f"({l} != {r})"
            if op == "^":
                return f"F.pow({l}, {r})"
            return f"({l} {op} {r})"
        if t == "call":
            return self.call(n[1].upper(), n[2])
        raise Unsupported("unsupported formula element")

    def s(self, n):
        c = self.ex(n)
        return f"F.lit({c})" if n[0] in ("str", "num") else c

    def over(self, code):
        self.r.warn("excel", "whole-column aggregate used per row -> Window.partitionBy() (single partition); "
                             "compute it once with df.agg(...) if you need a scalar")
        return code + ".over(Window.partitionBy())"

    def crit(self, rng, crit):
        """SUMIF-style criterion -> boolean Column"""
        c = self.col(rng[1])
        if crit[0] == "str":
            s = crit[1][1:-1]
            m = re.match(r"^(<=|>=|<>|=|<|>)?(.*)$", s)
            op, val = m.group(1) or "=", m.group(2)
            try:
                float(val)
                v = val
            except ValueError:
                if "*" in val or "?" in val:
                    pat = "^" + re.escape(val).replace(r"\*", ".*").replace(r"\?", ".") + "$"
                    return f"{c}.rlike({pat!r})"
                v = repr(val)
            return f"({c} {({'=': '==', '<>': '!='}).get(op, op)} {v})"
        return f"({c} == {self.ex(crit)})"

    def call(self, f, a):
        if f in AGG:
            if f == "COUNT" or len(a) == 1:
                pass
            if len(a) != 1:
                raise Unsupported(f"{f} with several arguments")
            return f"F.{AGG[f]}({self.col(a[0][1])})" if self.is_range(a[0]) else self.agg_scalar(f, a)
        if f == "COUNTA":
            return f"F.count({self.col(a[0][1])})"
        if f in ("SUMIF", "COUNTIF", "AVERAGEIF"):
            cond = self.crit(a[0], a[1])
            tgt = self.col((a[2] if len(a) > 2 else a[0])[1]) if f != "COUNTIF" else "F.lit(1)"
            fn = {"SUMIF": "sum", "COUNTIF": "count", "AVERAGEIF": "avg"}[f]
            return f"F.{fn}(F.when({strip_parens(cond)}, {tgt}))"
        if f in ("SUMIFS", "COUNTIFS", "AVERAGEIFS"):
            if f == "COUNTIFS":
                pairs = a
                tgt = "F.lit(1)"
            else:
                tgt, pairs = self.col(a[0][1]), a[1:]
            conds = " & ".join(self.crit(pairs[i], pairs[i + 1]) for i in range(0, len(pairs), 2))
            fn = {"SUMIFS": "sum", "COUNTIFS": "count", "AVERAGEIFS": "avg"}[f]
            return f"F.{fn}(F.when({conds}, {tgt}))"
        if f in SIMPLE:
            return f"F.{SIMPLE[f]}({self.ex(a[0])})"
        if f == "IF":
            els = self.s(a[2]) if len(a) > 2 else "F.lit(None)"
            return f"F.when({strip_parens(self.ex(a[0]))}, {self.s(a[1])}).otherwise({els})"
        if f == "IFS":
            code = None
            for i in range(0, len(a), 2):
                c = strip_parens(self.ex(a[i]))
                code = f"F.when({c}, {self.s(a[i + 1])})" if code is None else f"{code}.when({c}, {self.s(a[i + 1])})"
            return code
        if f == "IFERROR":
            self.r.warn("excel", "IFERROR -> coalesce(expr, fallback): Spark returns NULL (not an error) for most invalid operations")
            return f"F.coalesce({self.ex(a[0])}, {self.s(a[1])})"
        if f == "AND":
            return "(" + " & ".join(self.ex(x) for x in a) + ")"
        if f == "OR":
            return "(" + " | ".join(self.ex(x) for x in a) + ")"
        if f == "NOT":
            return f"(~{self.ex(a[0])})"
        if f == "ISBLANK":
            return f"{self.ex(a[0])}.isNull()"
        if f in ("ISNUMBER", "ISTEXT"):
            raise Unsupported(f"{f} depends on cell types; Spark columns are typed - cast/filter explicitly")
        if f == "ROUND":
            return f"F.round({self.ex(a[0])}, {self.ex(a[1]) if len(a) > 1 else 0})"
        if f in ("ROUNDUP", "ROUNDDOWN"):
            raise Unsupported(f"{f} - use F.ceil/F.floor with a scale factor")
        if f == "MOD":
            return f"({self.ex(a[0])} % {self.ex(a[1])})"
        if f == "POWER":
            return f"F.pow({self.ex(a[0])}, {self.ex(a[1])})"
        if f in ("CONCAT", "CONCATENATE"):
            return f"F.concat({', '.join(self.s(x) for x in a)})"
        if f == "TEXTJOIN":
            return f"F.concat_ws({self.ex(a[0])}, {', '.join(self.s(x) for x in a[2:])})"
        if f == "LEFT":
            return f"F.substring({self.ex(a[0])}, 1, {self.ex(a[1]) if len(a) > 1 else 1})"
        if f == "RIGHT":
            n = self.ex(a[1]) if len(a) > 1 else "1"
            return f"F.substring({self.ex(a[0])}, -{n}, {n})"
        if f == "MID":
            return f"F.substring({self.ex(a[0])}, {self.ex(a[1])}, {self.ex(a[2])})"
        if f == "SUBSTITUTE":
            return f"F.regexp_replace({self.ex(a[0])}, {re.escape(a[1][1][1:-1])!r}, {self.ex(a[2])})"
        if f == "TODAY":
            return "F.current_date()"
        if f == "NOW":
            return "F.current_timestamp()"
        if f == "DATE":
            return f"F.make_date({', '.join(self.ex(x) for x in a)})"
        if f == "WEEKDAY":
            return f"(((F.dayofweek({self.ex(a[0])}) + 5) % 7) + 1)"
        if f == "SWITCH":
            e, rest = self.ex(a[0]), a[1:]
            code = None
            for i in range(0, len(rest) - 1, 2):
                c = f"({e} == {self.ex(rest[i])})"
                code = f"F.when({c}, {self.s(rest[i + 1])})" if code is None else f"{code}.when({c}, {self.s(rest[i + 1])})"
            if len(rest) % 2:
                code += f".otherwise({self.s(rest[-1])})"
            return code
        if f == "VLOOKUP":
            if len(a) not in (3, 4):
                raise Unsupported("VLOOKUP needs (key, table, column, [exact])")
            if a[1][0] != "ref":
                raise Unsupported("VLOOKUP: the table must be a range on another sheet, e.g. Sheet2!A:C")
            idx = a[2]
            if idx[0] != "num" or not idx[1].isdigit() or int(idx[1]) < 1:
                raise Unsupported("VLOOKUP: the column index must be a positive integer literal")
            tbl, c1, c2, sheet = self.sheet(a[1][1], "VLOOKUP")
            k = int(idx[1])
            if c2 is not None and k > c2 - c1 + 1:
                raise Unsupported("VLOOKUP: the column index is outside the table (Excel gives #REF!)")
            exact = self.exact_flag(a[3] if len(a) > 3 else None, "VLOOKUP")
            key = self.ex(a[0])
            return self.lookup(key, tbl, self.lcol(sheet, c1), self.lcol(sheet, c1 + k - 1), exact)
        if f == "XLOOKUP":
            if len(a) < 3 or len(a) > 4 or any(x[0] != "ref" for x in a[1:3]):
                raise Unsupported("XLOOKUP: only (key, lookup_range, return_range, [if_not_found]) with ranges on another sheet; match_mode/search_mode are not converted")
            t1, c1, c1e, sheet1 = self.sheet(a[1][1], "XLOOKUP")
            t2, c2, c2e, sheet2 = self.sheet(a[2][1], "XLOOKUP")
            if sheet1 != sheet2 or c1e not in (None, c1) or c2e not in (None, c2):
                raise Unsupported("XLOOKUP: lookup and return ranges must be single columns of the same sheet")
            key = self.ex(a[0])
            res = self.lookup(key, t1, self.lcol(sheet1, c1), self.lcol(sheet1, c2), True)
            return f"F.coalesce({res}, {self.s(a[3])})" if len(a) == 4 else res
        if f == "INDEX":
            m = a[1] if len(a) == 2 else None
            if not (m and m[0] == "call" and m[1].upper() == "MATCH" and a[0][0] == "ref"):
                raise Unsupported("INDEX is only converted in the INDEX(return_range, MATCH(key, lookup_range, 0)) form")
            ma = m[2]
            if len(ma) != 3 or ma[1][0] != "ref" or not self.exact_flag(ma[2], "MATCH") :
                raise Unsupported("INDEX/MATCH: needs an exact match (third MATCH argument 0) and a lookup range on another sheet")
            t1, c1, c1e, sheet1 = self.sheet(ma[1][1], "MATCH")
            t2, c2, c2e, sheet2 = self.sheet(a[0][1], "INDEX")
            if sheet1 != sheet2 or c1e not in (None, c1) or c2e not in (None, c2):
                raise Unsupported("INDEX/MATCH: both ranges must be single columns of the same sheet")
            key = self.ex(ma[0])
            return self.lookup(key, t1, self.lcol(sheet1, c1), self.lcol(sheet1, c2), True)
        if f in ("MATCH", "HLOOKUP", "LOOKUP"):
            raise Unsupported(f"{f}() has no automatic conversion (only VLOOKUP, XLOOKUP and INDEX/MATCH are)")
        if f == "IFNA":
            return f"F.coalesce({self.ex(a[0])}, {self.s(a[1])})"
        raise Unsupported(f"function {f}() has no automatic conversion")

    def agg_scalar(self, f, a):
        raise Unsupported(f"{f}(single cell) - aggregate needs a range")


def convert_excel(source: str, report: Report, columns: dict | None = None) -> str:
    em = Excel(columns or {}, report)
    out = ["from pyspark.sql import functions as F, Window", ""]
    n_unnamed = 0
    for ln, raw in enumerate(source.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith(("#", "//")):
            continue
        name, formula = None, line
        m = re.match(r"^([A-Za-z_]\w*)\s*[:=]\s*(=.*)$", line)
        if m:
            name, formula = m.group(1), m.group(2)
        if not formula.startswith("="):
            formula = "=" + formula
        try:
            em.pre, em.post = [], []
            p = Pratt([("ref" if k == "sref" else k, v) for k, v in tokenize(formula[1:], TOK)], BP, UN, "Excel formula")
            node = p.expr()
            if not p.done():
                raise Unsupported("trailing tokens in formula")
            code = strip_parens(em.ex(node))
            if node[0] == "call" and node[1].upper() in AGG or (node[0] == "call" and node[1].upper() in ("SUMIF", "SUMIFS", "COUNTIF", "COUNTIFS", "AVERAGEIF", "AVERAGEIFS", "COUNTA")):
                code = em.over(code)
        except Unsupported as e:
            em.pre, em.post = [], []
            report.todo(ln, str(e))
            out += [f"# TODO(sparqix): {cm(e)}", f"# {raw.strip()}"]
            continue
        if name is None:
            n_unnamed += 1
            name = f"calc_{n_unnamed}"
            report.note(f"line {ln}: unnamed formula stored as column {name!r} (write `name = =formula` to choose)")
        out += em.pre
        out.append(f"df = df.withColumn({name!r}, {code})")
        if em.post:
            out.append(f"df = df.drop({', '.join(map(repr, em.post))})")
    head = list(out[:2])
    body = out[2:]
    if em.sheets:
        head[0] = "from pyspark.sql import SparkSession, functions as F, Window"
        head += ['spark = SparkSession.builder.appName("converted").getOrCreate()', ""]
        head += [f"{var} = spark.table({re.sub(chr(92) + 'W', '_', sheet).strip('_').lower() or 'sheet'!r})" for sheet, var in em.sheets.items()]
        head.append("")
        report.note("lookup sheets are read with spark.table(...): point them at the real source (spark.read.csv/parquet/...) and make the column names match the sheet letters "
                    "or map them with --excel-columns \"Sheet.A=name\"")
    if em.used_helpers:
        head += _LOOKUP_HELPERS.splitlines() + ["", ""]
    return "\n".join(head + body) + "\n"
