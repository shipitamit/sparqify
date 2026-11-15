"""Expression translators for Talend (Java) and Alteryx expressions -> PySpark Column code.

parse -> small AST -> emit (code, kind) where kind is 'str' | 'num' | 'bool' | 'date' | 'unk'.
Anything outside the supported subset raises Unsupported so the caller can keep the original text and add a TODO.
"""
from __future__ import annotations

import re

from .core import Unsupported

TOK = re.compile(r"""(?P<ws>\s+)|(?P<comment>//[^\n]*|/\*.*?\*/)
 |(?P<num>\d+\.?\d*(?:[eE][+-]?\d+)?[LlFfDd]?)|(?P<str>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')
 |(?P<col>\[[^\]]+\])|(?P<id>[A-Za-z_]\w*)
 |(?P<op>==|!=|<>|<=|>=|&&|\|\||::|[-+*/%<>=!?:.,()])""", re.X | re.S)

JAVA_CLASSES = {"stringhandling", "relational", "talenddate", "math", "integer", "double", "long", "string", "numeric",
                "bigdecimal", "system", "context", "globalmap", "mathematical", "talendstring", "boolean", "float", "short"}
BARE = ("pig", "ssis", "datastage")                  # bare identifiers are column names
WORDOPS = ("alteryx", "pig", "datastage")           # and / or / not as words
TERN = ("java", "ssis", "pig")
PREC = {":": 6, "matches": 5, "||": 1, "or": 1, "&&": 2, "and": 2, "==": 4, "=": 4, "!=": 4, "<>": 4, "<": 5, ">": 5, "<=": 5, ">=": 5,
        "+": 6, "-": 6, "*": 7, "/": 7, "%": 7}

_STRF = {"%Y": "yyyy", "%y": "yy", "%m": "MM", "%d": "dd", "%H": "HH", "%M": "mm", "%S": "ss", "%B": "MMMM", "%b": "MMM",
         "%A": "EEEE", "%a": "EEE", "%p": "a", "%I": "hh", "%j": "DDD"}


def _spark_fmt(f: str) -> str:
    for k, v in _STRF.items():
        f = f.replace(k, v)
    return f


def _ds_fmt(f: str) -> str:
    for k, v in (("%yyyy", "yyyy"), ("%yy", "yy"), ("%mm", "MM"), ("%dd", "dd"), ("%hh", "HH"), ("%nn", "mm"), ("%ss", "ss")):
        f = f.replace(k, v)
    return f


def _unesc(s: str) -> str:
    body = s[1:-1]
    return re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t", "r": "\r"}.get(m.group(1), m.group(1)), body)


class Parser:
    def __init__(self, text: str, lang: str):
        self.lang = lang
        toks = []
        for m in TOK.finditer(text):
            if m.lastgroup in ("ws", "comment"):
                continue
            toks.append((m.lastgroup, m.group()))
        pos = sum(len(m.group()) for m in TOK.finditer(text))
        if pos < len(text.rstrip()) and not TOK.fullmatch(text[pos:] or " "):
            raise Unsupported(f"cannot tokenize {text[pos:pos + 12]!r}")
        self.t, self.i = toks, 0
        self.text = text

    def peek(self, k=0):
        return self.t[self.i + k] if self.i + k < len(self.t) else ("eof", "")

    def next(self):
        t = self.peek()
        self.i += 1
        return t

    def isw(self, w, k=0):
        t = self.peek(k)
        return t[0] == "id" and t[1].lower() == w

    def expect(self, v):
        t = self.next()
        if t[1].lower() != v:
            raise Unsupported(f"expected {v!r}, got {t[1]!r}")

    def parse(self):
        n = self.expr()
        if self.i < len(self.t):
            raise Unsupported(f"unexpected {self.peek()[1]!r}")
        return n

    # ------------------------------------------------------------ grammar
    def expr(self):
        if self.lang == "alteryx" and self.isw("if"):
            return self.alteryx_if()
        if self.lang == "datastage" and self.isw("if"):
            self.next()
            c = self.expr()
            self.expect("then")
            a = self.expr()
            self.expect("else")
            return ("tern", c, a, self.expr())
        c = self.binary(1)
        if self.lang in TERN and self.peek()[1] == "?":
            self.next()
            a = self.expr()
            self.expect(":")
            b = self.expr()
            return ("tern", c, a, b)
        return c

    def alteryx_if(self):
        self.expect("if")
        arms = []
        c = self.expr()
        self.expect("then")
        arms.append((c, self.expr()))
        els = None
        while True:
            if self.isw("elseif"):
                self.next()
                c = self.expr()
                self.expect("then")
                arms.append((c, self.expr()))
            elif self.isw("else"):
                self.next()
                els = self.expr()
            elif self.isw("endif"):
                self.next()
                break
            else:
                raise Unsupported("IF without ENDIF")
        return ("if", arms, els)

    def binop(self):
        k, v = self.peek()
        key = v.lower()
        if k == "op" and key == ":":
            return ":" if self.lang == "datastage" else None
        if k == "op" and key in PREC:
            return key
        if k == "id" and key in ("and", "or") and self.lang in WORDOPS:
            return key
        if k == "id" and key == "matches" and self.lang == "pig":
            return "matches"
        if k == "id" and key == "in":
            return "in"
        return None

    def binary(self, minp):
        left = self.unary()
        while True:
            op = self.binop()
            if op == "in" and self.lang == "alteryx":
                self.next()
                self.expect("(")
                items = []
                while self.peek()[1] != ")":
                    items.append(self.expr())
                    if self.peek()[1] == ",":
                        self.next()
                self.expect(")")
                left = ("in", left, items)
                continue
            if self.lang == "pig" and self.isw("is") and 3 >= minp - 0:
                self.next()
                neg = False
                if self.isw("not"):
                    self.next()
                    neg = True
                self.expect("null")
                left = ("isnull", left, neg)
                continue
            if op is None or PREC[op] < minp:
                return left
            self.next()
            right = self.binary(PREC[op] + 1)
            left = ("bin", op, left, right)

    def unary(self):
        k, v = self.peek()
        if v == "!" or (k == "id" and v.lower() == "not" and self.lang in WORDOPS):
            self.next()
            return ("un", "!", self.unary())
        if v == "-":
            self.next()
            return ("un", "-", self.unary())
        if v == "+":
            self.next()
            return self.unary()
        return self.postfix(self.primary())

    def postfix(self, n):
        while self.peek()[1] == "." and self.lang == "java":
            self.next()
            k, name = self.next()
            if k != "id":
                raise Unsupported("bad member access")
            if self.peek()[1] == "(":
                n = ("call", name, self.args(), n)
            else:
                raise Unsupported(f"field access .{name}")
        return n

    def args(self):
        self.expect("(")
        out = []
        while self.peek()[1] != ")":
            if self.peek()[0] == "eof":
                raise Unsupported("unclosed (")
            out.append(self.expr())
            if self.peek()[1] == ",":
                self.next()
        self.expect(")")
        return out

    def primary(self):
        k, v = self.next()
        if k == "num":
            return ("num", v.rstrip("LlFfDd") if not v.lower().startswith("0x") else v)
        if k == "str":
            return ("str", _unesc(v))
        if k == "col":
            return ("col", v[1:-1])
        if v == "(" and self.lang == "ssis" and self.peek()[0] == "id" and self.peek()[1].upper().startswith("DT_"):
            ty = self.next()[1].upper()
            while self.peek()[1] != ")":
                if self.peek()[0] == "eof":
                    raise Unsupported("unclosed cast")
                self.next()
            self.next()
            return ("cast", ty, self.unary())
        if v == "(" and self.lang == "pig" and self.peek()[0] == "id" and self.peek(1)[1] == ")" and self.peek()[1].lower() in ("int", "long", "float", "double", "chararray", "boolean"):
            ty = self.next()[1].lower()
            self.next()
            return ("cast", ty, self.unary())
        if v == "(":
            if self.lang == "java" and self.peek()[0] == "id" and self.peek(1)[1] == ")" and self.peek()[1] in ("int", "long", "double", "float", "String", "Integer", "Long", "Double", "short"):
                ty = self.next()[1]
                self.next()
                return ("cast", ty, self.unary())
            e = self.expr()
            self.expect(")")
            return e
        if k == "id":
            lv = v.lower()
            if lv in ("true", "false"):
                return ("bool", lv == "true")
            if lv == "null" and self.peek()[1] != "(":
                return ("null",)
            if self.lang in ("java", "datastage") and self.peek()[1] == "." and lv not in JAVA_CLASSES:
                self.next()                                 # row.col
                k2, col = self.next()
                if v == "Var":
                    col = "_v_" + col
                if k2 != "id":
                    raise Unsupported("bad column reference")
                if self.peek()[1] == "(":
                    raise Unsupported(f"method {col} on {v}")
                return ("col", col)
            if self.lang == "java" and self.peek()[1] == "." and lv in JAVA_CLASSES:
                self.next()
                k2, m = self.next()
                if lv == "context" and self.peek()[1] != "(":
                    return ("ctx", m)
                return ("call", f"{v}.{m}", self.args(), None)
            if self.peek()[1] == "(":
                return ("call", v, self.args(), None)
            if self.lang in BARE:
                if self.peek()[1] == "::":
                    self.next()
                    v = self.next()[1]
                return ("col", v)
            if self.lang == "alteryx":
                raise Unsupported(f"bare word {v}")
            raise Unsupported(f"unknown identifier {v}")
        raise Unsupported(f"unexpected {v!r}")


# ---------------------------------------------------------------------------------------------- emit
class Emitter:
    def __init__(self, lang: str, types: dict | None = None, warn=None):
        self.lang, self.types, self._warn = lang, types or {}, warn or (lambda m: None)

    def e(self, n):
        t = n[0]
        return getattr(self, "e_" + t)(n)

    def col(self, c):
        return f"F.col({c!r})"

    def e_num(self, n):
        return f"F.lit({n[1]})", "num"

    def e_str(self, n):
        return f"F.lit({n[1]!r})", "str"

    def e_bool(self, n):
        return f"F.lit({n[1]})", "bool"

    def e_null(self, n):
        return "F.lit(None)", "unk"

    def e_col(self, n):
        ty = self.types.get(n[1], "unk")
        return self.col(n[1]), ty

    def e_ctx(self, n):
        self._warn(f"context variable {n[1]} -> ctx[{n[1]!r}]: define a `ctx` dict with your context values")
        return f"F.lit(ctx[{n[1]!r}])", "unk"

    def e_un(self, n):
        c, k = self.e(n[2])
        return (f"(~{c})", "bool") if n[1] == "!" else (f"(-{c})", "num")

    def e_cast(self, n):
        c, _ = self.e(n[2])
        m = {"DT_I1": "tinyint", "DT_I2": "smallint", "DT_I4": "int", "DT_I8": "long", "DT_UI1": "smallint", "DT_UI2": "int", "DT_UI4": "long",
             "DT_R4": "float", "DT_R8": "double", "DT_STR": "string", "DT_WSTR": "string", "DT_BOOL": "boolean", "DT_DATE": "timestamp",
             "DT_DBTIMESTAMP": "timestamp", "DT_NUMERIC": "decimal(38,10)", "DT_DECIMAL": "decimal(28,10)", "DT_CY": "decimal(19,4)",
             "chararray": "string", "boolean": "boolean",
             "int": "int", "Integer": "int", "short": "int", "long": "long", "Long": "long", "double": "double", "Double": "double",
             "float": "float", "String": "string"}
        if n[1] not in m:
            raise Unsupported(f"cast to {n[1]}")
        return f"{c}.cast({m[n[1]]!r})", "str" if m[n[1]] == "string" else "num"

    def e_tern(self, n):
        c, _ = self.e(n[1])
        a, ka = self.e(n[2])
        b, kb = self.e(n[3])
        return f"F.when({c}, {a}).otherwise({b})", ka if ka == kb else "unk"

    def e_if(self, n):
        out, kinds = None, set()
        for c, v in n[1]:
            cc, _ = self.e(c)
            vc, kv = self.e(v)
            kinds.add(kv)
            out = f"F.when({cc}, {vc})" if out is None else f"{out}.when({cc}, {vc})"
        if n[2] is not None:
            ec, ke = self.e(n[2])
            kinds.add(ke)
            out += f".otherwise({ec})"
        return out, kinds.pop() if len(kinds) == 1 else "unk"

    def e_in(self, n):
        x, _ = self.e(n[1])
        return f"{x}.isin([{', '.join(self._plain(i) for i in n[2])}])", "bool"

    def _plain(self, node):
        if node[0] in ("num",):
            return node[1]
        if node[0] == "str":
            return repr(node[1])
        raise Unsupported("IN list must contain literals")

    def e_isnull(self, n):
        c, _ = self.e(n[1])
        return (f"{c}.isNotNull()" if n[2] else f"{c}.isNull()"), "bool"

    def e_bin(self, n):
        op, l, r = n[1], n[2], n[3]
        if op == "matches":
            lc, _ = self.e(l)
            return f"{lc}.rlike({self._val(self.e(r))})", "bool"
        if op in ("==", "=", "!=", "<>") and (l[0] == "null" or r[0] == "null"):
            other = r if l[0] == "null" else l
            c, _ = self.e(other)
            return (f"{c}.isNull()" if op in ("==", "=") else f"{c}.isNotNull()"), "bool"
        lc, lk = self.e(l)
        rc, rk = self.e(r)
        if op == ":":
            return f"F.concat({self._s(lc, lk)}, {self._s(rc, rk)})", "str"
        if op in ("&&", "and"):
            return f"({lc} & {rc})", "bool"
        if op in ("||", "or"):
            return f"({lc} | {rc})", "bool"
        if op in ("==", "="):
            return f"({lc} == {rc})", "bool"
        if op in ("!=", "<>"):
            return f"({lc} != {rc})", "bool"
        if op in ("<", ">", "<=", ">="):
            return f"({lc} {op} {rc})", "bool"
        if op == "+":
            if "str" in (lk, rk):
                return f"F.concat({self._s(lc, lk)}, {self._s(rc, rk)})", "str"
            if lk == "unk" and rk == "unk":
                self._warn("`+` between values of unknown type is emitted as numeric addition; use F.concat if they are strings")
            return f"({lc} + {rc})", "num"
        return f"({lc} {op} {rc})", "num"

    def _s(self, c, k):
        return c if k == "str" else f"{c}.cast('string')"

    # ---------------------------------------------------------------- calls
    def e_call(self, n):
        _, name, args, recv = n
        a = [self.e(x) for x in args]
        c = [x[0] for x in a]
        if recv is not None:
            r, rk = self.e(recv)
            return self.method(name, r, rk, c, a)
        fn = {"java": self.java_static, "alteryx": self.alteryx_fn, "pig": self.pig_fn, "ssis": self.ssis_fn, "datastage": self.ds_fn}[self.lang]
        return fn(name, c, a, args)

    # ------------------------------------------------------------ Java
    def method(self, m, r, rk, c, a):
        ml = m
        if ml == "trim":
            return f"F.trim({r})", "str"
        if ml == "toUpperCase":
            return f"F.upper({r})", "str"
        if ml == "toLowerCase":
            return f"F.lower({r})", "str"
        if ml == "length":
            return f"F.length({r})", "num"
        if ml == "isEmpty":
            return f"(F.length({r}) == 0)", "bool"
        if ml == "substring":
            if len(c) == 1:
                return f"{r}.substr({c[0]} + 1, F.length({r}))", "str"
            return f"{r}.substr({c[0]} + 1, {c[1]} - {c[0]})", "str"
        if ml == "equals":
            return f"({r} == {c[0]})", "bool"
        if ml == "equalsIgnoreCase":
            return f"(F.lower({r}) == F.lower({c[0]}))", "bool"
        if ml == "contains":
            return f"{r}.contains({c[0]})", "bool"
        if ml == "startsWith":
            return f"{r}.startswith({c[0]})", "bool"
        if ml == "endsWith":
            return f"{r}.endswith({c[0]})", "bool"
        if ml == "replace":
            return f"F.regexp_replace({r}, {self._esc(a[0])}, {self._val(a[1])})", "str"
        if ml == "indexOf":
            return f"(F.locate({self._val(a[0])}, {r}) - 1)", "num"
        if ml == "concat":
            return f"F.concat({r}, {c[0]})", "str"
        if ml == "toString":
            return f"{r}.cast('string')", "str"
        if ml in ("intValue", "longValue", "doubleValue"):
            return f"{r}.cast({ml[:-5]!r})", "num"
        raise Unsupported(f"method .{m}()")

    def _esc(self, ac):
        import ast
        try:
            return repr(re.escape(ast.literal_eval(self._val(ac))))
        except (ValueError, SyntaxError):
            raise Unsupported("argument must be a string literal")

    def _val(self, ac):
        code = ac[0]
        m = re.fullmatch(r"F\.lit\((.*)\)", code)
        if not m:
            raise Unsupported("argument must be a literal")
        return m.group(1)

    def java_static(self, name, c, a, args):
        n = name.lower()
        if n in ("stringhandling.upcase", "talendstring.upcase"):
            return f"F.upper({c[0]})", "str"
        if n == "stringhandling.downcase":
            return f"F.lower({c[0]})", "str"
        if n == "stringhandling.trim":
            return f"F.trim({c[0]})", "str"
        if n == "stringhandling.ltrim":
            return f"F.ltrim({c[0]})", "str"
        if n == "stringhandling.rtrim":
            return f"F.rtrim({c[0]})", "str"
        if n == "stringhandling.len":
            return f"F.length({c[0]})", "num"
        if n == "stringhandling.left":
            return f"F.substring({c[0]}, 1, {self._val(a[1])})", "str"
        if n == "stringhandling.right":
            return f"F.substring({c[0]}, -{self._val(a[1])}, {self._val(a[1])})", "str"
        if n == "stringhandling.index":
            return f"(F.locate({self._val(a[1])}, {c[0]}))", "num"
        if n == "stringhandling.change":
            return f"F.regexp_replace({c[0]}, {self._esc(a[1])}, {self._val(a[2])})", "str"
        if n == "stringhandling.isempty" or n == "stringhandling.empty":
            return f"(F.length({c[0]}) == 0)", "bool"
        if n == "relational.isnull":
            return f"{c[0]}.isNull()", "bool"
        if n == "relational.not":
            return f"(~{c[0]})", "bool"
        if n in ("math.abs", "math.sqrt", "math.ceil", "math.floor", "math.exp", "math.log"):
            f = {"abs": "abs", "sqrt": "sqrt", "ceil": "ceil", "floor": "floor", "exp": "exp", "log": "log"}[n.split(".")[1]]
            return f"F.{f}({c[0]})", "num"
        if n == "math.round":
            return f"F.round({c[0]})", "num"
        if n == "math.pow":
            return f"F.pow({c[0]}, {c[1]})", "num"
        if n == "math.max":
            return f"F.greatest({c[0]}, {c[1]})", "num"
        if n == "math.min":
            return f"F.least({c[0]}, {c[1]})", "num"
        if n in ("integer.parseint", "integer.valueof"):
            return f"{c[0]}.cast('int')", "num"
        if n in ("long.parselong", "long.valueof"):
            return f"{c[0]}.cast('long')", "num"
        if n in ("double.parsedouble", "double.valueof"):
            return f"{c[0]}.cast('double')", "num"
        if n == "string.valueof":
            return f"{c[0]}.cast('string')", "str"
        if n == "talenddate.getcurrentdate":
            return "F.current_timestamp()", "date"
        if n == "talenddate.formatdate":
            return f"F.date_format({c[1]}, {c[0]})", "str"
        if n == "talenddate.parsedate":
            return f"F.to_timestamp({c[1]}, {c[0]})", "date"
        if n == "talenddate.adddate":
            unit = (self._val(a[2]).strip("'\"") if len(a) > 2 else "")
            if unit == "dd":
                return f"F.date_add({c[0]}, {c[1]})", "date"
            if unit == "MM":
                return f"F.add_months({c[0]}, {c[1]})", "date"
            if unit == "yyyy":
                return f"F.add_months({c[0]}, {c[1]} * 12)", "date"
            raise Unsupported(f"TalendDate.addDate unit {unit!r}")
        if n == "talenddate.diffdate":
            unit = (self._val(a[2]).strip("'\"") if len(a) > 2 else "dd")
            if unit == "dd":
                return f"F.datediff({c[0]}, {c[1]})", "num"
            raise Unsupported(f"TalendDate.diffDate unit {unit!r}")
        if n == "talenddate.getpartofdate":
            part = self._val(a[0]).strip("'\"")
            f = {"YEAR": "year", "MONTH": "month", "DAY_OF_MONTH": "dayofmonth", "HOUR": "hour", "MINUTE": "minute", "SECOND": "second"}.get(part)
            if not f:
                raise Unsupported(f"getPartOfDate {part}")
            return f"F.{f}({c[1]})", "num"
        raise Unsupported(f"routine {name}")

    # ------------------------------------------------------------ Alteryx
    def alteryx_fn(self, name, c, a, args):
        n = name.lower()
        k0 = a[0][1] if a else "unk"
        simple = {"uppercase": ("upper", "str"), "lowercase": ("lower", "str"), "trim": ("trim", "str"), "trimleft": ("ltrim", "str"),
                  "trimright": ("rtrim", "str"), "length": ("length", "num"), "abs": ("abs", "num"), "ceil": ("ceil", "num"),
                  "floor": ("floor", "num"), "sqrt": ("sqrt", "num"), "log": ("log", "num"), "exp": ("exp", "num"),
                  "titlecase": ("initcap", "str"), "datetimeyear": ("year", "num"), "datetimemonth": ("month", "num"),
                  "datetimeday": ("dayofmonth", "num"), "datetimehour": ("hour", "num"), "datetimeminute": ("minute", "num"),
                  "datetimesecond": ("second", "num"), "reverse": ("reverse", "str")}
        if n in simple and len(c) == 1:
            f, k = simple[n]
            return f"F.{f}({c[0]})", k
        if n == "null":
            return "F.lit(None)", "unk"
        if n == "isnull":
            return f"{c[0]}.isNull()", "bool"
        if n == "isempty":
            return f"({c[0]}.isNull() | (F.trim({c[0]}) == ''))", "bool"
        if n == "iif":
            return f"F.when({c[0]}, {c[1]}).otherwise({c[2]})", a[1][1]
        if n == "contains":
            return f"{c[0]}.contains({c[1]})", "bool"
        if n == "startswith":
            return f"{c[0]}.startswith({c[1]})", "bool"
        if n == "endswith":
            return f"{c[0]}.endswith({c[1]})", "bool"
        if n == "left":
            return f"F.substring({c[0]}, 1, {self._num(a[1])})", "str"
        if n == "right":
            return f"F.substring({c[0]}, -{self._num(a[1])}, {self._num(a[1])})", "str"
        if n == "substring":
            st = self._num(a[1])
            if len(a) > 2:
                return f"F.substring({c[0]}, {st} + 1, {self._num(a[2])})", "str"
            return f"F.substring({c[0]}, {st} + 1, 2147483647)", "str"
        if n == "findstring":
            return f"(F.locate({self._val(a[1])}, {c[0]}) - 1)", "num"
        if n == "replace":
            return f"F.regexp_replace({c[0]}, {self._esc(a[1])}, {self._val(a[2])})", "str"
        if n in ("padleft", "padright"):
            fn = "lpad" if n == "padleft" else "rpad"
            return f"F.{fn}({c[0]}, {self._num(a[2])}, {self._val(a[1])})", "str"
        if n == "tonumber":
            return f"{c[0]}.cast('double')", "num"
        if n == "tostring":
            if len(a) > 1:
                return f"F.format_number({c[0]}, {self._num(a[1])})", "str"
            return f"{c[0]}.cast('string')", "str"
        if n == "round":
            if len(a) == 1:
                return f"F.round({c[0]})", "num"
            fac = self._val(a[1])
            try:
                f = float(fac.strip("'\""))
            except ValueError:
                raise Unsupported("Round factor must be a literal")
            if f == 1:
                return f"F.round({c[0]})", "num"
            if 0 < f < 1:
                import math
                return f"F.round({c[0]}, {int(round(-math.log10(f)))})", "num"
            return f"(F.round({c[0]} / {fac}) * {fac})", "num"
        if n == "pow":
            return f"F.pow({c[0]}, {c[1]})", "num"
        if n == "mod":
            return f"({c[0]} % {c[1]})", "num"
        if n in ("min", "max") and len(c) >= 2:
            return f"F.{'least' if n == 'min' else 'greatest'}({', '.join(c)})", "num"
        if n == "datetimenow":
            return "F.current_timestamp()", "date"
        if n == "datetimetoday":
            return "F.current_date()", "date"
        if n == "datetimeformat":
            return f"F.date_format({c[0]}, {_spark_fmt(self._val(a[1]))})", "str"
        if n == "datetimeparse":
            return f"F.to_timestamp({c[0]}, {_spark_fmt(self._val(a[1]))})", "date"
        if n == "datetimeadd":
            unit = self._val(a[2]).strip("'\"").lower()
            if unit in ("day", "days"):
                return f"F.date_add({c[0]}, {c[1]})", "date"
            if unit in ("month", "months"):
                return f"F.add_months({c[0]}, {c[1]})", "date"
            if unit in ("year", "years"):
                return f"F.add_months({c[0]}, {c[1]} * 12)", "date"
            raise Unsupported(f"DateTimeAdd unit {unit!r}")
        if n == "datetimediff":
            unit = self._val(a[2]).strip("'\"").lower()
            if unit in ("day", "days"):
                return f"F.datediff({c[0]}, {c[1]})", "num"
            if unit in ("month", "months"):
                return f"F.months_between({c[0]}, {c[1]})", "num"
            raise Unsupported(f"DateTimeDiff unit {unit!r}")
        if n == "regex_match":
            return f"{c[0]}.rlike({c[1]})", "bool"
        if n == "regex_replace":
            return f"F.regexp_replace({c[0]}, {self._val(a[1])}, {self._val(a[2])})", "str"
        raise Unsupported(f"function {name}")

    def pig_fn(self, name, c, a, args):
        n = name.upper()
        one = {"UPPER": ("upper", "str"), "LOWER": ("lower", "str"), "TRIM": ("trim", "str"), "LTRIM": ("ltrim", "str"), "RTRIM": ("rtrim", "str"),
               "SIZE": ("length", "num"), "ABS": ("abs", "num"), "SQRT": ("sqrt", "num"), "CEIL": ("ceil", "num"), "FLOOR": ("floor", "num"),
               "ROUND": ("round", "num"), "EXP": ("exp", "num"), "LOG": ("log", "num"), "ToDate": ("to_timestamp", "date")}
        if n in one and len(c) == 1:
            return f"F.{one[n][0]}({c[0]})", one[n][1]
        if n == "CONCAT":
            return f"F.concat({', '.join(c)})", "str"
        if n == "SUBSTRING":                         # 0-based start, exclusive end
            return f"F.substring({c[0]}, {self._num(a[1])} + 1, {self._num(a[2])} - {self._num(a[1])})", "str"
        if n == "INDEXOF":
            return f"(F.locate({self._val(a[1])}, {c[0]}) - 1)", "num"
        if n == "REPLACE":
            return f"F.regexp_replace({c[0]}, {self._val(a[1])}, {self._val(a[2])})", "str"
        if n in ("COUNT", "COUNT_STAR", "SUM", "AVG", "MIN", "MAX"):
            raise Unsupported("aggregate outside GROUP context")
        if n == "ISEMPTY":
            return f"({c[0]}.isNull() | (F.length({c[0]}) == 0))", "bool"
        if n == "TOLOWER":
            return f"F.lower({c[0]})", "str"
        if n == "TOUPPER":
            return f"F.upper({c[0]})", "str"
        raise Unsupported(f"function {name}")

    def ds_fn(self, name, c, a, args):
        n = name.lower()
        one = {"trim": ("trim", "str"), "upcase": ("upper", "str"), "downcase": ("lower", "str"), "len": ("length", "num"), "abs": ("abs", "num"),
               "sqrt": ("sqrt", "num"), "ceil": ("ceil", "num"), "floor": ("floor", "num")}
        if n in one and len(c) == 1:
            return f"F.{one[n][0]}({c[0]})", one[n][1]
        if n == "isnull":
            return f"{c[0]}.isNull()", "bool"
        if n == "isnotnull":
            return f"{c[0]}.isNotNull()", "bool"
        if n == "nulltovalue":
            return f"F.coalesce({c[0]}, {c[1]})", a[1][1]
        if n == "nulltoempty":
            return f"F.coalesce({c[0]}, F.lit(''))", "str"
        if n == "nulltozero":
            return f"F.coalesce({c[0]}, F.lit(0))", "num"
        if n == "left":
            return f"F.substring({c[0]}, 1, {self._num(a[1])})", "str"
        if n == "right":
            return f"F.substring({c[0]}, -{self._num(a[1])}, {self._num(a[1])})", "str"
        if n == "int":
            return f"{c[0]}.cast('int')", "num"
        if n == "index":
            if len(a) > 2 and self._num(a[2]) != "1":
                raise Unsupported("Index occurrence > 1")
            return f"F.locate({self._val(a[1])}, {c[0]})", "num"
        if n == "ereplace" or n == "change":
            return f"F.regexp_replace({c[0]}, {self._esc(a[1])}, {self._val(a[2])})", "str"
        if n == "field":
            return f"F.split({c[0]}, {self._esc(a[1])}).getItem({self._num(a[2])} - 1)", "str"
        if n in ("currenttimestamp", "currenttimestampms"):
            return "F.current_timestamp()", "date"
        if n == "currentdate":
            return "F.current_date()", "date"
        if n in ("stringtodate", "stringtotimestamp"):
            return f"F.to_timestamp({c[0]}, {_ds_fmt(self._val(a[1]))})", "date"
        if n in ("datetostring", "timestamptostring"):
            return f"F.date_format({c[0]}, {_ds_fmt(self._val(a[1]))})", "str"
        if n == "str":
            return f"F.repeat({c[0]}, {self._num(a[1])})", "str"
        raise Unsupported(f"function {name}")

    def ssis_fn(self, name, c, a, args):
        n = name.upper()
        one = {"UPPER": ("upper", "str"), "LOWER": ("lower", "str"), "TRIM": ("trim", "str"), "LTRIM": ("ltrim", "str"), "RTRIM": ("rtrim", "str"),
               "LEN": ("length", "num"), "ABS": ("abs", "num"), "SQRT": ("sqrt", "num"), "CEILING": ("ceil", "num"), "FLOOR": ("floor", "num"),
               "YEAR": ("year", "num"), "MONTH": ("month", "num"), "DAY": ("dayofmonth", "num"), "EXP": ("exp", "num"), "LN": ("log", "num"),
               "REVERSE": ("reverse", "str")}
        if n in one and len(c) == 1:
            return f"F.{one[n][0]}({c[0]})", one[n][1]
        if n == "ISNULL":
            return f"{c[0]}.isNull()", "bool"
        if n == "REPLACENULL":
            return f"F.coalesce({c[0]}, {c[1]})", a[1][1]
        if n == "SUBSTRING":                         # 1-based
            return f"{c[0]}.substr({c[1]}, {c[2]})", "str"
        if n == "LEFT":
            return f"F.substring({c[0]}, 1, {self._num(a[1])})", "str"
        if n == "RIGHT":
            return f"F.substring({c[0]}, -{self._num(a[1])}, {self._num(a[1])})", "str"
        if n == "REPLACE":
            return f"F.regexp_replace({c[0]}, {self._esc(a[1])}, {self._val(a[2])})", "str"
        if n == "FINDSTRING":
            if len(a) > 2 and self._num(a[2]) != "1":
                raise Unsupported("FINDSTRING occurrence > 1")
            return f"F.locate({self._val(a[1])}, {c[0]})", "num"
        if n == "GETDATE":
            return "F.current_timestamp()", "date"
        if n == "ROUND":
            return f"F.round({c[0]}, {self._num(a[1])})" if len(a) > 1 else f"F.round({c[0]})", "num"
        if n == "POWER":
            return f"F.pow({c[0]}, {c[1]})", "num"
        if n == "DATEADD":
            unit = self._val(a[0]).strip("'\"").lower()
            if unit in ("day", "dd", "d"):
                return f"F.date_add({c[2]}, {c[1]})", "date"
            if unit in ("month", "mm", "m"):
                return f"F.add_months({c[2]}, {c[1]})", "date"
            if unit in ("year", "yy", "yyyy"):
                return f"F.add_months({c[2]}, {c[1]} * 12)", "date"
            raise Unsupported(f"DATEADD unit {unit}")
        if n == "DATEDIFF":
            unit = self._val(a[0]).strip("'\"").lower()
            if unit in ("day", "dd", "d"):
                return f"F.datediff({c[2]}, {c[1]})", "num"
            raise Unsupported(f"DATEDIFF unit {unit}")
        raise Unsupported(f"function {name}")

    def _num(self, ac):
        code = ac[0]
        m = re.fullmatch(r"F\.lit\((-?\d+)\)", code)
        if not m:
            raise Unsupported("argument must be an integer literal")
        return m.group(1)


def translate_ex(text: str, lang: str, types: dict | None = None, warn=None):
    """Like translate() but returns (code, kind)."""
    ast = Parser(text, lang).parse()
    return Emitter(lang, types, warn).e(ast)


def translate(text: str, lang: str, types: dict | None = None, warn=None) -> str:
    """Return PySpark Column code for a Talend ('java') or Alteryx ('alteryx') expression, or raise Unsupported."""
    return translate_ex(text, lang, types, warn)[0]
