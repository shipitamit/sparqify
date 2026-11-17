"""Informatica expression language (PowerCenter + IDMC) -> PySpark Column expressions."""
from __future__ import annotations

import re

from .core import Report, Unsupported, strip_parens
from .pratt import Pratt, tokenize
from .sql_conv import fmt_convert

TOK = re.compile(r"""(?P<ws>\s+)|(?P<comment>--[^\n]*|//[^\n]*)
 |(?P<num>\d+\.?\d*(?:[eE][+-]?\d+)?)|(?P<str>'(?:[^']|'')*')
 |(?P<param>\$\$\w+|\$\w+\$)|(?P<lkp>:LKP\.\w+)
 |(?P<word>(?i:\b(?:and|or|not)\b))|(?P<id>[A-Za-z_][\w$#]*)
 |(?P<op>\|\||<>|!=|<=|>=|[-+*/%=<>(),])""", re.X)
BP = {"or": 10, "and": 12, "=": 20, "<>": 20, "!=": 20, "<": 20, ">": 20, "<=": 20, ">=": 20, "||": 25,
      "+": 30, "-": 30, "*": 40, "/": 40, "%": 40}
UN = {"-": 50, "+": 50, "not": 15}
AGG = {"SUM": "sum", "AVG": "avg", "MIN": "min", "MAX": "max", "COUNT": "count", "FIRST": "first", "LAST": "last",
       "MEDIAN": "median", "STDDEV": "stddev", "VARIANCE": "variance"}
SIMPLE = {"UPPER": "upper", "LOWER": "lower", "LTRIM": "ltrim", "RTRIM": "rtrim", "LENGTH": "length", "ABS": "abs",
          "CEIL": "ceil", "FLOOR": "floor", "SQRT": "sqrt", "EXP": "exp", "LN": "log", "INITCAP": "initcap",
          "SIGN": "signum", "REVERSE": "reverse", "LAST_DAY": "last_day", "SYSDATE": "current_timestamp"}
DATEPART = {"YYYY": "year", "YYY": "year", "YY": "year", "Y": "year", "YEAR": "year", "MM": "month", "MON": "month",
            "MONTH": "month", "DD": "dayofmonth", "D": "dayofmonth", "DAY": "dayofmonth", "HH": "hour", "HH12": "hour",
            "HH24": "hour", "MI": "minute", "SS": "second"}


def strip_q(s):
    return s[1:-1].replace("''", "'")


class InfaExpr:
    def __init__(self, report: Report, params_var="params"):
        self.r = report
        self.pv = params_var
        self.where = ""

    def warn(self, m):
        self.r.warn(self.where, m)

    def parse(self, text):
        text = re.sub(r"\(\s*\*\s*\)", "(1)", text.strip())
        toks = [("id", v) if k in ("param", "lkp") else (k, v) for k, v in tokenize(text, TOK, skip=("ws", "comment"))]
        p = Pratt(toks, BP, UN, "Informatica expression")
        n = p.expr()
        if not p.done():
            raise Unsupported(f"cannot parse expression `{text[:60]}`")
        return n

    def code(self, text, where=""):
        self.where = where
        return strip_parens(self.ex(self.parse(text)))

    # ---------------------------------------------------------------- nodes
    def ex(self, n):
        t = n[0]
        if t == "num":
            return n[1]
        if t == "str":
            return repr(strip_q(n[1]))
        if t == "paren":
            return f"({self.ex(n[1])})"
        if t in ("id", "ref"):
            return self.ident(n[1])
        if t == "un":
            return {"-": f"(-{self.ex(n[2])})", "+": self.ex(n[2]), "not": f"(~{self.ex(n[2])})"}[n[1]]
        if t == "bin":
            op = n[1]
            l, r = self.ex(n[2]), self.ex(n[3])
            if op == "=":
                return f"({l} == {r})"
            if op in ("<>", "!="):
                return f"({l} != {r})"
            if op == "and":
                return f"({l} & {r})"
            if op == "or":
                return f"({l} | {r})"
            if op == "||":
                return f"F.concat({self.s(n[2])}, {self.s(n[3])})"
            return f"({l} {op} {r})"
        if t == "call":
            return self.call(n[1].upper(), n[2])
        raise Unsupported("unsupported expression element")

    def s(self, n):
        c = self.ex(n)
        return f"F.lit({c})" if n[0] in ("str", "num") else c

    def ident(self, name):
        u = name.upper()
        if name.startswith("$$") or (name.startswith("$") and name.endswith("$")):
            key = name.strip("$")
            return f"F.lit({self.pv}[{key!r}])"
        if u == "TRUE":
            return "F.lit(True)"
        if u == "FALSE":
            return "F.lit(False)"
        if u == "NULL":
            return "F.lit(None)"
        if u in ("SYSDATE", "SYSTIMESTAMP", "SESSSTARTTIME"):
            if u == "SESSSTARTTIME":
                self.warn("SESSSTARTTIME evaluated as current_timestamp() at run time (Informatica fixes it per session)")
            return "F.current_timestamp()"
        if u.startswith(("DD_", "$PM")):
            raise Unsupported(f"{name} is Informatica session/update-strategy specific")
        return f"F.col({name!r})"

    def lit_str(self, n, what):
        if n[0] != "str":
            raise Unsupported(f"{what} must be a string literal")
        return strip_q(n[1])

    def call(self, f, a):
        e = self.ex
        if f in AGG:
            if f == "COUNT" and (not a or a[0] == ("num", "1")):
                return "F.count(F.lit(1))"
            inner = e(a[0])
            if len(a) > 1:                      # SUM(x, filter)
                inner = f"F.when({strip_parens(e(a[1]))}, {inner})"
            return f"F.{AGG[f]}({inner})"
        if f in SIMPLE:
            return f"F.{SIMPLE[f]}({', '.join(e(x) for x in a)})" if a else f"F.{SIMPLE[f]}()"
        if f == "IIF":
            els = self.s(a[2]) if len(a) > 2 else "F.lit(None)"
            return f"F.when({strip_parens(e(a[0]))}, {self.s(a[1])}).otherwise({els})"
        if f == "DECODE":
            return self.decode(a)
        if f == "ISNULL":
            return f"{e(a[0])}.isNull()"
        if f == "IS_SPACES":
            return f"(F.trim({e(a[0])}) == '')"
        if f == "IN":
            return f"{e(a[0])}.isin({', '.join(e(x) for x in a[1:])})"
        if f == "TRIM":
            return f"F.trim({e(a[0])})"
        if f == "SUBSTR":
            ln = e(a[2]) if len(a) > 2 else str(2 ** 31 - 1)
            return f"F.substring({e(a[0])}, {e(a[1])}, {ln})"
        if f == "INSTR":
            if len(a) == 2 and a[1][0] == "str":
                return f"F.instr({e(a[0])}, {e(a[1])})"
            if len(a) >= 3:
                if len(a) > 3:
                    self.warn("INSTR occurrence argument ignored")
                return f"F.locate({e(a[1])}, {e(a[0])}, {e(a[2])})"
            raise Unsupported("INSTR with a non-literal search string")
        if f == "CONCAT":
            return f"F.concat({', '.join(self.s(x) for x in a)})"
        if f == "REPLACESTR":
            src = e(a[1])
            repl = self.s(a[-1]) if False else e(a[-1])
            code = src
            for old in a[2:-1]:
                code = f"F.regexp_replace({code}, {re.escape(self.lit_str(old, 'REPLACESTR search'))!r}, {self.lit_str(a[-1], 'REPLACESTR replacement')!r})"
            return code
        if f == "REPLACECHR":
            chars = re.sub(r"([\]\\^-])", r"\\\1", self.lit_str(a[2], "REPLACECHR chars"))
            return f"F.regexp_replace({e(a[1])}, {'[' + chars + ']'!r}, {self.lit_str(a[3], 'REPLACECHR replacement') if len(a) > 3 else ''!r})"
        if f in ("LPAD", "RPAD"):
            pad = e(a[2]) if len(a) > 2 else "' '"
            return f"F.{f.lower()}({e(a[0])}, {e(a[1])}, {pad})"
        if f == "TO_CHAR":
            if len(a) > 1 and a[1][0] == "str":
                return f"F.date_format({e(a[0])}, {fmt_convert(a[1][1], 'oracle')[1:-1]!r})"
            return f"{e(a[0])}.cast('string')"
        if f in ("TO_DATE", "TO_TIMESTAMP"):
            if len(a) > 1 and a[1][0] == "str":
                return f"F.to_timestamp({e(a[0])}, {fmt_convert(a[1][1], 'oracle')[1:-1]!r})"
            return f"F.to_timestamp({e(a[0])})"
        if f in ("TO_INTEGER", "TO_BIGINT", "TO_FLOAT", "TO_DECIMAL"):
            t = {"TO_INTEGER": "int", "TO_BIGINT": "bigint", "TO_FLOAT": "double"}.get(f)
            if f == "TO_DECIMAL":
                t = f"decimal(28,{self.lit_str(a[1], 'scale') if len(a) > 1 and a[1][0] == 'str' else (a[1][1] if len(a) > 1 else '0')})"
            return f"{e(a[0])}.cast({t!r})"
        if f == "ADD_TO_DATE":
            part = self.lit_str(a[1], "date part").upper()
            d, n = e(a[0]), e(a[2])
            if part in ("DD", "D", "DAY", "DY"):
                return f"({d} + {n} * F.expr('INTERVAL 1 DAY'))"
            if part in ("MM", "MON", "MONTH"):
                return f"F.add_months({d}, {n})"
            if part in ("YYYY", "YY", "Y", "YEAR"):
                return f"F.add_months({d}, 12 * {n})"
            unit = {"HH": "HOUR", "HH12": "HOUR", "HH24": "HOUR", "MI": "MINUTE", "SS": "SECOND"}.get(part)
            if unit:
                return f"({d} + {n} * F.expr('INTERVAL 1 {unit}'))"
            raise Unsupported(f"ADD_TO_DATE part {part}")
        if f == "DATE_DIFF":
            part = self.lit_str(a[2], "date part").upper()
            d1, d2 = e(a[0]), e(a[1])
            if part in ("DD", "D", "DAY"):
                return f"F.datediff({d1}, {d2})"
            if part in ("MM", "MON", "MONTH"):
                return f"F.months_between({d1}, {d2})"
            if part in ("YYYY", "YY", "Y", "YEAR"):
                return f"F.floor(F.months_between({d1}, {d2}) / 12)"
            div = {"HH": 3600, "HH12": 3600, "HH24": 3600, "MI": 60, "SS": 1}.get(part)
            if div:
                return f"((F.unix_timestamp({d1}) - F.unix_timestamp({d2})) / {div})"
            raise Unsupported(f"DATE_DIFF part {part}")
        if f == "GET_DATE_PART":
            part = self.lit_str(a[1], "date part").upper()
            if part in DATEPART:
                return f"F.{DATEPART[part]}({e(a[0])})"
            raise Unsupported(f"GET_DATE_PART {part}")
        if f == "TRUNC":
            if len(a) == 2 and a[1][0] == "str":
                part = strip_q(a[1][1]).upper()
                m = {"MM": "MM", "MON": "MM", "MONTH": "MM", "YYYY": "YEAR", "YY": "YEAR", "Y": "YEAR", "YEAR": "YEAR",
                     "Q": "QUARTER", "DD": None, "D": None, "DAY": None, "HH": "HOUR", "MI": "MINUTE", "SS": "SECOND"}.get(part, "?")
                if m == "?":
                    raise Unsupported(f"TRUNC date part {part}")
                if m is None:
                    return f"F.to_date({e(a[0])})"
                return f"F.date_trunc({m!r}, {e(a[0])})" if m in ("HOUR", "MINUTE", "SECOND") else f"F.trunc({e(a[0])}, {m!r})"
            if len(a) == 1:
                self.warn("TRUNC(x) assumed numeric (truncate toward zero); for dates use TRUNC(date, 'DD')")
                x = e(a[0])
                return f"F.when({x} >= 0, F.floor({x})).otherwise(F.ceil({x}))"
            raise Unsupported("TRUNC(number, scale)")
        if f == "ROUND":
            if len(a) > 1 and a[1][0] == "str":
                raise Unsupported("ROUND(date, fmt)")
            return f"F.round({e(a[0])}, {e(a[1]) if len(a) > 1 else 0})"
        if f == "MOD":
            return f"({e(a[0])} % {e(a[1])})"
        if f == "POWER":
            return f"F.pow({e(a[0])}, {e(a[1])})"
        if f == "LOG":
            return f"F.log({e(a[0])}, {e(a[1])})"
        if f == "REG_EXTRACT":
            return f"F.regexp_extract({e(a[0])}, {e(a[1])}, {e(a[2]) if len(a) > 2 else 1})"
        if f == "REG_REPLACE":
            return f"F.regexp_replace({e(a[0])}, {e(a[1])}, {e(a[2]) if len(a) > 2 else repr('')})"
        if f == "REG_MATCH":
            if a[1][0] != "str":
                raise Unsupported("REG_MATCH with a non-literal pattern")
            return f"{e(a[0])}.rlike({('^(' + strip_q(a[1][1]) + ')$')!r})"
        if f in ("ERROR", "ABORT"):
            raise Unsupported(f"{f}() row-level error handling has no Spark equivalent (filter bad rows to a reject table)")
        if f.startswith(":LKP."):
            raise Unsupported(f"unconnected lookup {f}(...) - convert it to a left join against the lookup table")
        raise Unsupported(f"function {f}() has no automatic conversion")

    def decode(self, a):
        if len(a) < 3:
            raise Unsupported("DECODE needs at least 3 arguments")
        subject, rest = a[0], a[1:]
        boolean = subject in (("id", "TRUE"), ("id", "true"))
        code = None
        for i in range(0, len(rest) - 1, 2):
            cond = strip_parens(self.ex(rest[i])) if boolean else f"{self.ex(subject)} == {self.ex(rest[i])}"
            val = self.s(rest[i + 1])
            code = f"F.when({cond}, {val})" if code is None else f"{code}.when({cond}, {val})"
        if len(rest) % 2:
            code += f".otherwise({self.s(rest[-1])})"
        return code
