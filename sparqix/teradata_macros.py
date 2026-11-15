"""Teradata macros: REPLACE/CREATE MACRO name (params) AS ( stmt; stmt; ) and EXEC name(args).

A macro becomes a Python function that runs its statements with spark.sql(..., args={...}): Teradata's `:param` markers are exactly Spark's named
parameter markers (Spark 3.4+), so values are bound, never pasted into the SQL text. EXEC calls with literal arguments become calls of that function.
"""
from __future__ import annotations

import keyword
import re

from .teradata_sql import _skip_literal, match_paren, split_args

_HEAD = re.compile(r"(?is)\b(?:REPLACE|CREATE)\s+MACRO\s+([\w.\"]+)\s*")


def extract_macros(source: str, ctx):
    """-> (source with the macro definitions removed and EXEC statements turned into marker comments, [macro dicts])"""
    macros, out, pos = [], [], 0
    while True:
        m = _HEAD.search(source, pos)
        if not m:
            out.append(source[pos:])
            break
        j = m.end()
        params = []
        if j < len(source) and source[j] == "(":
            c = match_paren(source, j)
            if c < 0:
                out.append(source[pos:])
                break
            for p in split_args(source[j + 1:c]):
                pm = re.match(r"(?is)^(\w+)\s+(.*)$", p.strip())
                if pm:
                    params.append(pm.group(1))
            j = c + 1
        am = re.match(r"(?is)\s*AS\s*\(", source[j:])
        if not am:
            out.append(source[pos:j])
            pos = j
            continue
        o = j + am.end() - 1
        c = match_paren(source, o)
        if c < 0:
            out.append(source[pos:])
            break
        body = source[o + 1:c]
        stmts = [s.strip() for s in _split_top(body) if s.strip()]
        macros.append({"name": m.group(1).split(".")[-1].strip('"'), "params": params, "stmts": stmts})
        out.append(source[pos:m.start()])
        k = c + 1
        while k < len(source) and source[k] in " \t\r\n;":
            k += 1
        pos = k
    text = "".join(out)

    execs = ctx.execs = getattr(ctx, "execs", [])

    def ex(m, args=""):
        execs.append(f"-- __EXEC__ {m.group(1)}({m.group(2) if m.re.groups > 1 else ''})")
        return f"SELECT 'SPARQIX_EXEC_{len(execs) - 1}';"
    text = re.sub(r"(?im)^[ \t]*EXEC(?:UTE)?[ \t]+([\w.\"]+)[ \t]*\((.*?)\)[ \t]*;", ex, text)
    text = re.sub(r"(?im)^[ \t]*EXEC(?:UTE)?[ \t]+([\w.\"]+)[ \t]*;", ex, text)
    return text, macros


def _split_top(body):
    out, cur, depth, j = [], [], 0, 0
    while j < len(body):
        k = _skip_literal(body, j)
        if k != j:
            cur.append(body[j:k])
            j = k
            continue
        ch = body[j]
        depth += ch == "("
        depth -= ch == ")"
        if ch == ";" and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
        j += 1
    out.append("".join(cur))
    return out


def _py(name):
    n = re.sub(r"\W", "_", name)
    n = ("u" + n) if n.startswith("__") or n[:1].isdigit() else n
    return n + "_" if keyword.iskeyword(n) or n in ("spark", "args") else n


def macro_code(macros, to_spark_sql):
    """Python lines defining one function per macro; to_spark_sql(sql_text) -> converted SQL text"""
    lines = []
    for mc in macros:
        pn = [_py(p) for p in mc["params"]]
        lines.append(f"def {_py(mc['name'])}({', '.join(p + '=None' for p in pn)}):")
        lines.append(f'    """Teradata macro {re.sub(chr(34) * 1, "", mc["name"])[:80].replace(chr(10), " ")}: statements run in order, the last result is returned"""'.replace("\\", ""))
        lines.append("    result = None")
        for st in mc["stmts"]:
            conv = to_spark_sql(st).strip().rstrip(";").strip()
            if conv.startswith("--") and "\n" not in conv:
                lines.append("    # " + conv.lstrip("-").strip())
                continue
            used = [p for p in mc["params"] if re.search(rf":{re.escape(p)}\b", conv)]
            body = conv.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
            arg = ", args={" + ", ".join(f'{p!r}: {_py(p)}' for p in used) + "}" if used else ""
            lines.append(f'    result = spark.sql("""{body}"""{arg})')
        lines.append("    return result")
        lines.append("")
        lines.append("")
    return lines


_LIT = re.compile(r"(?is)^(?:-?\d+(?:\.\d+)?|'(?:[^']|'')*'|NULL)$")


def exec_call(text):
    """'-- __EXEC__ name(args)' marker -> a Python call, or None when an argument is not a plain literal"""
    m = re.match(r"(?s)-- __EXEC__ ([\w.\"]+)\((.*)\)\s*$", text.strip())
    if not m:
        return None
    args = [a for a in split_args(m.group(2)) if a != ""]
    py = []
    for a in args:
        if not _LIT.match(a):
            return None
        if a.upper() == "NULL":
            py.append("None")
        elif a.startswith("'"):
            py.append(repr(a[1:-1].replace("''", "'")))
        else:
            py.append(a)
    return f"{_py(m.group(1).split('.')[-1].strip(chr(34)))}({', '.join(py)})"
