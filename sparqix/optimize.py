"""Performance post-pass for generated PySpark (`--optimize`). Conservative: every rewrite is reported.

 * simple Python UDFs  -> built-in column expressions (UDFs are slow: they ship every row to Python)
 * broadcast joins     -> F.broadcast() on the right side when it is evidently small (limit / aggregate
                          result) or listed with --broadcast; other joins are reported as candidates
 * cache()             -> DataFrames that are expensive (join/aggregate/sort) and used by 2+ later statements
 * warnings            -> collect()/toPandas()/count() inside loops, remaining UDFs, empty-partition windows,
                          repartition(1), crossJoin
"""
from __future__ import annotations

import ast

from .core import Report

_CMP = {ast.Eq: "==", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}
_BIN = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/", ast.Mod: "%"}


class _No(Exception):
    pass


def _lam(node, param, col):
    """Translate a lambda body to a Column expression string, or raise _No."""
    def t(n):
        if isinstance(n, ast.Name):
            if n.id == param:
                return col
            raise _No
        if isinstance(n, ast.Constant) and isinstance(n.value, (int, float, str, bool)) and n.value is not None:
            return f"F.lit({n.value!r})"
        if isinstance(n, ast.BinOp) and type(n.op) in _BIN:
            if isinstance(n.op, ast.Add) and (_isstr(n.left) or _isstr(n.right)):
                return f"F.concat({t(n.left)}, {t(n.right)})"
            return f"({t(n.left)} {_BIN[type(n.op)]} {t(n.right)})"
        if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.USub):
            return f"(-{t(n.operand)})"
        if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.Not):
            return f"(~{t(n.operand)})"
        if isinstance(n, ast.Compare) and len(n.ops) == 1:
            op = n.ops[0]
            if type(op) in _CMP:
                return f"({t(n.left)} {_CMP[type(op)]} {t(n.comparators[0])})"
            if isinstance(op, ast.In) and isinstance(n.comparators[0], (ast.Tuple, ast.List, ast.Set)):
                vals = ", ".join(repr(ast.literal_eval(e)) for e in n.comparators[0].elts)
                return f"{t(n.left)}.isin({vals})"
        if isinstance(n, ast.BoolOp):
            sep = " & " if isinstance(n.op, ast.And) else " | "
            return "(" + sep.join(t(v) for v in n.values) + ")"
        if isinstance(n, ast.IfExp):
            return f"F.when({t(n.test)}, {t(n.body)}).otherwise({t(n.orelse)})"
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and not n.keywords:
            m, base = n.func.attr, t(n.func.value)
            simple = {"upper": "F.upper", "lower": "F.lower", "strip": "F.trim"}
            if m in simple and not n.args:
                return f"{simple[m]}({base})"
            if m == "startswith" and len(n.args) == 1 and isinstance(n.args[0], ast.Constant):
                return f"{base}.startswith({n.args[0].value!r})"
            if m == "endswith" and len(n.args) == 1 and isinstance(n.args[0], ast.Constant):
                return f"{base}.endswith({n.args[0].value!r})"
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and not n.keywords:
            f, a = n.func.id, n.args
            if f == "len" and len(a) == 1:
                return f"F.length({t(a[0])})"
            if f == "abs" and len(a) == 1:
                return f"F.abs({t(a[0])})"
            if f == "str" and len(a) == 1:
                return f"{t(a[0])}.cast('string')"
            if f == "int" and len(a) == 1:
                return f"{t(a[0])}.cast('int')"
            if f == "float" and len(a) == 1:
                return f"{t(a[0])}.cast('double')"
        raise _No

    def _isstr(n):
        return isinstance(n, ast.Constant) and isinstance(n.value, str)

    return t(node)


def _is_udf_call(n):
    return (isinstance(n, ast.Call) and isinstance(n.func, ast.Call)
            and ast.unparse(n.func.func) in ("F.udf", "udf", "functions.udf")
            and n.func.args and isinstance(n.func.args[0], ast.Lambda) and len(n.args) == 1)


def _offsets(src):
    starts, pos = [], 0
    for line in src.splitlines(keepends=True):
        starts.append(pos)
        pos += len(line)
    return starts


def _span(starts, n):
    return starts[n.lineno - 1] + n.col_offset, starts[n.end_lineno - 1] + n.end_col_offset


def _expensive(value):
    names = {n.attr for n in ast.walk(value) if isinstance(n, ast.Attribute)}
    return bool(names & {"join", "groupBy", "agg", "orderBy", "sort", "distinct", "dropDuplicates", "crossJoin"})


def _small(value):
    names = {n.attr for n in ast.walk(value) if isinstance(n, ast.Attribute)}
    return "limit" in names or ({"groupBy", "agg"} <= names) or "distinct" in names


def optimize(code: str, report: Report, broadcast=()) -> str:
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return code
    if not code.isascii():          # column offsets are in utf-8 bytes; keep it simple and skip
        report.note("optimize skipped: non-ASCII source")
        return code
    starts = _offsets(code)
    edits = []                      # (start, end, text)
    broadcast = set(broadcast)

    # --- UDF -> expression
    for n in ast.walk(tree):
        if _is_udf_call(n):
            lam = n.func.args[0]
            if len(lam.args.args) == 1 and not lam.args.vararg:
                try:
                    new = _lam(lam.body, lam.args.args[0].arg, ast.unparse(n.args[0]))
                except (_No, ValueError):
                    report.warn(f"line {n.lineno}", "Python UDF kept: rewrite with built-in functions (or pandas_udf) for speed")
                    continue
                s, e = _span(starts, n)
                edits.append((s, e, new))
                report.note(f"line {n.lineno}: UDF replaced by a built-in expression (null handling follows Spark, not Python)")

    # --- statement analysis (top level and function bodies are each analysed as a block)
    def block(stmts):
        assigned, uses = {}, {}
        for idx, st in enumerate(stmts):
            for sub in ast.walk(st):
                if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                    uses.setdefault(sub.id, []).append(idx)
            if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name):
                assigned.setdefault(st.targets[0].id, []).append((idx, st))
        # broadcast
        for st in stmts:
            for sub in ast.walk(st):
                if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and sub.func.attr == "join"
                        and sub.args and isinstance(sub.args[0], ast.Name)):
                    r = sub.args[0].id
                    if r in broadcast or any(_small(a[1].value) for a in assigned.get(r, [])):
                        s, e = _span(starts, sub.args[0])
                        edits.append((s, e, f"F.broadcast({r})"))
                        report.note(f"line {sub.lineno}: join with {r} uses F.broadcast ({'--broadcast' if r in broadcast else 'small side: limit/aggregate'})")
                    else:
                        report.note(f"line {sub.lineno}: join with {r}: if {r} is small (under ~10 MB) wrap it in F.broadcast({r}); use --broadcast {r}")
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and sub.func.attr == "crossJoin":
                    report.warn(f"line {sub.lineno}", "crossJoin is quadratic: make sure one side is tiny")
        # cache
        for name, defs in assigned.items():
            for k, (idx, st) in enumerate(defs):
                nxt = defs[k + 1][0] if k + 1 < len(defs) else len(stmts)
                readers = {u for u in uses.get(name, []) if idx < u < nxt}
                # a reader that also reassigns the name (x = x.f()) still counts as one use
                if len(readers) >= 2 and _expensive(st.value) and ".cache()" not in ast.unparse(st.value) \
                        and ".persist(" not in ast.unparse(st.value) and name not in broadcast:
                    s, e = _span(starts, st.value)
                    edits.append((e, e, ".cache()"))
                    report.note(f"line {st.lineno}: {name}.cache() added ({len(readers)} later uses of an expensive result; call {name}.unpersist() when done)")

    block(tree.body)
    for fn in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
        block(fn.body)

    # --- warnings
    for n in ast.walk(tree):
        if isinstance(n, (ast.For, ast.While)):
            for sub in ast.walk(n):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and sub.func.attr in ("collect", "toPandas", "count", "show", "first"):
                    report.warn(f"line {sub.lineno}", f".{sub.func.attr}() inside a loop runs a Spark job per iteration: restructure as one query")
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
            if n.func.attr == "repartition" and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == 1:
                report.warn(f"line {n.lineno}", "repartition(1) funnels all data through one task")
            if n.func.attr == "partitionBy" and not n.args and "Window" in ast.unparse(n.func.value):
                report.warn(f"line {n.lineno}", "Window without partitionBy moves all rows to one partition")

    # apply edits back to front (drop overlapping ones: a UDF inside a broadcast arg etc. is rare)
    out, last = code, len(code) + 1
    for s, e, text in sorted(edits, key=lambda x: (x[0], x[1]), reverse=True):
        if e > last:
            continue
        out, last = out[:s] + text + out[e:], s
    try:
        ast.parse(out)
    except SyntaxError:
        report.warn("optimize", "optimised code failed to parse; returning the unoptimised conversion")
        return code
    return out
