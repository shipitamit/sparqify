"""Dask DataFrame -> PySpark. Dask mirrors the pandas API, so: normalise Dask-only calls, then reuse pandas_conv."""
from __future__ import annotations

import ast

from .core import Report
from .pandas_conv import convert_pandas

DROP_NOARG = {"compute", "persist", "optimize"}
NOOP_ARG = {"npartitions", "chunksize"}


class _Norm(ast.NodeTransformer):
    def __init__(self, report):
        self.r = report
        self.alias = {"dd", "dask_dataframe"}

    def visit_Import(self, n):
        return n

    def visit_Name(self, n):
        return ast.copy_location(ast.Name(id="pd", ctx=n.ctx), n) if n.id in self.alias else n

    def visit_Call(self, n):
        self.generic_visit(n)
        f = n.func
        if isinstance(f, ast.Attribute):
            if f.attr in DROP_NOARG:
                return f.value
            if f.attr == "repartition":
                kw = {k.arg: k.value for k in n.keywords}
                num = n.args[0] if n.args else kw.get("npartitions")
                if num is not None:
                    return ast.Call(func=ast.Attribute(value=f.value, attr="repartition", ctx=ast.Load()),
                                    args=[num], keywords=[])
                return f.value
            if f.attr == "map_partitions":
                return n  # falls through to the pandas converter -> flagged TODO
        return n


def convert_dask(source: str, report: Report) -> str:
    tree = ast.parse(source)
    tree.body = [s for s in tree.body
                 if not (isinstance(s, (ast.Import, ast.ImportFrom)) and "dask" in ast.unparse(s))]
    tree = ast.fix_missing_locations(_Norm(report).visit(tree))
    report.note("Dask converted via the pandas rules (.compute()/.persist() removed; Spark is lazy until an action)")
    return convert_pandas(ast.unparse(tree), report)
