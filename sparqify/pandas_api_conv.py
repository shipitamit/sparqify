"""pandas -> pandas API on Spark (`pyspark.pandas`).  Keeps the original code almost verbatim: the pandas
module becomes `ps`, and constructs whose behaviour or cost differs on Spark are reported.
Use it for a fast migration; use the default PySpark DataFrame output for the best performance/control."""
from __future__ import annotations

import ast

from .core import Report
from .optimize import _offsets, _span

# pandas features pyspark.pandas does not provide (or only partially)
_UNSUPPORTED_ATTRS = {
    "to_pickle": "to_pickle is not supported: write parquet/CSV instead",
    "to_clipboard": "to_clipboard is not supported",
    "style": "DataFrame.style is not supported (convert a small result with .to_pandas() first)",
    "eval": "DataFrame.eval is not supported: use normal column expressions",
    "memory_usage": "memory_usage is of little use on a distributed frame",
    "iterrows": "iterrows pulls rows through the driver one by one: very slow on large data, use vectorised column operations",
    "itertuples": "itertuples pulls rows through the driver: very slow on large data",
    "values": ".values collects the whole frame to the driver (OOM risk on large data)",
    "to_numpy": "to_numpy() collects the whole frame to the driver (OOM risk on large data)",
    "tolist": "tolist() collects to the driver",
    "to_pandas": "to_pandas() collects to the driver: only on small results",
    "interpolate": "interpolate supports only method='linear'",
    "applymap": "applymap runs a Python function per cell (UDF speed)",
}


def convert_pandas_api(source: str, report: Report) -> str:
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        report.todo("line %s" % e.lineno, f"syntax error: {e.msg}")
        return source
    if not source.isascii():
        report.note("non-ASCII source: pandas-API conversion keeps it as-is except imports; review manually")
    starts = _offsets(source)
    edits, aliases, froms = [], set(), []
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            for a in n.names:
                if a.name == "pandas":
                    aliases.add(a.asname or "pandas")
        elif isinstance(n, ast.ImportFrom) and n.module == "pandas" and n.level == 0:
            froms.append(n)
    # imports
    seen_ps = False
    for n in ast.walk(tree):
        if isinstance(n, ast.Import) and any(a.name == "pandas" for a in n.names):
            s, e = _span(starts, n)
            others = [a for a in n.names if a.name != "pandas"]
            txt = "" if seen_ps else "import pyspark.pandas as ps"
            seen_ps = True
            if others:
                txt += ("\n" if txt else "") + "import " + ", ".join(a.name + (f" as {a.asname}" if a.asname else "") for a in others)
            edits.append((s, e, txt or "# (pandas import merged above)"))
    for n in froms:
        s, e = _span(starts, n)
        edits.append((s, e, source[s:e].replace("from pandas", "from pyspark.pandas", 1)))
        seen_ps = True
    header = ""
    if not aliases and not froms:
        report.note("no pandas import found: nothing to convert")
        return source
    # names / attributes
    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and n.id in aliases and isinstance(n.ctx, ast.Load):
            s, e = _span(starts, n)
            edits.append((s, e, "ps"))
        if isinstance(n, ast.Attribute):
            if n.attr in _UNSUPPORTED_ATTRS and not (isinstance(n.value, ast.Name) and n.value.id in aliases):
                lvl = report.todo if n.attr in ("to_pickle", "to_clipboard", "style", "eval") else report.warn
                lvl(f"line {n.lineno}", _UNSUPPORTED_ATTRS[n.attr])
            if isinstance(n.value, ast.Name) and n.value.id in aliases and n.attr in ("read_pickle", "read_clipboard", "read_fwf_"):
                report.todo(f"line {n.lineno}", f"pandas.{n.attr} is not supported in pandas API on Spark")
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
            f = n.func.attr
            kws = {k.arg for k in n.keywords}
            if f == "read_csv" and ("chunksize" in kws or "iterator" in kws):
                report.todo(f"line {n.lineno}", "read_csv chunksize/iterator is not supported: Spark reads in parallel, drop it")
            if f == "apply" and any(k.arg == "axis" and isinstance(k.value, ast.Constant) and k.value.value in (1, "columns") for k in n.keywords):
                report.warn(f"line {n.lineno}", "apply(axis=1) runs a Python function per row (slow); add a return type hint to the function or use column operations")
            if f in ("to_csv", "to_parquet", "to_json") and n.args:
                report.warn(f"line {n.lineno}", f"{f} writes a directory of part files, not a single file")
            if f == "where" and isinstance(n.func.value, ast.Name) and n.func.value.id in ("np", "numpy"):
                report.warn(f"line {n.lineno}", "np.where is not reliably supported on pandas-on-Spark Series: prefer Series.where()/mask(), or use the default DataFrame output mode")
            if f in ("iloc",):
                report.warn(f"line {n.lineno}", "iloc is positional: needs a global ordering and is expensive")
        if isinstance(n, ast.Subscript) and isinstance(n.value, ast.Attribute) and n.value.attr == "iloc":
            report.warn(f"line {n.lineno}", "iloc is positional: needs a global ordering and is expensive")
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "len":
            report.note(f"line {n.lineno}: len() triggers a Spark count job")
    if any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("merge", "concat", "join") for n in ast.walk(tree)):
        header += "ps.set_option('compute.ops_on_diff_frames', True)  # needed when combining columns of different frames\n"
        report.note("added compute.ops_on_diff_frames=True (operations between different DataFrames are off by default)")
    report.note("default index: pandas-on-Spark builds a sequential index (extra cost); if you do not need row numbers set ps.set_option('compute.default_index_type', 'distributed')")
    report.note("sort/head order and some dtypes (e.g. object columns, NaN vs None) can differ from pandas: run `sparqify verify` on a sample")
    out, last = source, len(source) + 1
    for s, e, text in sorted(set(edits), key=lambda x: (x[0], x[1]), reverse=True):
        if e > last:
            continue
        out, last = out[:s] + text + out[e:], s
    if header:
        lines = out.splitlines(keepends=True)
        i = max((k + 1 for k, l in enumerate(lines) if l.startswith(("import ", "from "))), default=0)
        lines.insert(i, header)
        out = "".join(lines)
    try:
        ast.parse(out)
    except SyntaxError:
        report.todo("file", "generated pandas-API code failed to parse; original kept")
        return source
    return out
