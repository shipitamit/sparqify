"""Jupyter notebook (.ipynb) -> Spark notebook. Cells are converted in order with shared state, so a
DataFrame defined in cell 1 is still known in cell 7. Output is a new .ipynb (JSON string)."""
from __future__ import annotations

import ast
import json
import re

from .core import Report, Unsupported, detect_language
from .pandas_conv import Converter, Translator, header_for


def _clean(src: str):
    """strip IPython magics; return (code, cell_magic)"""
    lines = src.splitlines()
    if lines and lines[0].startswith("%%"):
        return "\n".join(lines[1:]), lines[0][2:].split()[0]
    out = []
    for l in lines:
        out.append("# (magic removed) " + l if l.lstrip().startswith(("%", "!", "?")) else l)
    return "\n".join(out), None


def _cell_src(c):
    s = c.get("source", "")
    return "".join(s) if isinstance(s, list) else s


def convert_notebook(source: str, report: Report, sql_dialect: str = "generic") -> str:
    nb = json.loads(source)
    cells = nb.get("cells", [])
    kernel = (nb.get("metadata", {}).get("kernelspec", {}).get("language") or "").lower()
    code_cells = [_clean(_cell_src(c)) for c in cells if c.get("cell_type") == "code"]
    joined = "\n".join(src for src, magic in code_cells if magic is None)
    lang = "r" if kernel == "r" else detect_language(joined) if joined.strip() else "python"
    if lang in ("sql", "notebook"):
        lang = "python"
    report.note(f"notebook language detected: {lang}")

    translator = None
    if lang in ("pandas", "dask"):
        translator = Translator(report)
    elif lang == "polars":
        from .polars_conv import PolarsTranslator
        translator = PolarsTranslator(report)

    out_cells, bodies = [], []
    n = 0
    for c in cells:
        if c.get("cell_type") != "code":
            out_cells.append(c)
            continue
        n += 1
        src, magic = _clean(_cell_src(c))
        before = (len(report.todos), len(report.warnings))
        if not src.strip():
            body = ""
        elif magic == "sql":
            from .sql_conv import convert_sql
            body = convert_sql(src, report, dialect=sql_dialect, out="pyspark")
            body = "\n".join(l for l in body.splitlines() if not l.startswith(("from pyspark", "spark = "))).strip()
        elif magic in ("bash", "sh", "html", "javascript", "latex", "markdown"):
            body = "\n".join("# " + l for l in src.splitlines())
        elif lang in ("pandas", "dask", "polars"):
            code = src
            if lang == "dask":
                from .dask_conv import _Norm
                tree = ast.parse(code)
                tree.body = [s for s in tree.body if not (isinstance(s, (ast.Import, ast.ImportFrom)) and "dask" in ast.unparse(s))]
                code = ast.unparse(ast.fix_missing_locations(_Norm(report).visit(tree)))
            try:
                body = Converter(code, report, translator=translator).body()
            except SyntaxError as e:
                body = "# TODO(sparqix): could not parse cell: " + str(e) + "\n" + "\n".join("# " + l for l in src.splitlines())
                report.todo(f"cell {n}", f"syntax error: {e}")
        elif lang == "r":
            from .r_conv import convert_r
            body = convert_r(src, report)
            body = "\n".join(l for l in body.splitlines() if not l.startswith(("from pyspark", "spark = SparkSession"))).strip()
        else:
            from .python_conv import PyConv
            conv = PyConv(src, report)
            body = conv.run().split("sc = spark.sparkContext\n\n" if "sc = spark" in conv.run() else 'getOrCreate()\n\n', 1)[-1].strip()
        # re-label findings with the cell number
        report.todos[before[0]:] = [(f"cell {n}, line {l}" if isinstance(l, int) else l, m) for l, m in report.todos[before[0]:]]
        report.warnings[before[1]:] = [(f"cell {n}, line {l}" if isinstance(l, int) else l, m) for l, m in report.warnings[before[1]:]]
        bodies.append(body)
        nc = dict(c)
        nc["source"] = body.splitlines(keepends=True)
        nc["outputs"] = []
        nc["execution_count"] = None
        out_cells.append(nc)

    if lang in ("pandas", "dask", "polars"):
        head = header_for(translator, "\n".join(bodies) + " spark")
    elif lang == "r":
        head = "from pyspark.sql import SparkSession, functions as F, Window\n\nspark = SparkSession.builder.appName(\"converted\").getOrCreate()"
    else:
        head = 'from pyspark.sql import SparkSession\n\nspark = SparkSession.builder.appName("converted").getOrCreate()\nsc = spark.sparkContext'
    setup = {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
             "source": head.splitlines(keepends=True)}
    nb["cells"] = [setup] + out_cells
    nb.setdefault("metadata", {})["sparqix"] = {"converted_from": lang}
    return json.dumps(nb, indent=1) + "\n"
