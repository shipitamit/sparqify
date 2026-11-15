"""dbt model (SQL + Jinja) -> Spark.

Resolves ref()/source()/config(), drops incremental blocks (with a warning) and hands the remaining SQL to the
dialect converter. Output registers the result as a temp view named after the model so downstream models work.
"""
from __future__ import annotations

import re

from .core import Report, cm
from .sql_conv import convert_sql


def preprocess(src: str, report: Report):
    info = {"materialized": None}

    def config(m):
        mm = re.search(r"materialized\s*=\s*['\"](\w+)['\"]", m.group(1))
        if mm:
            info["materialized"] = mm.group(1)
        return ""

    src = re.sub(r"\{#.*?#\}", "", src, flags=re.S)
    src = re.sub(r"\{\{\s*config\((.*?)\)\s*\}\}", config, src, flags=re.S)
    src = re.sub(r"\{\{\s*ref\(\s*(?:['\"][\w-]+['\"]\s*,\s*)?['\"]([\w-]+)['\"]\s*\)\s*\}\}", r"\1", src)
    src = re.sub(r"\{\{\s*source\(\s*['\"]([\w-]+)['\"]\s*,\s*['\"]([\w-]+)['\"]\s*\)\s*\}\}", r"\1.\2", src)
    # incremental blocks: keep the full-refresh (else) branch, drop the incremental one
    def incr(m):
        report.warn("incremental", "is_incremental() block removed - the model now does a full refresh; use MERGE/INSERT OVERWRITE for incremental loads")
        return m.group(2) or ""
    src = re.sub(r"\{%-?\s*if\s+is_incremental\(\)\s*-?%\}(.*?)(?:\{%-?\s*else\s*-?%\}(.*?))?\{%-?\s*endif\s*-?%\}", incr, src, flags=re.S)
    src = re.sub(r"\{\{\s*this\s*\}\}", "THIS_MODEL", src)
    # var() with a literal default
    src = re.sub(r"\{\{\s*var\(\s*['\"][\w-]+['\"]\s*,\s*([^)]+?)\s*\)\s*\}\}", lambda m: m.group(1).strip("'\""), src)
    for pat, msg in ((r"\{%.*?%\}", "Jinja control flow ({% ... %}) is not evaluated"),
                     (r"\{\{.*?\}\}", "unresolved Jinja expression / macro call")):
        for m in re.finditer(pat, src, flags=re.S):
            report.todo("jinja", f"{msg}: {m.group().strip()[:60]}")
    return src.strip(), info


def convert_dbt(source: str, report: Report, dialect: str = "generic", model: str = "model") -> str:
    sql, info = preprocess(source, report)
    code = convert_sql(sql, report, dialect=dialect, out="pyspark")
    code = code.replace("result = spark.sql(", f"{model} = spark.sql(", 1)
    code += f"\n{model}.createOrReplaceTempView({model!r})\n"
    mat = info["materialized"]
    if mat in ("table", "incremental"):
        code += f"# materialized={cm(mat)}: persist with {cm(model)}.write.mode('overwrite').saveAsTable({model!r})\n"
    report.note(f"dbt model registered as temp view {model!r}; run upstream models first (ref() -> view names)")
    return code
