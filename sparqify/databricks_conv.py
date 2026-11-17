"""Databricks support.
 * target:  wrap converted code as a Databricks notebook (source format: .py with `# COMMAND ----------`
           cells, or .sql), reuse the notebook's `spark`, use display() instead of show().
 * source:  read a Databricks notebook source file; %sql / %python / pandas cells are converted cell by
           cell, Spark-native cells and dbutils/display calls are kept (they are valid on Databricks).
"""
from __future__ import annotations

import re

from .core import Report, detect_language

PY_HDR = "# Databricks notebook source"
SQL_HDR = "-- Databricks notebook source"
SEP_PY = "# COMMAND ----------"
SEP_SQL = "-- COMMAND ----------"


def _drop_session(code: str) -> str:
    code = re.sub(r"^spark\s*=\s*SparkSession\.builder.*\n?", "", code, flags=re.M)
    code = re.sub(r"^(from pyspark\.sql import )SparkSession,\s*", r"\1", code, flags=re.M)
    code = re.sub(r"^from pyspark\.sql import SparkSession\n", "", code, flags=re.M)
    return code


def _display(code: str) -> str:
    # `x.show()` / `x.show(n)` as a whole statement -> display(x); chains are left as .show()
    return re.sub(r"^(\w+)\.show\((?:\d+)?\)\s*$", r"display(\1)", code, flags=re.M)


def to_databricks(code: str, sql_only: bool = False) -> str:
    if sql_only:
        stmts = [s.strip() for s in code.split(";\n") if s.strip()]
        return SQL_HDR + "\n" + ("\n\n" + SEP_SQL + "\n\n").join(s.rstrip(";") + ";" for s in stmts) + "\n"
    code = _display(_drop_session(code)).strip("\n")
    lines = code.splitlines()
    i = 0
    while i < len(lines) and (lines[i].startswith(("from ", "import ")) or not lines[i].strip()):
        i += 1
    head, body = "\n".join(lines[:i]).strip(), "\n".join(lines[i:]).strip()
    cells = [c for c in (head, body) if c]
    return PY_HDR + "\n\n" + ("\n\n" + SEP_PY + "\n\n").join(cells) + "\n"


def _split_cells(src: str):
    src = src.split("\n", 1)[1] if src.startswith(("# Databricks notebook source", "-- Databricks notebook source")) else src
    parts = re.split(r"^(?:#|--) COMMAND -+\s*$", src, flags=re.M)
    return [p.strip("\n") for p in parts if p.strip()]


def _magic(cell: str):
    lines = cell.splitlines()
    if lines and all(l.startswith(("# MAGIC", "-- MAGIC")) for l in lines if l.strip()):
        body = [re.sub(r"^(?:#|--) MAGIC ?", "", l) for l in lines]
        if body and body[0].startswith("%"):
            return body[0][1:].split()[0], "\n".join(body[1:]).strip("\n")
        return "python", "\n".join(body)
    return None, cell


def convert_databricks(source: str, report: Report, sql_dialect: str = "generic") -> str:
    from .pandas_conv import Converter, Translator
    from .sql_conv import convert_sql
    is_sql = source.startswith("-- Databricks")
    cells = _split_cells(source)
    kinds = [_magic(c) for c in cells]
    pycode = "\n".join(b for k, b in kinds if k in (None, "python") and not is_sql)
    lang = detect_language(pycode) if pycode.strip() else "python"
    tr = Translator(report) if lang == "pandas" else None
    if lang == "pandas":
        report.note("python cells use pandas: converted to PySpark")
    out = []
    for n, (kind, body) in enumerate(kinds, 1):
        if kind in ("md", "sh", "fs", "run", "pip", "scala", "r"):
            if kind in ("scala", "r", "run"):
                report.warn(f"cell {n}", f"%{kind} cell kept as-is; convert separately if needed")
            out.append("\n".join(["# MAGIC %" + kind] + ["# MAGIC " + l for l in body.splitlines()]))
        elif kind == "sql" or is_sql:
            txt = convert_sql(body, report, dialect="spark" if sql_dialect in ("generic", "databricks") else sql_dialect, out="sql")
            out.append(txt.strip() if is_sql else "\n".join(["# MAGIC %sql"] + ["# MAGIC " + l for l in txt.strip().splitlines()]))
        elif lang == "pandas":
            try:
                code = Converter(body, report, translator=tr).body()
            except SyntaxError as e:
                report.todo(f"cell {n}", f"syntax error: {e}")
                code = "# TODO(sparqify): could not parse cell\n" + "\n".join("# " + l for l in body.splitlines())
            out.append(_display(code))
        else:
            out.append(body)
    if lang == "pandas":
        out.insert(0, "from pyspark.sql import functions as F")
    sep, hdr = (SEP_SQL, SQL_HDR) if is_sql else (SEP_PY, PY_HDR)
    return hdr + "\n\n" + ("\n\n" + sep + "\n\n").join(o for o in out if o.strip()) + "\n"
