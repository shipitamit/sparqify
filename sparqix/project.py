"""Whole-project conversion:  sparqix project SRC_DIR -o OUT_DIR [--dialect D] [--target databricks]

Walks a folder, converts every recognised file, mirrors the tree into OUT_DIR and writes
SPARQIX_REPORT.md (per-file status, TODOs, warnings, local-import dependencies, skipped files)."""
from __future__ import annotations

import ast
import os
import re

from .core import convert, detect_language

SKIP_DIRS = {".git", ".hg", ".svn", "__pycache__", "node_modules", ".venv", "venv", ".tox", ".idea", ".pytest_cache", "target", "build", "dist"}
EXTS = {".dtsx", ".py", ".sql", ".ipynb", ".r", ".sas", ".scala", ".java", ".xml", ".json", ".item", ".yxmd", ".ktr", ".pig", ".hql", ".impala", ".bteq", ".dsx", ".fld", ".mld", ".tpt"}
OUT_EXT = {"sql": ".py", "dbt": ".py", "sas": ".py", "r": ".py", "notebook": ".ipynb", "scala": ".scala", "java": ".java"}


def _out_name(rel: str, lang: str, sql_out: str, used: set) -> str:
    """Same name and folder as the source (output goes to a separate tree, so local imports keep working);
    only when two sources would land on one file (sales.py + sales.sas) is the source extension added."""
    base, ext = os.path.splitext(rel)
    new = ".sql" if lang in ("sql", "dbt") and sql_out == "sql" else OUT_EXT.get(lang, ".py")
    name = base + new
    if name in used:
        name = f"{base}_{ext.lstrip('.').lower()}{new}"
    used.add(name)
    return name


def _local_imports(code: str, modules: set[str]) -> set[str]:
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return set()
    found = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            found |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
            found.add(n.module.split(".")[0])
    return found & modules


MAX_FILE_BYTES = 20_000_000


def convert_project(src: str, out: str, *, sql_dialect="generic", sql_out="pyspark", target="spark", optimize=False, broadcast=(), mode="dataframe", delta=False, catalog=None, schema="default", only=None, ai=False, ai_apply=False, ai_model=None, ai_max=20, ai_provider=None, ai_base_url=None):
    src, out = os.path.abspath(src), os.path.abspath(out)
    rows, skipped, used = [], [], set()
    py_modules = set()
    for root, dirs, files in os.walk(src):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS and not os.path.abspath(os.path.join(root, d)) == out)
        for f in sorted(files):
            if f.endswith(".py"):
                py_modules.add(f[:-3])
    for root, dirs, files in os.walk(src):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS and not os.path.abspath(os.path.join(root, d)) == out)
        for f in sorted(files, key=lambda f: (not f.endswith(".py"), f)):    # .py first: it keeps its own name
            path = os.path.join(root, f)
            rel = os.path.relpath(path, src)
            if os.path.islink(path):                      # a link could point at files outside the project
                skipped.append((rel, "symbolic link: not followed"))
                continue
            if only is not None and rel.replace(os.sep, "/") not in only:
                continue
            ext = os.path.splitext(f)[1].lower()
            if ext not in EXTS:
                skipped.append((rel, "not a recognised code file"))
                continue
            try:
                if os.path.getsize(path) > MAX_FILE_BYTES:
                    skipped.append((rel, "file larger than 20 MB: skipped")); continue
                text = open(path, encoding="utf-8").read()
            except (UnicodeDecodeError, OSError) as e:
                skipped.append((rel, f"unreadable ({e.__class__.__name__})"))
                continue
            lang = detect_language(text, path)
            if ext == ".py" and re.search(r"from airflow|import airflow", text):
                skipped.append((rel, "Airflow DAG: use `sparqix orchestrate`")); continue
            if ext == ".xml" and "<WORKFLOW" in text[:20000].upper() and "<MAPPING" not in text.upper():
                skipped.append((rel, "PowerCenter workflow: use `sparqix orchestrate`")); continue
            if ext == ".dtsx" and "Microsoft.Pipeline" not in text and "pipeline" not in text.lower():
                skipped.append((rel, "SSIS package without data flows: use `sparqix orchestrate`")); continue
            if ext in (".xml", ".item", ".yxmd", ".ktr") and lang not in ("powercenter", "talend", "alteryx", "pentaho"):
                skipped.append((rel, "xml but not an Informatica PowerCenter export")); continue
            if ext == ".json" and lang != "idmc":
                skipped.append((rel, "json but not an IDMC mapping")); continue
            if ext == ".java" and lang != "java":
                skipped.append((rel, "java file without stream/RDD code")); continue
            try:
                code, rep = convert(text, lang, sql_dialect=sql_dialect, sql_out=sql_out,
                                    filename=path, target=target, optimize=optimize, broadcast=broadcast, mode=mode, delta=delta, catalog=catalog, schema=schema,
                                    ai=ai, ai_apply=ai_apply, ai_model=ai_model, ai_max=ai_max, ai_provider=ai_provider, ai_base_url=ai_base_url)
            except Exception as e:                       # one bad file must not stop the project
                if type(e).__name__ == "AIError" and not rows:
                    raise
                rows.append(dict(rel=rel, lang=lang, out=None, todos=[("file", f"conversion crashed: {e!r}")], warnings=[], notes=[], deps=set(), status="FAILED"))
                continue
            out_rel = _out_name(rel, lang, sql_out, used)
            os.makedirs(os.path.dirname(os.path.join(out, out_rel)) or out, exist_ok=True)
            with open(os.path.join(out, out_rel), "w", encoding="utf-8") as fh:
                fh.write(code)
            deps = _local_imports(text, py_modules - {os.path.splitext(f)[0]}) if ext == ".py" else set()
            status = "NEEDS REVIEW" if rep.todos else ("CHECK" if rep.warnings else "OK")
            rows.append(dict(rel=rel, lang=lang, out=out_rel, todos=list(rep.todos), warnings=list(rep.warnings),
                             notes=list(rep.notes), deps=deps, status=status))
    report = _report(rows, skipped)
    import json
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "SPARQIX_REPORT.json"), "w", encoding="utf-8") as fh:
        json.dump({"files": [{**r, "deps": sorted(r["deps"]), "todos": [list(map(str, t)) for t in r["todos"]],
                              "warnings": [list(map(str, w)) for w in r["warnings"]]} for r in rows],
                   "skipped": [list(x) for x in skipped]}, fh, indent=1)
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "SPARQIX_REPORT.md"), "w", encoding="utf-8") as fh:
        fh.write(report)
    return rows, skipped


def _report(rows, skipped) -> str:
    n = {k: sum(r["status"] == k for r in rows) for k in ("OK", "CHECK", "NEEDS REVIEW", "FAILED")}
    L = ["# Sparqix project report", "",
         f"{len(rows)} files converted: {n['OK']} OK, {n['CHECK']} with warnings, "
         f"{n['NEEDS REVIEW']} needing manual review, {n['FAILED']} failed. {len(skipped)} skipped.", "",
         "| File | Detected | Output | Status | TODOs | Warnings |", "|---|---|---|---|---|---|"]
    for r in sorted(rows, key=lambda r: (["FAILED", "NEEDS REVIEW", "CHECK", "OK"].index(r["status"]), r["rel"])):
        L.append(f"| {r['rel']} | {r['lang']} | {r['out'] or '-'} | {r['status']} | {len(r['todos'])} | {len(r['warnings'])} |")
    todo_rows = [r for r in rows if r["todos"] or r["warnings"]]
    if todo_rows:
        L += ["", "## Details"]
        for r in todo_rows:
            L += ["", f"### {r['rel']}"]
            L += [f"- TODO ({loc}): {msg}" for loc, msg in r["todos"]]
            L += [f"- warning ({loc}): {msg}" for loc, msg in r["warnings"]]
    dep = [r for r in rows if r["deps"]]
    if dep:
        L += ["", "## Local imports between converted files",
              "Converted modules keep their names, so imports still resolve, but a function that used to take or "
              "return a pandas DataFrame now takes a Spark DataFrame: check every call across these files.", ""]
        L += [f"- {r['rel']} imports {', '.join(sorted(r['deps']))}" for r in dep]
    if skipped:
        L += ["", "## Skipped files", ""] + [f"- {rel}: {why}" for rel, why in skipped]
    return "\n".join(L) + "\n"


def _read_only(path):
    if not path:
        return None
    with open(path, encoding="utf-8") as fh:
        return {l.strip().replace("\\", "/") for l in fh if l.strip()}


def main(argv) -> int:
    import argparse
    p = argparse.ArgumentParser(prog="sparqix project")
    p.add_argument("src")
    p.add_argument("-o", "--output", required=True)
    p.add_argument("--dialect", default="generic")
    p.add_argument("--sql-out", default="pyspark", choices=["pyspark", "sql"])
    p.add_argument("--target", default="spark", choices=["spark", "databricks", "gluten", "comet", "fabric", "photon"])
    p.add_argument("--mode", default="dataframe", choices=["dataframe", "pandas-api"])
    p.add_argument("--delta", action="store_true")
    p.add_argument("--catalog", default=None)
    p.add_argument("--schema", default="default")
    p.add_argument("--optimize", action="store_true")
    p.add_argument("--broadcast", default="")
    p.add_argument("--only", help="file with relative paths (one per line): convert only these files, e.g. the files changed in a pull request")
    p.add_argument("--ai", action="store_true", help="model-assisted suggestions for TODO blocks (sends snippets to the provider you choose)")
    p.add_argument("--ai-apply", action="store_true")
    p.add_argument("--ai-model", default=None)
    p.add_argument("--ai-max", type=int, default=20, help="maximum TODO blocks sent per file")
    p.add_argument("--strict", action="store_true", help="exit 2 if any file has TODOs or failed")
    a = p.parse_args(argv)
    if os.path.abspath(a.output) == os.path.abspath(a.src):
        print("output folder must differ from the source folder"); return 2
    rows, skipped = convert_project(a.src, a.output, sql_dialect=a.dialect, sql_out=a.sql_out, target=a.target,
                                  optimize=a.optimize, mode=a.mode, delta=a.delta, catalog=a.catalog, schema=a.schema, only=_read_only(a.only), ai=a.ai, ai_apply=a.ai_apply, ai_model=a.ai_model, ai_max=a.ai_max, ai_provider=a.ai_provider, ai_base_url=a.ai_base_url, broadcast=[b for b in a.broadcast.split(',') if b])
    for r in rows:
        print(f"{r['status']:<13} {r['rel']}  [{r['lang']}]")
    print(f"--- {len(rows)} converted, {len(skipped)} skipped; report: {os.path.join(a.output, 'SPARQIX_REPORT.md')} ---")
    return 2 if a.strict and any(r["todos"] or r["status"] == "FAILED" for r in rows) else 0
