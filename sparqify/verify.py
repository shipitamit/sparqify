"""Run-and-compare validation: execute the ORIGINAL script and the converted Spark code on the same sample data and diff the
DataFrames / scalars they produce. Needs pandas + pyspark installed.

    sparqify verify script.py --data ./sample [--vars out,merged] [--rtol 1e-6]

Which originals can be run (the "original runner" per language):
  pandas, polars, dask, Jupyter notebooks (code cells)  executed in-process, like pandas (polars / dask must be installed)
  SQL (any dialect)                                      executed with DuckDB if installed, else SQLite: only SQL that engine accepts can be verified;
                                                         each CSV in the data folder is a table named after the file; every SELECT is one result
  R                                                      executed with `Rscript` (must be installed); every data.frame / numeric scalar is compared
Everything else needs the original tool or a database (SAS, Informatica, Teradata, ...) or a JVM build (Scala/Java): `unverifiable_reason()` says why.

Comparison ignores row order (rows are sorted) and column order, uses a float tolerance, and treats a date as the timestamp at midnight of that day."""
from __future__ import annotations

import json
import math
import os
import shutil
import tempfile


def _isnum(v):
    """a real number for comparison purposes: int, float, numpy numbers and decimal.Decimal (Spark returns DECIMAL columns as Decimal); never bool"""
    import decimal
    import numbers
    return isinstance(v, (numbers.Real, decimal.Decimal)) and not isinstance(v, bool)


def _norm(df):
    import datetime
    import decimal
    import pandas as pd
    df = df.rename(columns=str).copy()
    for c in df.columns:
        if df[c].dtype == object:                  # a DECIMAL column (Python Decimal objects): compare it as numbers, like the float the other side holds
            vals = df[c].dropna()
            if len(vals) and all(isinstance(v, decimal.Decimal) for v in vals):
                df[c] = pd.to_numeric(df[c].map(lambda v: float(v) if isinstance(v, decimal.Decimal) else v))
        # Spark DATE columns arrive as datetime.date objects, pandas parse_dates gives datetime64: same day, so compare them as midnight timestamps
        if df[c].dtype == object:
            vals = df[c].dropna()
            if len(vals) and all(isinstance(v, datetime.date) and not isinstance(v, datetime.datetime) for v in vals):
                df[c] = pd.to_datetime(df[c])
    df = df[sorted(df.columns)]
    if len(df.columns):
        df = df.sort_values(list(df.columns), na_position="last", kind="mergesort")
    return df.reset_index(drop=True)


def compare_frames(a, b, rtol: float = 1e-6):
    """Return a list of human-readable differences (empty list == equal)."""
    import datetime
    import pandas as pd
    a, b = a.rename(columns=str).copy(), b.rename(columns=str).copy()
    for c in set(a.columns) & set(b.columns):
        # a date column that one side holds as text (R's read.csv) and the other as dates (Spark's inferSchema): same day, compare as timestamps
        for x, y in ((a, b), (b, a)):
            xv = x[c].dropna()
            if len(xv) and all(isinstance(v, str) for v in xv) and (
                    str(y[c].dtype).startswith("datetime64")
                    or (y[c].dtype == object and len(y[c].dropna()) and all(isinstance(v, datetime.date) for v in y[c].dropna()))):
                parsed = pd.to_datetime(x[c], errors="coerce", format="%Y-%m-%d")
                if parsed.notna().sum() == x[c].notna().sum():
                    x[c] = parsed
    a, b = _norm(a), _norm(b)
    diffs = []
    if set(a.columns) != set(b.columns):
        only_a, only_b = sorted(set(a.columns) - set(b.columns)), sorted(set(b.columns) - set(a.columns))
        diffs.append(f"columns differ: only in original {only_a}, only in spark {only_b}")
        keep = sorted(set(a.columns) & set(b.columns))
        a, b = a[keep], b[keep]
    if len(a) != len(b):
        diffs.append(f"row count differs: original {len(a)}, spark {len(b)}")
        return diffs
    for c in a.columns:
        x, y = a[c], b[c]
        bad = 0
        for u, v in zip(x, y):
            if (pd.isna(u) and pd.isna(v)):
                continue
            if _isnum(u) and _isnum(v):
                if not math.isclose(float(u), float(v), rel_tol=rtol, abs_tol=rtol):
                    bad += 1
            elif str(u) != str(v):
                if not (hasattr(u, "isoformat") and hasattr(v, "isoformat") and str(u)[:19] == str(v)[:19]):
                    bad += 1
        if bad:
            diffs.append(f"column {c!r}: {bad} of {len(a)} values differ (e.g. {x.iloc[0]!r} vs {y.iloc[0]!r})")
    return diffs


def _as_pandas(v, errors=None):
    """pandas object / number for any DataFrame-like value (pandas, polars, dask, pyspark, pyspark.pandas), else None"""
    import pandas as pd
    if isinstance(v, bool):
        return None
    if isinstance(v, (pd.DataFrame, pd.Series, int, float)):
        return v
    mod = type(v).__module__.split(".")[0]
    try:
        if hasattr(v, "toPandas"):
            return v.toPandas()
        if mod == "polars":
            return (v.collect() if hasattr(v, "collect") else v).to_pandas()
        if mod == "dask" and hasattr(v, "compute"):
            r = v.compute()
            return r if isinstance(r, (pd.DataFrame, pd.Series, int, float)) else None
        if hasattr(v, "to_pandas"):
            return v.to_pandas()
    except Exception as e:
        if errors is not None:
            errors.append(f"{type(e).__name__}: {str(e).strip().splitlines()[0][:300] if str(e).strip() else ''}")
        return None
    return None


def _compare_values(name, a, b, rtol):
    import pandas as pd
    a = _as_pandas(a)
    if isinstance(a, pd.Series):
        a = a.reset_index()      # index becomes a column, as Spark's groupBy/value_counts output has it
    if isinstance(a, pd.DataFrame):
        return compare_frames(a, b, rtol)
    try:
        if not math.isclose(float(a), float(b), rel_tol=rtol, abs_tol=rtol):
            return [f"value differs: original {a!r}, spark {b!r}"]
        return []
    except (TypeError, ValueError):
        return [] if str(a) == str(b) else [f"value differs: original {a!r}, spark {b!r}"]


def _run(code, workdir):
    ns = {"__name__": "__sparqify_verify__"}
    old = os.getcwd()
    os.chdir(workdir)
    try:
        exec(compile(code, "<script>", "exec"), ns)
    finally:
        os.chdir(old)
    return ns


def _notebook_code(text):
    """the code cells of a .ipynb (JSON) as one script; %magics and !shell lines are dropped"""
    nb = json.loads(text)
    out = []
    for c in nb.get("cells", []):
        if c.get("cell_type") == "code":
            src = c.get("source", "")
            src = "".join(src) if isinstance(src, list) else str(src)
            out.append("\n".join(l for l in src.splitlines() if not l.lstrip().startswith(("%", "!"))))
    return "\n\n".join(out) + "\n"


def _split_sql(text):
    """statements split on ; outside quotes and comments"""
    out, cur, i, n, q = [], [], 0, len(text), None
    while i < n:
        ch = text[i]
        if q:
            cur.append(ch)
            if ch == q:
                if i + 1 < n and text[i + 1] == q:
                    cur.append(text[i + 1])
                    i += 1
                else:
                    q = None
        elif ch in "'\"`":
            q = ch
            cur.append(ch)
        elif text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 1
        elif ch == ";":
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
        i += 1
    out.append("".join(cur))
    return [x.strip() for x in out if x.strip()]


def _is_query(stmt):
    return stmt.lstrip("( \n\t").upper().startswith(("SELECT", "WITH", "SEL ", "VALUES"))


def _csv_tables(workdir):
    import pandas as pd
    t = {}
    for f in sorted(os.listdir(workdir)):
        if f.lower().endswith(".csv"):
            name = "".join(c if (c.isalnum() or c == "_") else "_" for c in f[:-4])
            t[name] = pd.read_csv(os.path.join(workdir, f))
    return t


def _orig_sql(source, workdir):
    """the original SQL, run by DuckDB if it is installed, else SQLite, over the CSV files in the data folder -> {query_1: DataFrame, ...}"""
    tables, res, k = _csv_tables(workdir), {}, 0
    try:
        import duckdb
        con = duckdb.connect()
        for n, df in tables.items():
            con.register(n, df)
        run = lambda q: con.execute(q).fetchdf()
        engine = "DuckDB"
    except ImportError:
        import sqlite3
        import pandas as pd
        con = sqlite3.connect(":memory:")
        for n, df in tables.items():
            df.to_sql(n, con, index=False)
        run = lambda q: pd.read_sql_query(q, con) if _is_query(q) else con.execute(q)
        engine = "SQLite"
    for stmt in _split_sql(source):
        try:
            r = run(stmt)
        except Exception as e:
            raise RuntimeError(f"the original SQL does not run in {engine} (a dialect-specific statement cannot be verified here): {type(e).__name__}: {str(e)[:200]}  [{stmt[:80]!r}]")
        if _is_query(stmt):
            k += 1
            res[f"query_{k}"] = r
    return res


def _conv_sql(converted, workdir):
    """the converted Spark SQL, run on the same CSV files (registered as views) -> {query_1: DataFrame, ...}"""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("sparqify-verify").config("spark.ui.enabled", "false").getOrCreate()
    for f in sorted(os.listdir(workdir)):
        if f.lower().endswith(".csv"):
            name = "".join(c if (c.isalnum() or c == "_") else "_" for c in f[:-4])
            spark.read.csv(os.path.join(workdir, f), header=True, inferSchema=True).createOrReplaceTempView(name)
    res, k = {}, 0
    for stmt in _split_sql(converted):
        r = spark.sql(stmt)
        if _is_query(stmt):
            k += 1
            res[f"query_{k}"] = r.toPandas()
    return res


_R_TAIL = """
.sqx_out <- %s
for (.n in ls()) {
  .v <- get(.n)
  if (is.data.frame(.v)) utils::write.csv(.v, file.path(.sqx_out, paste0(.n, ".csv")), row.names = FALSE)
  else if (is.numeric(.v) && length(.v) == 1 && !is.na(.v)) cat("SQX_SCALAR\t", .n, "\t", format(.v, digits = 17), "\n", sep = "")
}
"""


def _orig_r(source, workdir):
    """the original R script run with Rscript in the data folder; every data.frame (written to CSV) and numeric scalar it leaves behind"""
    import pandas as pd
    import subprocess
    out = tempfile.mkdtemp()
    path = os.path.join(out, "orig.R")
    with open(path, "w") as fh:
        fh.write(source + "\n" + _R_TAIL % json.dumps(out))
    try:
        p = subprocess.run(["Rscript", "--vanilla", path], cwd=workdir, capture_output=True, text=True, timeout=240, env=dict(os.environ, SPARQIFY_R_OUT=out))
    except FileNotFoundError:
        raise RuntimeError("Rscript was not found: R scripts can only be verified where R is installed (build the sandbox image with WITH_R=1)")
    if p.returncode != 0:
        raise RuntimeError("Rscript failed: " + (p.stderr or p.stdout)[-400:])
    res = {}
    for f in sorted(os.listdir(out)):
        if f.endswith(".csv"):
            res[f[:-4]] = pd.read_csv(os.path.join(out, f))
    for line in p.stdout.splitlines():
        if line.startswith("SQX_SCALAR\t"):
            _, n, v = line.split("\t")
            res[n] = float(v)
    return res


def _original_ns(language, code, workdir):
    """-> {name: value} produced by the ORIGINAL"""
    if language == "sql":
        return _orig_sql(code, workdir)
    if language == "r":
        return _orig_r(code, workdir)
    if language == "notebook":
        code = _notebook_code(code)
    return _run(code, workdir)


def _converted_ns(language, code, workdir):
    """-> {name: value} produced by the CONVERTED code"""
    if language == "sql":
        return _conv_sql(code, workdir)
    if language == "notebook":
        code = _notebook_code(code)
    # Spark resolves relative paths against the JVM's working directory, which os.chdir() in this process does not change (and which is
    # whatever folder the first session was started in). Make the relative data paths of the converted script absolute, inside `workdir`.
    patched = []
    try:
        from pyspark.sql import readwriter as _rw
        import functools

        def _fix(p):
            return os.path.join(workdir, p) if isinstance(p, str) and "://" not in p and not os.path.isabs(p) and not p.startswith("file:") else p

        def _wrap(orig):
            @functools.wraps(orig)
            def f(self, *a, **k):
                if a:
                    a = ([_fix(x) for x in a[0]] if isinstance(a[0], (list, tuple)) else _fix(a[0]),) + a[1:]
                if "path" in k:
                    k["path"] = _fix(k["path"])
                return orig(self, *a, **k)
            return f
        for cls, names in ((_rw.DataFrameReader, ("csv", "json", "parquet", "orc", "text", "load")),
                           (_rw.DataFrameWriter, ("csv", "json", "parquet", "orc", "text", "save"))):
            for nm in names:
                if hasattr(cls, nm):
                    patched.append((cls, nm, getattr(cls, nm)))
                    setattr(cls, nm, _wrap(getattr(cls, nm)))
    except ImportError:
        pass
    try:
        return _run(code, workdir)
    finally:
        for cls, nm, fn in patched:
            setattr(cls, nm, fn)


def _default_names(orig):
    return [k for k, v in orig.items() if not k.startswith("_") and _as_pandas(v) is not None]


VERIFIABLE = ("pandas", "polars", "dask", "notebook", "sql", "r")
_WHY_NOT = {
    "sas": "there is no SAS engine to run the original with",
    "scala": "the original needs a JVM build and the converted RDD code needs Spark jars: run both yourself and diff the output",
    "java": "the original needs a JVM build and the converted RDD code needs Spark jars: run both yourself and diff the output",
    "excel": "formulas live in a workbook with no table to run: compare against the workbook's own values by hand",
    "dbt": "models use Jinja and ref() and need the warehouse they run on",
    "python": "plain-Python results are lists and RDDs, not DataFrames: compare them yourself",
}


def unverifiable_reason(language):
    """None when `language` can be verified, else why not"""
    if language in VERIFIABLE:
        return None
    return _WHY_NOT.get(language) or "the original needs its own tool or database (Informatica, Teradata, a stored-procedure engine, ...) to run"


class NotVerifiable(Exception):
    pass


def verify(source: str, data_dir: str = ".", variables=None, rtol: float = 1e-6, language: str = "pandas", mode: str = "dataframe", target: str = "spark",
           dialect: str = "generic", original: str = None):
    """`original`: for SQL in a dialect the local engine (DuckDB) cannot run, a hand-written DuckDB-runnable equivalent to use as the reference result
    (the `source` is still the text that gets converted). Returns ({variable: [differences]}, report); raises ImportError if pyspark/pandas are missing and NotVerifiable for languages with no original runner."""
    why = unverifiable_reason(language)
    if why:
        raise NotVerifiable(f"{language}: {why}")
    import pandas as pd
    from .core import convert
    if language == "sql":
        converted, report = convert(source, "sql", sql_dialect=dialect, sql_out="sql")
    else:
        converted, report = convert(source, language, mode=mode, target=target)
    results = {}
    with tempfile.TemporaryDirectory() as t1, tempfile.TemporaryDirectory() as t2:
        for t in (t1, t2):
            shutil.copytree(data_dir, t, dirs_exist_ok=True)
        orig = _original_ns(language, original if (original and language == "sql") else source, t1)
        new = _converted_ns(language, converted, t2)
        # Spark DataFrames are lazy: they read the CSVs when first used, so the comparison must happen while the data folders still exist
        for n in variables or _default_names(orig):
            if n not in orig:
                results[n] = ["variable not present in the original"]
            elif n not in new:
                results[n] = ["variable not present in converted code"]
            else:
                errs = []
                b = _as_pandas(new[n], errs)
                results[n] = [("could not turn the converted value into a table" + (f": {errs[0]}" if errs else ""))] if b is None else _compare_values(n, orig[n], b, rtol)
    if language == "sql" and len([k for k in orig if k.startswith("query_")]) != len([k for k in new if k.startswith("query_")]):
        results["statements"] = ["the original and the converted SQL have a different number of SELECT statements"]
    return results, report


_HARNESS_BODY = r'''
def _side(label, fn, code, language):
    d = tempfile.mkdtemp()
    try:
        shutil.copytree(DATA, d, dirs_exist_ok=True)
        return fn(language, code, d), None
    except BaseException:
        return None, label + " failed: " + traceback.format_exc()[-700:]


out = {"results": {}, "error": None}
orig, err = _side("original", _original_ns, ORIGINAL, LANGUAGE)
if err:
    out["error"] = err
else:
    new, err = _side("converted", _converted_ns, CONVERTED, LANGUAGE)
    if err:
        out["error"] = err
    else:
        names = VARS or _default_names(orig)
        for n in names[:20]:
            if n not in orig:
                out["results"][n] = ["variable not present in the original"]
            elif n not in new:
                out["results"][n] = ["variable not present in converted code"]
            else:
                try:
                    errs = []
                    b = _as_pandas(new[n], errs)
                    out["results"][n] = ["could not turn the converted value into a table" + (": " + errs[0] if errs else "")] if b is None else [str(x)[:300] for x in _compare_values(n, orig[n], b, RTOL)[:5]]
                except Exception as e:
                    out["results"][n] = ["could not compare: " + type(e).__name__ + ": " + str(e)[:200]]
        if LANGUAGE == "sql" and len([k for k in orig if k.startswith("query_")]) != len([k for k in new if k.startswith("query_")]):
            out["results"]["statements"] = ["the original and the converted SQL have a different number of SELECT statements"]
sys.stdout.flush()
print("SPARQIFY_VERIFY_JSON:" + json.dumps(out))
'''


def build_harness(original: str, converted: str, variables=None, rtol: float = 1e-6, data: str = "/data", language: str = "pandas") -> str:
    """One self-contained script that runs the ORIGINAL code and the CONVERTED code on copies of the sample data and prints one JSON line
    with the differences. It is meant to run inside the no-network sandbox (see sandbox.py); sparqify itself never executes it on the host.
    The original and the converted text are embedded as literals read by sparqify, not supplied by a model, so a model cannot change what is compared."""
    import inspect
    why = unverifiable_reason(language)
    if why:
        raise NotVerifiable(f"{language}: {why}")
    funcs = "\n\n".join(inspect.getsource(f) for f in (_isnum, _norm, compare_frames, _as_pandas, _compare_values, _run, _notebook_code, _split_sql, _is_query, _csv_tables,
                                                       _orig_sql, _conv_sql, _orig_r, _original_ns, _converted_ns, _default_names)) + "\n\n_R_TAIL = " + repr(_R_TAIL)
    head = "import json, math, os, shutil, sys, tempfile, traceback\n\n" + funcs + "\n\n"
    consts = (f"ORIGINAL = {original!r}\nCONVERTED = {converted!r}\nVARS = {list(variables) if variables else None!r}\nRTOL = {float(rtol)!r}\n"
              f"DATA = {data!r}\nLANGUAGE = {language!r}\n")
    return head + consts + _HARNESS_BODY


def parse_harness_output(text: str):
    """-> dict {"results": {...}, "error": str|None} from the sandbox output, or None when the harness did not get as far as printing it"""
    import json
    for line in reversed(str(text).splitlines()):
        if line.startswith("SPARQIFY_VERIFY_JSON:"):
            try:
                d = json.loads(line[len("SPARQIFY_VERIFY_JSON:"):])
                return d if isinstance(d, dict) else None
            except ValueError:
                return None
    return None


def main(argv) -> int:
    import argparse
    p = argparse.ArgumentParser(prog="sparqify verify")
    p.add_argument("file")
    p.add_argument("--data", default=".", help="folder with the sample input files the script reads")
    p.add_argument("--vars", default="", help="comma-separated variables to compare (default: all DataFrames/numbers)")
    p.add_argument("--mode", default="dataframe", choices=["dataframe", "pandas-api"])
    p.add_argument("--language", default="", help="pandas, polars, dask, notebook, sql or r (default: detected from the file)")
    p.add_argument("--dialect", default="generic", help="SQL dialect of the original (SQL files only)")
    p.add_argument("--rtol", type=float, default=1e-6)
    p.add_argument("--gluten", action="store_true",
                   help="run the Spark side with the Gluten + Velox settings (the Gluten jar must be on the classpath, e.g. PYSPARK_SUBMIT_ARGS='--jars $GLUTEN_JAR ... pyspark-shell'); compares results with plain pandas")
    p.add_argument("--trust", action="store_true",
                   help="required: verify EXECUTES the script (and its pandas twin) on this machine with your permissions; only use it on code you wrote or reviewed")
    a = p.parse_args(argv)
    if not a.trust:
        print("verify runs the script: refusing without --trust (see SECURITY.md). Re-run with --trust only for code you have reviewed.")
        return 2
    from .core import detect_language
    src = open(a.file).read()
    lang = a.language or detect_language(src, a.file)
    try:
        results, _ = verify(src, a.data, [v for v in a.vars.split(",") if v] or None, a.rtol, language=lang, mode=a.mode, target="gluten" if a.gluten else "spark", dialect=a.dialect)
    except NotVerifiable as e:
        print(f"cannot verify: {e}")
        return 4
    except ImportError as e:
        print(f"verify needs pandas and pyspark installed ({e})")
        return 3
    except RuntimeError as e:
        print(f"the original could not be run: {e}")
        return 5
    bad = 0
    for n, d in results.items():
        print(("OK       " if not d else "MISMATCH ") + n)
        for line in d:
            print("           - " + line)
        bad += bool(d)
    print(f"--- {len(results) - bad} match, {bad} differ ---")
    return 1 if bad else 0
