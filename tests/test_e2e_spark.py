"""End-to-end check on REAL Spark: run each original script in tests/e2e/cases and its converted PySpark code on the same data, diff the results.
Skipped unless pyspark is installed (the `spark-e2e` CI job installs it). Add a case by dropping a script into tests/e2e/cases/;
SQL cases may carry a dialect in the name: `name.<dialect>.sql`; a dialect DuckDB cannot run (Teradata, BigQuery, Databricks) gets a hand-written DuckDB equivalent `name.<dialect>.oracle.sql` as the reference; Polars / Dask cases are `name.polars.py` / `name.dask.py`."""
import glob
import os

import pytest

pyspark = pytest.importorskip("pyspark")
from sparqix import verify as V  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "e2e", "data")
CASES = sorted(c for c in glob.glob(os.path.join(HERE, "e2e", "cases", "*")) if not c.endswith(".oracle.sql"))
_LANG = {".py": "pandas", ".sql": "sql", ".R": "r"}


def _lang(path):
    """language from the extension; a .py case may name its library: `name.polars.py`, `name.dask.py` (plain `.py` is pandas)"""
    parts = os.path.basename(path).split(".")
    if parts[-1] == "py" and len(parts) > 2 and parts[-2] in ("polars", "dask"):
        return parts[-2]
    return _LANG[os.path.splitext(path)[1]]


@pytest.fixture(autouse=True)
def _spark_environment_is_usable():
    """PySpark 3.x imports `distutils`, which Python 3.12+ removed: fail with the fix instead of a confusing per-case error."""
    major = int(pyspark.__version__.split(".")[0])
    if major < 4:
        try:
            import distutils.version  # noqa: F401
        except ImportError:
            pytest.fail(f"pyspark {pyspark.__version__} needs `distutils`, which this Python lacks. Use Python 3.11 (what CI uses), "
                        "or `pip install setuptools`, or `pip install 'pyspark>=4'` (needs Java 17+).")


@pytest.mark.parametrize("path", CASES, ids=[os.path.basename(c) for c in CASES])
def test_converted_matches_original(path):
    lang = _lang(path)
    if lang == "r":
        import shutil
        if not shutil.which("Rscript"):
            pytest.skip("Rscript not installed")
    if lang in ("polars", "dask"):
        pytest.importorskip(lang)
    parts = os.path.basename(path).split(".")
    dialect = parts[-2] if lang == "sql" and len(parts) > 2 else "generic"
    oracle = path[:-len(".sql")] + ".oracle.sql" if lang == "sql" else ""
    results, report = V.verify(open(path).read(), DATA, language=lang, dialect=dialect,
                               original=open(oracle).read() if oracle and os.path.exists(oracle) else None)
    assert results, "nothing was compared"
    bad = {k: v for k, v in results.items() if v}
    assert not bad, bad
