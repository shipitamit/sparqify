"""End-to-end check on REAL Spark: run each original script in tests/e2e/cases and its converted PySpark code on the same data, diff the results.
Skipped unless pyspark is installed (the `spark-e2e` CI job installs it). Add a case by dropping a script into tests/e2e/cases/;
SQL cases may carry a dialect in the name: `name.<dialect>.sql`."""
import glob
import os

import pytest

pyspark = pytest.importorskip("pyspark")
from sparqix import verify as V  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "e2e", "data")
CASES = sorted(glob.glob(os.path.join(HERE, "e2e", "cases", "*")))
_LANG = {".py": "pandas", ".sql": "sql", ".R": "r"}


def _lang(path):
    return _LANG[os.path.splitext(path)[1]]


@pytest.mark.parametrize("path", CASES, ids=[os.path.basename(c) for c in CASES])
def test_converted_matches_original(path):
    lang = _lang(path)
    if lang == "r":
        import shutil
        if not shutil.which("Rscript"):
            pytest.skip("Rscript not installed")
    parts = os.path.basename(path).split(".")
    dialect = parts[-2] if lang == "sql" and len(parts) > 2 else "generic"
    results, report = V.verify(open(path).read(), DATA, language=lang, dialect=dialect)
    assert results, "nothing was compared"
    bad = {k: v for k, v in results.items() if v}
    assert not bad, bad
