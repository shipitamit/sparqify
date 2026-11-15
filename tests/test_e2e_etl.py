"""Real-Spark checks for the ETL converters that have no engine to run the original with (Pentaho, Pig, ...).
The converted program runs on small hand-made files; the expected result is computed independently in pandas, so a failure points at the
converted code or at the expectation (read both). Skipped unless pyspark is installed (the `spark-e2e` CI job installs it)."""
import glob
import os

import pytest

pyspark = pytest.importorskip("pyspark")
pd = pytest.importorskip("pandas")
from sparqix import verify as V  # noqa: E402
from sparqix.core import convert  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
EX = os.path.join(HERE, "..", "examples")

ORDERS = [  # order_id, customer_id, region, amount, qty
    (1, 1, "EU", 150.0, 2), (2, 1, "EU", 50.0, 3), (3, 2, "US", 200.0, 1), (4, 2, "EU", 120.0, 2),
    (5, 4, "EU", 500.0, 1), (6, 3, "EU", 101.0, 3), (7, 3, "EU", 100.0, 5), (8, 1, "eu", 400.0, 1)]
CUSTOMERS = [(1, "Ann"), (2, "Bob"), (3, "Cy")]            # customer 4 has no row: the left join must keep order 5 with a NULL name


@pytest.fixture(autouse=True)
def _spark_environment_is_usable():
    major = int(pyspark.__version__.split(".")[0])
    if major < 4:
        try:
            import distutils.version  # noqa: F401
        except ImportError:
            pytest.fail(f"pyspark {pyspark.__version__} needs `distutils`, which this Python lacks. Use Python 3.11 (what CI uses) or pyspark>=4.")


def _write_inputs(root, header):
    os.makedirs(os.path.join(root, "data"))
    pd.DataFrame(ORDERS, columns=["order_id", "customer_id", "region", "amount", "qty"]).to_csv(os.path.join(root, "data", "orders.csv"), index=False, header=header)
    pd.DataFrame(CUSTOMERS, columns=["customer_id", "name"]).to_csv(os.path.join(root, "data", "customers.csv"), index=False, header=header)


def _run(example, language, tmp_path, header, rewrites=()):
    """convert `example`, point its /data and /out paths into tmp_path, run it on Spark, return (namespace, report)"""
    code, report = convert(open(os.path.join(EX, example)).read(), language, filename=os.path.join(EX, example))
    root = str(tmp_path)
    _write_inputs(root, header)
    code = code.replace("'/data/", f"'{root}/data/").replace("'/out/", f"'{root}/out/")
    for old, new in rewrites:
        assert old in code, old
        code = code.replace(old, new.format(root=root))
    ns = {"__name__": "converted"}
    exec(compile(code, example, "exec"), ns)
    return ns, report


def _csv_out(tmp_path, name, **kw):
    parts = sorted(glob.glob(os.path.join(str(tmp_path), "out", name, "part-*")))
    assert parts, f"no output written to out/{name}"
    return pd.concat([pd.read_csv(p, **kw) for p in parts], ignore_index=True)


def _eq(expected, got, name):
    bad = V.compare_frames(expected, got)
    assert not bad, f"{name}: {bad}"


def test_pentaho_transformation_matches_expected(tmp_path):
    ns, rep = _run("orders.ktr", "pentaho", tmp_path, header=True,
                   # the example saves to a Hive-style table (stg.small_orders); write a parquet folder instead so the test needs no metastore
                   rewrites=[(".saveAsTable('stg.small_orders')", ".parquet('{root}/out/small_orders')")])
    o = pd.DataFrame(ORDERS, columns=["order_id", "customer_id", "region", "amount", "qty"])
    c = pd.DataFrame(CUSTOMERS, columns=["customer_id", "name"])
    big = o[o["amount"] > 100].assign(total=lambda d: d["amount"] * d["qty"])
    expected = big.merge(c, on="customer_id", how="left").groupby("name", dropna=False)["total"].sum().reset_index(name="revenue")
    _eq(expected, ns["s_Sum"].toPandas(), "revenue per customer")
    _eq(expected, _csv_out(tmp_path, "revenue", sep=";"), "revenue.csv written with ';'")
    _eq(o[~(o["amount"] > 100)], ns["spark"].read.parquet(os.path.join(str(tmp_path), "out", "small_orders")).toPandas(), "orders at or below 100 (the table write)")
    assert any("Denormaliser" in str(t) for t in rep.todos)           # the one step it cannot convert is reported, not guessed


def test_pig_script_matches_expected(tmp_path):
    ns, rep = _run("orders.pig", "pig", tmp_path, header=False)
    o = pd.DataFrame(ORDERS, columns=["order_id", "customer_id", "region", "amount", "qty"])
    c = pd.DataFrame(CUSTOMERS, columns=["customer_id", "name"])
    big = o[(o["amount"] > 100) & (o["region"] == "EU")]
    calc = big.assign(total=big["amount"] * big["qty"])
    joined = calc.merge(c, on="customer_id", how="left")
    expected = joined.groupby("name", dropna=False).agg(revenue=("total", "sum"), n=("total", "size")).reset_index()
    _eq(expected, ns["agg"].toPandas(), "revenue and count per customer")
    top = _csv_out(tmp_path, "top", header=None, names=["name", "revenue", "n"], keep_default_na=True)
    _eq(expected, top, "top written without a header")
    assert list(ns["srt"].toPandas()["revenue"]) == sorted(expected["revenue"], reverse=True)       # ORDER ... DESC
    assert len(rep.todos) == 2                                                                      # FLATTEN(TOKENIZE()) and the UDF
