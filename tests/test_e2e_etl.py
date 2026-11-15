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


# ------------------------------------------------------------------------------------------------ Informatica PowerCenter
class _Catalog:
    """stands in for `spark` when the converted code reads metastore tables (spark.table('DB.TABLE')): serves test DataFrames, delegates the rest"""

    def __init__(self, spark, tables):
        self._spark, self._tables = spark, {k.upper(): v for k, v in tables.items()}

    def table(self, name):
        return self._tables[name.upper()]

    def __getattr__(self, item):
        return getattr(self._spark, item)


def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.master("local[1]").appName("sparqix-e2e-etl").config("spark.ui.enabled", "false").getOrCreate()


def test_informatica_mapping_matches_expected():
    import datetime
    sp = _spark()
    d = datetime.date
    orders = [(1, 1, 100.0, d(2024, 1, 15), "A"), (2, 1, 1000.0, d(2024, 2, 10), "A"), (3, 2, 50.0, d(2024, 2, 20), "X"),      # 3: filtered by the source qualifier
              (4, 2, 10.0, d(2024, 3, 5), "A"), (5, 3, 0.0, d(2024, 3, 6), "A"),                                              # 5: total 0 -> reject
              (6, 4, 200.0, d(2024, 3, 7), "A"),                                                                              # 6: no such customer -> dropped by the inner join
              (7, 3, 300.0, d(2024, 4, 1), "A")]                                                                              # 7: customer without a region -> reject
    customers = [(1, "Ann"), (2, "Bob"), (3, "Cy")]
    regions = [(1, "north"), (1, "north"), (2, "south")]                                                                      # the duplicate row must not duplicate orders
    tables = {"SALES_DB.ORDERS": sp.createDataFrame(orders, "ORDER_ID int, CUST_ID int, AMOUNT double, ORDER_DT date, STATUS string"),
              "SALES_DB.CUSTOMERS": sp.createDataFrame(customers, "CUST_ID int, NAME string"),
              "SALES_DB.CUST_REGION": sp.createDataFrame(regions, "CUST_ID int, REGION string")}
    p = os.path.join(EX, "m_orders_fact.xml")
    code, rep = convert(open(p).read(), "powercenter", filename=p)
    ns = {"__name__": "converted"}
    exec(compile(code, "m_orders_fact", "exec"), ns)
    out = ns["m_orders_fact"](_Catalog(sp, tables), write=False)
    assert not rep.todos

    # independent expectation, straight from the mapping's rules
    o = pd.DataFrame(orders, columns=["ORDER_ID", "CUST_ID", "AMOUNT", "ORDER_DT", "STATUS"])
    o = o[o["STATUS"] != "X"].merge(pd.DataFrame(customers, columns=["CUST_ID", "NAME"]), on="CUST_ID", how="inner")
    o["TOTAL"] = (o["AMOUNT"] * 1.1).round(2)
    o["TIER"] = o["TOTAL"].map(lambda t: "gold" if t > 1000 else "silver" if t > 100 else "basic")
    o["ORDER_YM"] = pd.to_datetime(o["ORDER_DT"]).dt.strftime("%Y%m")
    o["CUST_NAME"] = o["NAME"].str.upper()
    o = o.merge(pd.DataFrame(regions, columns=["CUST_ID", "REGION"]).drop_duplicates("CUST_ID"), on="CUST_ID", how="left")
    valid = o[(o["TOTAL"] > 0) & o["REGION"].notna()]

    fact = out["ORDERS_FACT"].toPandas()
    assert sorted(fact["FACT_ID"]) == list(range(1, len(valid) + 1))                                     # surrogate key 1..n
    _eq(valid[["ORDER_ID", "CUST_NAME", "REGION", "TOTAL", "TIER", "ORDER_YM"]], fact[["ORDER_ID", "CUST_NAME", "REGION", "TOTAL", "TIER", "ORDER_YM"]], "ORDERS_FACT")
    assert fact["LOAD_TS"].notna().all()
    _eq(o[~o["ORDER_ID"].isin(valid["ORDER_ID"])][["ORDER_ID", "TOTAL"]], out["ORDERS_REJECT"].toPandas(), "ORDERS_REJECT")
    summary = valid.groupby("REGION").agg(REVENUE=("TOTAL", "sum"), N=("TOTAL", "size")).reset_index()
    _eq(summary, out["REGION_SUMMARY"].toPandas(), "REGION_SUMMARY")


# ------------------------------------------------------------------------------------------------ SSIS
def test_ssis_data_flow_matches_expected(tmp_path):
    import re
    sp = _spark()
    rows = [(1, 1, "EU", 1500.0, 2), (2, 1, "EU", 200.0, 1), (3, 2, "US", 50.0, 4), (4, 3, "EU", 100.0, 3), (5, 3, "ASIA", 2000.0, 1)]
    src = sp.createDataFrame(rows, "order_id int, customer_id int, region string, amount double, qty int")
    p = os.path.join(EX, "orders_flow.dtsx")
    code, rep = convert(open(p).read(), "ssis", filename=p)
    root = str(tmp_path)
    # the OLE DB source needs a database; feed the data flow from a DataFrame instead, and write the table target as a parquet folder
    code, n = re.subn(r"(d_ole_db_source = )spark\.read\.format\('jdbc'\).*", r"\1INPUT", code, count=1)
    assert n == 1
    assert ".saveAsTable('dbo.revenue_eu')" in code
    code = code.replace(".saveAsTable('dbo.revenue_eu')", f".parquet('{root}/out/revenue_eu')").replace("'/out/", f"'{root}/out/")
    ns = {"__name__": "converted", "INPUT": src}
    exec(compile(code, "orders_flow", "exec"), ns)
    ns["df_load_orders"]()

    t = pd.DataFrame(rows, columns=["order_id", "customer_id", "region", "amount", "qty"])
    t["total"] = t["amount"] * t["qty"]
    eu = t[t["region"] == "EU"]
    _eq(eu.groupby("customer_id")["total"].sum().reset_index(name="revenue"), sp.read.parquet(f"{root}/out/revenue_eu").toPandas(), "revenue_eu")
    other = t[t["region"] != "EU"].copy()
    other["tier"] = other["amount"].map(lambda a: "gold" if a > 1000 else "basic")
    other["label"] = other["region"].str.upper() + "-" + other["order_id"].astype(str)
    got = _csv_out(tmp_path, "orders_eu.csv")
    _eq(other[["order_id", "customer_id", "region", "amount", "qty", "total", "tier", "label"]], got[["order_id", "customer_id", "region", "amount", "qty", "total", "tier", "label"]], "orders_eu.csv")
    assert "weird" in got.columns and got["weird"].isna().all()           # the untranslatable TOKEN() column is an empty string column, and the file could be written
    assert any("FuzzyLookup" in str(t_) for t_ in rep.todos) and any("TOKEN" in str(t_) for t_ in rep.todos)
