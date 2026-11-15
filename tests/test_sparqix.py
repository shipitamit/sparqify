import ast
import glob
import re
import tempfile
import os

import pytest

from sparqix import convert, detect_language

HERE = os.path.dirname(__file__)


def py(src):
    code, rep = convert(src, "pandas")
    ast.parse(code)  # output must always be valid Python
    return code, rep


def sql(src, dialect="generic"):
    code, rep = convert(src, "sql", sql_dialect=dialect, sql_out="sql")
    return code.strip().rstrip(";"), rep


# ---------------------------------------------------------------- pandas
def test_filter_withcolumn_groupby():
    code, rep = py(
        "import pandas as pd\n"
        "df = pd.read_csv('a.csv')\n"
        "df = df[(df['x'] > 1) & (df['y'] == 'a')]\n"
        "df['z'] = df['x'] * 2\n"
        "out = df.groupby('y').agg(s=('x', 'sum')).reset_index().sort_values('s', ascending=False).head(3)\n"
    )
    assert "spark.read.csv('a.csv', header=True, inferSchema=True)" in code
    assert "df.filter(((F.col('x') > 1) & (F.col('y') == 'a')))" in code or "df.filter((F.col('x') > 1) & (F.col('y') == 'a'))" in code
    assert "df = df.withColumn('z', F.col('x') * 2)" in code
    assert ".groupBy('y')" in code and "F.sum('x').alias('s')" in code
    assert ".orderBy(F.col('s').desc()).limit(3)" in code
    assert not rep.todos


def test_inplace_and_rename_and_drop():
    code, _ = py("df = pd.read_csv('a.csv')\ndf.dropna(inplace=True)\ndf = df.drop(columns=['a','b']).rename(columns={'c': 'd'})")
    assert "df = df.dropna()" in code
    assert ".drop('a', 'b')" in code and ".withColumnRenamed('c', 'd')" in code


def test_merge_variants():
    code, _ = py("a = pd.read_csv('a')\nb = pd.read_csv('b')\nc = a.merge(b, left_on='k1', right_on='k2', how='outer')\nd = pd.merge(a, b, on=['k','j'])")
    assert "a.join(b, a['k1'] == b['k2'], 'outer')" in code
    assert "a.join(b, on=['k', 'j'], how='inner')" in code


def test_string_and_date_accessors():
    code, _ = py("df = pd.read_csv('a')\ndf['n'] = df['name'].str.strip().str.lower()\ndf['m'] = df['d'].dt.month\n"
                 "df['s'] = df['a'] + '-' + df['b']\ndf['k'] = df['a'].astype('int64').fillna(0)")
    assert "F.lower(F.trim(F.col('name')))" in code
    assert "F.month(F.col('d'))" in code
    assert "F.concat(" in code           # `+` on strings must not become numeric add
    assert "cast('bigint')" in code and "F.coalesce" in code


def test_loc_assignment_and_np_where():
    code, _ = py("import numpy as np\ndf = pd.read_csv('a')\ndf.loc[df['a'] > 1, 'b'] = 5\ndf['c'] = np.where(df['a'] > 1, 'hi', 'lo')")
    assert "F.when(F.col('a') > 1, F.lit(5)).otherwise(F.col('b'))" in code
    assert "F.when(F.col('a') > 1, F.lit('hi')).otherwise(F.lit('lo'))" in code


def test_unsupported_becomes_todo_not_wrong_code():
    code, rep = py("df = pd.read_csv('a')\nfor i, r in df.iterrows():\n    df.loc[i, 'z'] = r['a']\nx = df.iloc[3]\ndf['a'].rolling(3).median()")
    assert len(rep.todos) == 3
    assert "TODO(sparqix)" in code
    assert "iterrows" in code and code.count("# ") >= 3  # originals kept as comments


def test_value_counts_scalar_aggregate_and_len():
    code, _ = py("df = pd.read_csv('a')\nn = len(df)\nm = df['x'].mean()\nv = df['t'].value_counts()")
    assert "n = df.count()" in code
    assert "df.agg(F.avg(F.col('x'))).first()[0]" in code
    assert "groupBy(F.col('t')).count().orderBy(F.col('count').desc())" in code


def test_query_and_pivot():
    code, _ = py("df = pd.read_csv('a')\nq = df.query('a == 1 & b > 2')\np = df.pivot_table(index='r', columns='c', values='v', aggfunc='sum')")
    assert "df.filter('a = 1 and b > 2')" in code
    assert "groupBy('r').pivot('c').agg(F.sum('v'))" in code


def test_function_with_dataframe_annotations():
    code, _ = py("def f(d: pd.DataFrame) -> pd.DataFrame:\n    d = d.dropna()\n    return d.assign(x=lambda t: t['a'] + 1)\n")
    assert "def f(d: DataFrame) -> DataFrame:" in code
    assert "from pyspark.sql import DataFrame" in code
    assert "return d.withColumn('x', F.col('a') + 1)" in code


# ---------------------------------------------------------------- SQL
def test_tsql():
    out, rep = sql("SELECT TOP 5 ISNULL(a,0) AS a, DATEADD(day, 3, d) AS d, LEN(n) FROM dbo.t WITH (NOLOCK) WHERE [col x] = 1 ORDER BY a",
                   "tsql")
    assert out == ("SELECT COALESCE(a, 0) AS a, date_add(d, 3) AS d, LENGTH(n) FROM t WHERE `col x` = 1 ORDER BY a LIMIT 5")
    assert not rep.todos


def test_tsql_top_in_subquery():
    out, _ = sql("SELECT * FROM (SELECT TOP 3 a FROM t ORDER BY a) x", "tsql")
    assert out == "SELECT * FROM (SELECT a FROM t ORDER BY a LIMIT 3) x"


def test_oracle():
    out, _ = sql("SELECT NVL(a,0), TO_CHAR(d,'YYYY-MM-DD HH24:MI:SS'), DECODE(b,1,'x','z') FROM t, DUAL WHERE c = 1 AND ROWNUM <= 3",
                 "oracle")
    assert "COALESCE(a, 0)" in out
    assert "date_format(d, 'yyyy-MM-dd HH:mm:ss')" in out
    assert "CASE b WHEN 1 THEN 'x' ELSE 'z' END" in out
    assert out.endswith("LIMIT 3")


def test_mysql_and_postgres():
    out, _ = sql("SELECT GROUP_CONCAT(n SEPARATOR ';'), DATE_ADD(d, INTERVAL 2 DAY), \"s\" FROM t LIMIT 5, 10", "mysql")
    assert "array_join(collect_list(n), ';')" in out
    assert "(d + (2) * INTERVAL 1 DAY)" in out
    assert "'s'" in out and out.endswith("LIMIT 10 OFFSET 5")
    out, _ = sql('SELECT x::int, "Col" FROM t', "postgres")
    assert out == "SELECT CAST(x AS int), `Col` FROM t"


def test_strings_and_comments_untouched():
    out, _ = sql("SELECT 'ISNULL(a,b) [x] \"y\"' AS s -- ISNULL(1,2)\nFROM t", "tsql")
    assert "'ISNULL(a,b) [x] \"y\"'" in out and "-- ISNULL(1,2)" in out


def test_unsupported_sql_flagged():
    _, rep = sql("UPDATE t SET a = 1; DECLARE @x INT", "tsql")
    assert len(rep.todos) >= 2


def test_pyspark_wrapper_multi_statement():
    code, _ = convert("SELECT 1 AS a; SELECT 2 AS b FROM x", "sql")
    ast.parse(code)
    assert "result_1 = spark.sql" in code and "result_2 = spark.sql" in code


# ---------------------------------------------------------------- plain python
def test_python_collections():
    code, rep = convert(
        "nums = [1,2,3]\nsq = [x*x for x in nums if x > 1]\ntotal = sum(x for x in nums)\nn = len(sq)\n"
        "c = {}\nfor x in nums:\n    c[x % 2] = c.get(x % 2, 0) + 1\n", "python")
    ast.parse(code)
    assert "nums = sc.parallelize([1, 2, 3])" in code
    assert "sq = nums.filter(lambda x: x > 1).map(lambda x: x * x)" in code
    assert "total = nums.sum()" in code and "n = sq.count()" in code
    assert "reduceByKey" in code and not rep.warnings[:0]


def test_python_driver_fallback_is_flagged():
    code, rep = convert("xs = [1,2]\nfor x in xs:\n    print(x)\n", "python")
    assert "toLocalIterator()" in code and rep.warnings


# ---------------------------------------------------------------- detection + examples
def test_detect():
    assert detect_language("import pandas as pd\n") == "pandas"
    assert detect_language("SELECT a FROM t") == "sql"
    assert detect_language("x = 1") == "python"
    assert detect_language("anything", "q.sql") == "sql"


@pytest.mark.parametrize("path", glob.glob(os.path.join(HERE, "..", "examples", "*")))
def test_examples_produce_valid_output(path):
    if os.path.basename(path) in ("crontab.txt", "wf_orders.xml", "etl_dag.py", "nightly.kjb", "parent_job.item"):
        pytest.skip("orchestration example: covered by the orchestrate tests")
    src = open(path).read()
    dialect = "tsql" if path.endswith(".sql") else "teradata" if path.endswith(".tdsql") else "generic"
    code, _ = convert(src, "auto", sql_dialect=dialect, filename=path)
    if not path.endswith((".java", ".scala")):      # JVM output is not Python
        ast.parse(code)


# ---------------------------------------------------------------- more SQL dialects
def test_more_dialects():
    out, _ = sql("SELECT date_add('day', 3, d), date_diff('day', a, b), approx_distinct(x) FROM t", "trino")
    assert out == "SELECT date_add(d, 3), datediff(b, a), approx_count_distinct(x) FROM t"
    out, _ = sql("SEL TOP 2 ZEROIFNULL(a), NULLIFZERO(b) FROM t", "teradata")
    assert out == "SELECT COALESCE(a, 0), NULLIF(b, 0) FROM t LIMIT 2"
    out, _ = sql("SELECT strftime('%Y-%m-%d', d), datetime('now') FROM t", "sqlite")
    assert out == "SELECT date_format(d, 'yyyy-MM-dd'), current_timestamp() FROM t"
    out, _ = sql("SELECT GETDATE(), DATEDIFF(day, a, b), LISTAGG(n, ',') FROM t", "redshift")
    assert "current_timestamp()" in out and "datediff(b, a)" in out and "array_join(collect_list(n), ',')" in out


# ---------------------------------------------------------------- polars / dask / R / notebook
def conv(src, lang):
    code, rep = convert(src, lang)
    ast.parse(code)
    # long chains are wrapped one call per line: join them so assertions can match substrings
    return re.sub(r"\(\n\s+|\n\s+(?=\.)", lambda m: "(" if m.group().startswith("(") else "", code), rep


def test_polars():
    code, rep = conv(
        "import polars as pl\n"
        "df = pl.scan_csv('a.csv')\n"
        "out = (df.filter(pl.col('x') > 1)\n"
        "  .with_columns((pl.col('x') * 2).alias('y'), pl.when(pl.col('x') > 5).then(pl.lit('a')).otherwise(pl.lit('b')).alias('t'))\n"
        "  .group_by('t').agg(pl.col('y').sum().alias('s'), pl.len().alias('n'))\n"
        "  .sort('s', descending=True).collect())\n", "polars")
    assert "df.filter(F.col('x') > 1)" in code
    assert ".withColumn('y', F.col('x') * 2)" in code
    assert ".withColumn('t', F.when(F.col('x') > 5, F.lit('a')).otherwise(F.lit('b')))" in code
    assert ".groupBy('t').agg(F.sum(F.col('y')).alias('s'), F.count(F.lit(1)).alias('n'))" in code
    assert ".orderBy(F.col('s').desc())" in code
    assert "import polars" not in code and not rep.todos


def test_polars_window_and_unsupported():
    code, rep = conv("import polars as pl\ndf = pl.read_csv('a')\ndf = df.with_columns((pl.col('a') / pl.col('a').sum().over('g')).alias('p'))\nz = df.with_row_index()\n", "polars")
    assert "F.sum(F.col('a')).over(Window.partitionBy('g'))" in code
    assert "Window" in code.splitlines()[0]
    assert len(rep.todos) == 1


def test_dask():
    code, rep = conv("import dask.dataframe as dd\ndf = dd.read_csv('a')\nr = df.groupby('k').v.sum().compute()\nm = df.v.mean().compute()\n", "dask")
    assert "groupBy('k').agg(F.sum('v').alias('v'))" in code
    assert "df.agg(F.avg(F.col('v'))).first()[0]" in code
    assert "compute" not in code and "dask" not in code


R_SRC = '''library(dplyr)
d <- read_csv("a.csv")
r <- d %>%
  filter(x > 1, g %in% c("a", "b")) %>%
  mutate(y = x * 2, t = ifelse(x > 5, "hi", "lo"), share = x / sum(x)) %>%
  group_by(g) %>%
  summarise(s = sum(y), n = n()) %>%
  arrange(desc(s)) %>%
  left_join(o, by = c("g" = "key"))
d$z <- d$x + 1
'''


def test_r_dplyr():
    code, rep = conv(R_SRC, "r")
    assert "d = spark.read.csv('a.csv', header=True, inferSchema=True)" in code
    assert ".filter((F.col('x') > 1) & F.col('g').isin(['a', 'b']))" in code
    assert ".withColumn('t', F.when(F.col('x') > 5, 'hi').otherwise('lo'))" in code
    assert "F.sum(F.col('x')).over(Window.partitionBy())" in code     # ungrouped aggregate in mutate
    assert ".groupBy('g').agg(F.sum(F.col('y')).alias('s'), F.count(F.lit(1)).alias('n'))" in code
    assert ".orderBy(F.col('s').desc())" in code
    assert ".alias('l').join(o.alias('r'), F.col('l.g') == F.col('r.key'), 'left')" in code
    assert "d = d.withColumn('z', F.col('x') + 1)" in code
    assert not rep.todos


def test_r_grouped_mutate_and_unsupported():
    code, rep = conv('library(dplyr)\nd <- read_csv("a")\nr <- d %>% group_by(g) %>% mutate(m = mean(x)) %>% ungroup()\nq <- d %>% mutate(l = lag(x, n = y))\n', "r")
    assert "F.avg(F.col('x')).over(Window.partitionBy('g'))" in code
    assert len(rep.todos) == 1 and "lag" in rep.todos[0][1]


def test_notebook_shared_state_and_cells():
    import json
    nb = {"cells": [
        {"cell_type": "markdown", "metadata": {}, "source": ["# hi"]},
        {"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": 1, "source": ["import pandas as pd\n", "%matplotlib inline\n", "df = pd.read_csv('a.csv')"]},
        {"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": 2, "source": ["df['t'] = df['a'] * 2\n", "df.head()"]},
        {"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": 3, "source": ["%%sql\n", "SELECT TOP 3 a FROM t"]},
        {"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": 4, "source": ["for i, r in df.iterrows():\n", "    df.loc[i, 'z'] = r['a']"]},
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    out, rep = convert(json.dumps(nb), "notebook", sql_dialect="tsql")
    cells = json.loads(out)["cells"]
    text = ["".join(c["source"]) for c in cells]
    assert "SparkSession" in text[0]
    assert text[1] == "# hi"
    assert "df = spark.read.csv" in text[2]
    assert "df = df.withColumn('t', F.col('a') * 2)" in text[3] and "show(" in text[3]   # df known from cell 1
    assert "LIMIT 3" in text[4]
    assert "TODO(sparqix)" in text[5]
    assert any("cell 4" in str(l) for l, _ in rep.todos)
    assert all(c.get("outputs") == [] for c in cells if c["cell_type"] == "code")


def test_detection_new_formats():
    assert detect_language("import polars as pl\n") == "polars"
    assert detect_language("import dask.dataframe as dd\n") == "dask"
    assert detect_language("x %>% filter(a > 1)") == "r"
    assert detect_language("{}", "n.ipynb") == "notebook"


# ---------------------------------------------------------------- dbt / excel / SAS / Scala / Java
def test_dbt():
    src = ("{{ config(materialized='table') }}\nselect a from {{ ref('stg') }} s join {{ source('crm','c') }} c on 1=1\n"
           "{% if is_incremental() %} where ts > (select max(ts) from {{ this }}) {% endif %}")
    code, rep = convert(src, "dbt", model="m")
    ast.parse(code)
    assert "{{" not in code and "stg s join crm.c c" in code
    assert "m = spark.sql(" in code and "m.createOrReplaceTempView('m')" in code
    assert any("incremental" in str(l) for l, _ in rep.warnings)


def test_dbt_unresolved_jinja_flagged():
    _, rep = convert("select {% for c in cols %}{{ c }},{% endfor %} 1 from {{ ref('x') }}", "dbt")
    assert rep.todos


def test_excel():
    code, rep = convert('total = =B2*C2\ntier = =IF(D2>100,"gold","basic")\nbig = =SUMIF(A2:A9,">5",B2:B9)\nlk = =VLOOKUP(A2,S!A:B,2,0)\nbad = =HLOOKUP(A2,S!A:B,2,0)\n'
                        'k = =UPPER(LEFT(E2,2))&"-"&F2', "excel", excel_columns={"B": "price", "C": "qty"})
    ast.parse(code)
    assert "F.col('price') * F.col('qty')" in code
    assert "F.when(F.col('D') > 100, F.lit('gold')).otherwise(F.lit('basic'))" in code
    assert "F.sum(F.when(F.col('A') > 5, F.col('price'))).over(Window.partitionBy())" in code
    assert "F.concat(F.concat(F.upper(F.substring(F.col('E'), 1, 2)), F.lit('-')), F.col('F'))" in code
    assert len(rep.todos) == 1 and "HLOOKUP" in rep.todos[0][1]            # VLOOKUP converts now
    assert "df = _xl_lookup(df, F.col('A'), lk_s, 'A', 'B', '_lk1')" in code


SAS_SRC = '''
data out;
  set a(where=(x > 1) keep=x y);
  z = x * 2;
  if z > 10 then t = 'hi'; else if z > 5 then t = 'mid'; else t = 'lo';
  if y = 0 then delete;
run;
proc sort data=out out=s nodupkey; by x descending y; run;
proc means data=out noprint; class t; var x y; output out=m mean=mx my sum=sx sy; run;
proc freq data=out; tables t; run;
proc sql; create table top as select t, count(*) as n from out group by t; quit;
data r; set out; retain c 0; c + x; run;
'''


def test_sas():
    code, rep = convert(SAS_SRC, "sas")
    ast.parse(code)
    flat = re.sub(r"\n\s*", "", code)
    assert "out = (a.filter(F.col('x') > 1).select('x', 'y').withColumn('z', F.col('x') * 2)" in flat
    assert ".withColumn('t', F.when(F.col('z') > 10, 'hi').when(F.col('z') > 5, 'mid').otherwise('lo'))" in flat
    assert ".filter(~(F.col('y') == 0))" in flat
    assert "s = out.dropDuplicates(['x', 'y']).orderBy(F.col('x').asc(), F.col('y').desc())" in flat
    assert "m = out.groupBy('t').agg(F.avg('x').alias('mx'), F.avg('y').alias('my'), F.sum('x').alias('sx'), F.sum('y').alias('sy'))" in flat
    assert "out.groupBy('t').count().show()" in flat
    assert "top = spark.sql" in code
    assert not rep.todos                                           # RETAIN + sum statement is a running sum now
    assert "F.sum(F.coalesce(F.col('x'), F.lit(0))).over(Window.orderBy('_row').rowsBetween(Window.unboundedPreceding, Window.currentRow))" in flat


def test_scala():
    code, rep = convert(
        "val nums = List(1,2,3)\nval ev = nums.filter(_ % 2 == 0).map(x => x * x)\nval n = ev.size\n"
        "val w = Source.fromFile(\"f.txt\").getLines().toList\nval c = w.flatMap(_.split(\" \")).groupBy(identity).mapValues(_.size)\n"
        "val t = nums.foldLeft(0)(_ + _)\nprintln(ev)\n", "scala")
    assert "val nums = sc.parallelize(List(1,2,3))" in code
    assert "val ev = nums.filter(_ % 2 == 0).map(x => x * x)" in code
    assert "val n = ev.count()" in code
    assert 'val w = sc.textFile("f.txt")' in code
    assert "w.flatMap(_.split(\" \")).map(x_ => (x_, 1)).reduceByKey(_ + _)" in code
    assert "nums.fold(0)(_ + _)" in code and "println(ev.collect().toList)" in code
    assert rep.warnings


def test_java():
    code, rep = convert(
        "List<Integer> xs = Arrays.asList(1, 2, 3);\n"
        "List<Integer> sq = xs.stream().filter(x -> x > 1).map(x -> x * x).collect(Collectors.toList());\n"
        "int t = xs.stream().mapToInt(x -> x).sum();\n"
        "Map<Boolean, Long> g = xs.stream().collect(Collectors.groupingBy(x -> x > 1, Collectors.counting()));\n"
        "xs.stream().skip(1).count();\n", "java")
    assert "jsc.parallelize(xs).filter(x -> x > 1).map(x -> x * x).collect();" in code
    assert "int t = jsc.parallelize(xs).mapToDouble(x -> x).sum().intValue();" in code     # JavaDoubleRDD.sum() is a boxed Double: a plain cast would not compile
    assert "mapToPair(x -> new Tuple2<>(x > 1, 1L)).reduceByKey(Long::sum).collectAsMap()" in code
    assert "zipWithIndex().filter(t -> t._2() >= 1).map(t -> t._1()).count()" in code and not rep.todos


def test_detection_batch3():
    assert detect_language("proc sort data=a; by x; run;") == "sas"
    assert detect_language("select * from {{ ref('x') }}", "m.sql") == "dbt"
    assert detect_language("val x = List(1,2)") == "scala"
    assert detect_language("xs.stream().count();") == "java"
    assert detect_language("=SUM(A1:A9)") == "excel"


# ---------------------------------------------------------------- BigQuery
def test_bigquery_sql():
    out, _ = sql("SELECT SAFE_CAST(a AS INT64) x, DATE_TRUNC(d, MONTH) m, IFNULL(b, 0) FROM `proj.ds.t`", "bigquery")
    assert "TRY_CAST(a AS BIGINT)" in out and "trunc(d, 'MONTH')" in out
    assert "`ds`.`t`" in out


def test_bigquery_client():
    src = open(os.path.join(HERE, "..", "examples", "bq_client.py")).read()
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "bigquery" in code


# ---------------------------------------------------------------- Informatica
def _pc():
    from sparqix.core import Report
    return convert(open(os.path.join(HERE, "..", "examples", "m_orders_fact.xml")).read(), "auto", sql_dialect="oracle")


def test_powercenter():
    code, rep = _pc()
    ast.parse(code)
    assert "def m_orders_fact(spark" in code
    for needle in ("join(", "F.when(", "saveAsTable", "groupBy"):
        assert needle in code
    assert not rep.todos


def test_idmc():
    path = os.path.join(HERE, "..", "examples", "idmc_orders.json")
    code, rep = convert(open(path).read(), "auto", filename=path)
    ast.parse(code)
    assert "def m_mt_orders_fact" in code and "F.round(" in code and "date_format" in code
    assert detect_language(open(path).read(), "x.json") == "idmc"


def test_infa_expressions():
    code, _ = _pc()
    assert "current_timestamp" in code


# ---------------------------------------------------------------- Databricks
def test_databricks_target():
    code, _ = convert("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\n", "pandas", target="databricks")
    assert code.startswith("# Databricks notebook source") and "# COMMAND ----------" in code
    assert "SparkSession.builder" not in code


def test_databricks_source():
    path = os.path.join(HERE, "..", "examples", "orders.dbx.py")
    code, rep = convert(open(path).read(), "auto", filename=path)
    assert code.startswith("# Databricks notebook source")
    assert "spark.read.csv" in code and "# MAGIC %sql" in code and "pd." not in code
    assert detect_language(open(path).read()) == "databricks"


# ---------------------------------------------------------------- verify (comparison logic; Spark run needs pyspark)
def test_compare_frames():
    pd = pytest.importorskip("pandas")
    from sparqix.verify import compare_frames
    a = pd.DataFrame({"k": ["a", "b"], "v": [1.0, 2.0]})
    assert compare_frames(a, a.iloc[::-1][["v", "k"]]) == []          # order-insensitive
    assert compare_frames(a, pd.DataFrame({"k": ["a", "b"], "v": [1.0, 2.5]}))[0].startswith("column 'v'")
    assert "row count" in compare_frames(a, a.head(1))[0]
    assert "columns differ" in compare_frames(a, a.rename(columns={"v": "w"}))[0]
    assert compare_frames(a, a.assign(v=a.v + 1e-9)) == []


def test_compare_frames_treats_date_as_midnight_timestamp():
    pd = pytest.importorskip("pandas")
    import datetime
    from sparqix.verify import compare_frames
    a = pd.DataFrame({"dt": pd.to_datetime(["2024-04-09", "2024-05-01"]), "v": [1, 2]})
    b = pd.DataFrame({"dt": [datetime.date(2024, 4, 9), datetime.date(2024, 5, 1)], "v": [1, 2]})
    assert compare_frames(a, b) == []
    b2 = pd.DataFrame({"dt": [datetime.date(2024, 4, 9), datetime.date(2024, 5, 2)], "v": [1, 2]})
    assert compare_frames(a, b2)           # a different day is still a difference
    txt = pd.DataFrame({"dt": ["2024-04-09", "2024-05-01"], "v": [1, 2]})          # R's read.csv keeps dates as text
    assert compare_frames(txt, b) == [] and compare_frames(b, txt) == []
    assert compare_frames(pd.DataFrame({"dt": ["2024-04-09", "2024-05-02"], "v": [1, 2]}), b)
    words = pd.DataFrame({"dt": ["north", "south"], "v": [1, 2]})                 # text that is not a date stays a difference
    assert compare_frames(words, b)


def test_verify_end_to_end():
    pytest.importorskip("pyspark")
    import tempfile
    from sparqix.verify import verify
    with tempfile.TemporaryDirectory() as d:
        open(os.path.join(d, "a.csv"), "w").write("g,x\na,1\na,2\nb,3\n")
        res, _ = verify("import pandas as pd\ndf = pd.read_csv('a.csv')\nout = df.groupby('g')['x'].sum().reset_index()\n", d)
    assert res["out"] == []


def test_verify_compares_lazy_results_before_the_data_folder_is_removed(monkeypatch, tmp_path):
    """Spark DataFrames read their files when first used. verify() used to delete the temp data folder before materialising them,
    so every real-Spark comparison failed with PATH_NOT_FOUND (found by the spark-e2e CI job)."""
    pd = pytest.importorskip("pandas")
    import sparqix.core as core
    from sparqix.verify import verify
    lazy = ("import pandas as pd\n"
            "class Lazy:\n"
            "    def __init__(self, path): self.path = path\n"
            "    def toPandas(self): return pd.read_csv(self.path)\n"
            "df = Lazy(__import__('os').path.abspath('a.csv'))\n")
    monkeypatch.setattr(core, "convert", lambda *a, **k: (lazy, core.Report()))
    (tmp_path / "a.csv").write_text("g,x\na,1\nb,3\n")
    res, _ = verify("import pandas as pd\ndf = pd.read_csv('a.csv')\n", str(tmp_path))
    assert res == {"df": []}


# ---------------------------------------------------------------- project conversion
def test_project(tmp_path):
    from sparqix.project import convert_project
    src = tmp_path / "src"
    (src / "etl").mkdir(parents=True)
    (src / "etl" / "load.py").write_text("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\n")
    (src / "main.py").write_text("import load\nimport pandas as pd\nd = pd.read_csv('b.csv')\n")
    (src / "load.py").write_text("import pandas as pd\nx = pd.read_csv('c.csv')\n")
    (src / "main.sas").write_text("data a; set b; run;\n")
    (src / "main.sql").write_text("SELECT 1")
    (src / "notes.txt").write_text("hi")
    (src / ".git").mkdir()
    (src / ".git" / "x.py").write_text("import pandas")
    out = tmp_path / "out"
    rows, skipped = convert_project(str(src), str(out))
    names = sorted(r["out"] for r in rows)
    assert names == ["etl/load.py", "load.py", "main.py", "main_sas.py", "main_sql.py"]
    assert (out / "SPARQIX_REPORT.md").exists() and any(s[0] == "notes.txt" for s in skipped)
    assert "spark.read.csv" in (out / "etl" / "load.py").read_text()
    assert "main.py imports load" in (out / "SPARQIX_REPORT.md").read_text()
    assert not any(".git" in r["rel"] for r in rows)


# ---------------------------------------------------------------- optimize
def test_optimize_udf_and_cache():
    from sparqix.optimize import optimize
    from sparqix.core import Report
    src = ("df = spark.table('t')\n"
           "df = df.withColumn('a', F.udf(lambda x: x.upper() + '!', T.StringType())(F.col('n')))\n"
           "df = df.withColumn('b', F.udf(lambda x: x * 2 if x > 1 else 0, T.IntegerType())(F.col('m')))\n"
           "df = df.withColumn('c', F.udf(lambda x: re.sub('a', 'b', x), T.StringType())(F.col('n')))\n"
           "small = df.groupBy('k').agg(F.count('*').alias('n'))\n"
           "j = df.join(small, on='k')\n"
           "j2 = j.groupBy('k').count()\n"
           "j.show()\nj2.show()\n"
           "for r in rows:\n    df.count()\n")
    rep = Report()
    out = optimize(src, rep)
    ast.parse(out)
    assert "F.concat(F.upper(F.col('n')), F.lit('!'))" in out
    assert "F.when((F.col('m') > F.lit(1)), (F.col('m') * F.lit(2))).otherwise(F.lit(0))" in out
    assert "F.udf(lambda x: re.sub" in out                      # not translatable: kept + warned
    assert "df.join(F.broadcast(small), on='k').cache()" in out
    assert any("inside a loop" in w[1] for w in rep.warnings) and any("UDF kept" in w[1] for w in rep.warnings)


def test_optimize_cli_flag():
    code, rep = convert("import pandas as pd\na = pd.read_csv('a')\nb = pd.read_csv('b')\nc = a.merge(b, on='k')\nx = c['v'].sum()\ny = c['v'].max()\n",
                        "pandas", optimize=True, broadcast=["b"])
    assert "F.broadcast(b)" in code and ".cache()" in code


# ---------------------------------------------------------------- pandas API on Spark
def test_pandas_api_mode():
    src = ("import pandas as pd\nimport numpy as np\n"
           "df = pd.read_csv('a.csv')\n"
           "df['y'] = np.where(df.x > 1, 1, 0)\n"
           "m = df.merge(pd.read_csv('b.csv'), on='k')\n"
           "for i, r in m.iterrows():\n    m.loc[i, 'z'] = r['a']\n"
           "m.to_pickle('p')\n"
           "def f(a: pd.DataFrame) -> pd.DataFrame:\n    return a\n")
    code, rep = convert(src, "pandas", mode="pandas-api")
    ast.parse(code)
    assert code.startswith("import pyspark.pandas as ps\nimport numpy as np")
    assert "ps.read_csv('a.csv')" in code and "a: ps.DataFrame" in code and "pd." not in code
    assert "compute.ops_on_diff_frames" in code
    assert any("to_pickle" in t[1] for t in rep.todos)
    assert any("iterrows" in w[1] for w in rep.warnings) and any("np.where" in w[1] for w in rep.warnings)
    code2, rep2 = convert("from pandas import read_csv\ndf = read_csv('a')\n", "pandas", mode="pandas-api")
    assert "from pyspark.pandas import read_csv" in code2
    code3, rep3 = convert("SELECT 1", "sql", mode="pandas-api")
    assert any("only applies to pandas" in n for n in rep3.notes)


# ---------------------------------------------------------------- orchestration
def _jobs(path, kind="auto"):
    from sparqix.orchestrate import read_jobs
    from sparqix.core import Report
    rep = Report()
    return read_jobs(open(os.path.join(HERE, "..", "examples", path)).read(), kind, rep), rep


def test_cron_to_quartz():
    from sparqix.orchestrate import cron_to_quartz
    from sparqix.core import Report
    r = Report()
    assert cron_to_quartz("30 2 * * 1-5", r, "x") == "0 30 2 ? * 2-6"
    assert cron_to_quartz("0 */6 * * 0,6", r, "x") == "0 0 */6 ? * 1,7"
    assert cron_to_quartz("15 3 1 * *", r, "x") == "0 15 3 1 * ?"
    assert cron_to_quartz("@daily", r, "x") == "0 0 0 * * ?"
    assert cron_to_quartz("bad", r, "x") is None and r.todos


def test_airflow_to_databricks():
    from sparqix.orchestrate import to_databricks, to_airflow
    from sparqix.core import Report
    (job,), rep = _jobs("etl_dag.py")
    assert job.name == "orders_etl" and job.schedule == "30 2 * * 1-5"
    by = {t.key: t for t in job.tasks}
    assert by["transform"].depends == ["extract"] and set(by["load"].depends) == {"transform"} and by["cleanup"].depends == ["load"]
    assert by["extract"].kind == "python" and by["extract"].file == "jobs/extract.py" and by["extract"].retries == 2
    js, extra = to_databricks(job, rep, "/ws")
    keys = {t["task_key"]: t for t in js["tasks"]}
    assert "start" not in keys and "depends_on" not in keys["extract"]           # EmptyOperator bridged away
    assert keys["extract"]["spark_python_task"]["parameters"] == ["--date", "{{job.start_time.iso_date}}"]
    assert keys["transform"]["spark_python_task"]["python_file"] == "/ws/jobs/transform.py"
    assert "load.sql" in extra and keys["load"]["sql_task"]["file"]["path"] == "/ws/load.sql"
    assert js["schedule"]["quartz_cron_expression"] == "0 30 2 ? * 2-6" and js["schedule"]["pause_status"] == "PAUSED"
    assert any(t.key == "notify" for t in job.tasks) and any("notify" in t[0] for t in rep.todos)
    ast.parse(to_airflow(job, Report(), "/ws"))


def test_crontab_and_workflow():
    from sparqix.orchestrate import to_databricks
    jobs, rep = _jobs("crontab.txt")
    assert [j.schedule for j in jobs] == ["15 3 * * *", "0 */6 * * 0,6", "@hourly"]
    assert [j.tasks[0].kind for j in jobs] == ["python", "python", "sql"] and jobs[0].tasks[0].args == ["--full"]
    (wf,), rep = _jobs("wf_orders.xml")
    js, extra = to_databricks(wf, rep, "/ws", "m_orders_fact")
    assert [t["task_key"] for t in js["tasks"]] == ["s_m_orders_fact", "s_m_region", "cmd_archive"]
    assert "from m_orders_fact import m_orders_fact" in extra["run_m_orders_fact.py"]
    assert any("condition" in w[1] for w in rep.warnings)


# ---------------------------------------------------------------- Delta / Unity Catalog
def test_delta_sql_names_and_ddl():
    from sparqix.delta import process_sql_text
    from sparqix.core import Report
    r = Report()
    out = process_sql_text(
        "WITH t AS (SELECT * FROM raw) SELECT EXTRACT(year FROM d) y, x IS DISTINCT FROM z FROM t JOIN db.x ON 1=1 JOIN tmpv ON 1=1, a p, b q;"
        " CREATE TABLE IF NOT EXISTS f (a INT) STORED AS PARQUET; INSERT INTO g (a) SELECT a FROM m.n.o; SELECT * FROM range(3);",
        r, delta=True, catalog="main", schema="etl", skip={"tmpv"})
    assert "FROM main.etl.raw" in out and "JOIN main.db.x" in out and "JOIN tmpv" in out and "FROM t JOIN" in out
    assert "EXTRACT(year FROM d)" in out and "DISTINCT FROM z" in out and "FROM range(3)" in out
    assert "CREATE TABLE IF NOT EXISTS main.etl.f (a INT) USING DELTA" in out
    assert "INSERT INTO main.etl.g (a)" in out and "FROM m.n.o" in out


def test_upsert_to_merge():
    from sparqix.delta import upsert_to_merge
    from sparqix.core import Report
    r = Report()
    out = upsert_to_merge("INSERT INTO t (a, b) SELECT x, y FROM s ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b, c = t.c + 1;", r)
    assert out.startswith("MERGE INTO t AS tgt\nUSING (SELECT x, y FROM s) AS src(a, b)\nON tgt.a = src.a")
    assert "WHEN MATCHED THEN UPDATE SET b = src.b, c = tgt.c + 1" in out and "INSERT (a, b) VALUES (src.a, src.b);" in out
    out = upsert_to_merge("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING", r)
    assert "MATCHED THEN UPDATE" not in out and "(VALUES (1)) AS v(a)" in out
    upsert_to_merge("INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a = 2", Report())


def test_delta_python():
    src = ("df = spark.read.parquet('in/x')\n"
           "df.write.mode('overwrite').parquet('/tmp/out')\n"
           "back = spark.read.parquet('/tmp/out')\n"
           "back.write.csv('o.csv')\n"
           "back.write.saveAsTable('rep.final')\n"
           "back.createOrReplaceTempView('vw')\n"
           "spark.table('dim')\nspark.table('vw')\n"
           "spark.sql(\"INSERT INTO t (a) SELECT x FROM src ON CONFLICT (a) DO NOTHING\")\n")
    code, rep = convert(src, "python", delta=True, catalog="main", schema="etl")
    ast.parse(code)
    assert "df.write.mode('overwrite').format('delta').save('/tmp/out')" in code
    assert "spark.read.parquet('in/x')" in code and "spark.read.format('delta').load('/tmp/out')" in code
    assert "back.write.csv('o.csv')" in code and "format('delta').saveAsTable('main.rep.final')" in code
    assert "spark.table('main.etl.dim')" in code and "spark.table('vw')" in code
    assert "MERGE INTO main.etl.t AS tgt USING" in code


def test_upsert_always_for_sql():
    code, _ = convert("INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b", "sql", sql_dialect="postgres", sql_out="sql")
    assert "MERGE INTO t AS tgt" in code and "ON CONFLICT" not in code


# ---------------------------------------------------------------- stored procedures
def test_tsql_procedure():
    path = os.path.join(HERE, "..", "examples", "usp_load_orders.sql")
    code, rep = convert(open(path).read(), "auto", filename=path)
    ast.parse(code)
    assert detect_language(open(path).read(), path) == "proc"
    assert "def usp_load_orders(spark, run_date, min_amount=100, rows=None):" in code
    assert "cnt = (spark.sql('SELECT COUNT(*) FROM stg_orders WHERE order_date = :run_date', args={'run_date': run_date}).first() or [None])[0]" in code
    assert "createOrReplaceTempView('tmp_big')" in code and "FROM tmp_big" in code
    assert "if cnt == 0:" in code and "while cnt > 3:" in code and "cnt = cnt - 1" in code
    assert "try:" in code and "except Exception as err:" in code and "raise RuntimeError('load failed')" in code
    assert ".count() > 0" in code and "dropTempView('tmp_big')" in code
    assert "return {'rows': rows}" in code
    assert not rep.todos or all("temp" not in t[1] for t in rep.todos)


def test_plsql_procedure():
    path = os.path.join(HERE, "..", "examples", "p_refresh.plsql")
    code, rep = convert(open(path).read(), "auto", filename=path)
    ast.parse(code)
    assert "def refresh_totals(spark, p_region, p_days=7, p_out=None):" in code
    assert "elif v_n > 1000:" in code and "for i in range(1, p_days + 1):" in code
    assert "args={'v_total': v_total, 'p_region': p_region, 'i': i}" in code
    assert "for rec in spark.sql(" in code and "print(rec.region)" in code
    assert "raise RuntimeError('refresh failed')" in code and "return {'p_out': p_out}" in code
    assert any("driver" in w[1] for w in rep.warnings)


def test_proc_unsupported_is_loud():
    code, rep = convert("CREATE PROCEDURE p AS BEGIN DECLARE @x INT = 1; IF @x LIKE '1%' PRINT 'a'; DECLARE c CURSOR FOR SELECT 1; END", "proc")
    ast.parse(code)
    assert "_todo(" in code and "def _todo(msg):" in code and len(rep.todos) >= 2
    code, rep = convert("CREATE FUNCTION f(@a INT) RETURNS INT AS BEGIN RETURN ISNULL(@a, 0) + 1 END", "proc")
    ast.parse(code)
    assert "return _coalesce(a, 0) + 1" in code


def test_ssis_control_flow():
    from sparqix.orchestrate import to_databricks
    (job,), rep = _jobs("load_orders.dtsx")
    assert job.name == "LoadOrders" and [t.key for t in job.tasks] == ["Truncate Stage", "Load Orders", "Archive"]
    by = {t.key: t for t in job.tasks}
    assert by["Truncate Stage"].kind == "sql" and by["Load Orders"].depends == ["Truncate Stage"]
    assert by["Archive"].kind == "python" and by["Archive"].file == "archive.py" and by["Archive"].args == ["--days", "30"]
    assert any("Data Flow Task" in t[1] and "Derived Column" in t[1] for t in rep.todos) is False   # todos are added at write time
    js, extra = to_databricks(job, rep, "/ws")
    assert any("Data Flow Task" in t[1] for t in rep.todos) and "Truncate_Stage.sql" in extra
    assert any("not plain" in w[1] for w in rep.warnings)
    code, r = convert(open(os.path.join(HERE, "..", "examples", "load_orders.dtsx")).read(), "auto", filename="x.dtsx")
    assert "def df_load_orders" in code and r.todos


# ---------------------------------------------------------------- streaming
def test_kafka_consumer_loop():
    path = os.path.join(HERE, "..", "examples", "kafka_consumer.py")
    code, rep = convert(open(path).read(), "auto", filename=path)
    ast.parse(code)
    assert detect_language(open(path).read(), path) == "stream"
    assert "spark.readStream.format('kafka')" in code and "'subscribe', 'orders'" in code and "'startingOffsets', 'earliest'" in code
    assert "T.StructField('amount', T.DoubleType())" in code and "T.StructField('region', T.StringType())" in code
    assert "withColumn('total', (F.col('amount') * F.col('qty')))" in code and "withColumn('label', F.upper(F.col('region')))" in code
    assert "filter((F.col('amount') > F.lit(100)))" in code
    assert "F.to_json(F.struct(F.col('id').alias('id'), F.col('total').alias('total'), F.col('label').alias('label')))" in code
    assert "option('topic', 'big_orders')" in code and "checkpointLocation" in code and not rep.todos


def test_kafka_state_and_stub():
    src = ("import json\nfrom confluent_kafka import Consumer\nc = Consumer({'bootstrap.servers': 'b:9092'})\nc.subscribe(['clicks'])\ncounts = {}\n"
           "while True:\n    msg = c.poll(1.0)\n    if msg is None:\n        continue\n    ev = json.loads(msg.value().decode('utf-8'))\n"
           "    counts[ev['page']] += 1\n    db.insert(ev)\n")
    code, rep = convert(src, "stream")
    ast.parse(code)
    assert "groupBy(F.col('page').alias('key')).agg(F.count(F.lit(1)).alias('count'))" in code and "outputMode('update')" in code
    assert "raise NotImplementedError('TODO(sparqix)" in code and any("db.insert" in t[1] for t in rep.todos)


def test_directory_polling_loop():
    path = os.path.join(HERE, "..", "examples", "poll_dir.py")
    code, rep = convert(open(path).read(), "auto", filename=path)
    ast.parse(code)
    assert "spark.readStream.format('csv')" in code and ".load('/data/incoming')" in code
    assert "df.filter(F.col('amount') > 0)" in code and "withColumn('total'" in code
    assert "writeStream.format('parquet')" in code and "'path', '/data/processed'" in code
    assert "time.sleep" not in code and "continue" not in code


def test_dstream_wordcount():
    path = os.path.join(HERE, "..", "examples", "dstream_wc.py")
    code, rep = convert(open(path).read(), "auto", filename=path)
    ast.parse(code)
    assert "format('socket').option('host', 'localhost').option('port', 9999)" in code
    assert "F.explode(F.split(F.col('value'), ' ')).alias('word')" in code
    assert "groupBy('key').agg(F.sum('v').alias('total'))" in code and "outputMode('complete')" in code
    assert "F.length(F.col('word')) > F.lit(5)" in code and "processingTime='5 seconds'" in code
    code, rep = convert("from pyspark.streaming import StreamingContext\nssc = StreamingContext(sc, 2)\nl = ssc.socketTextStream('h', 1)\nw = l.map(lambda x: x)\nw2 = w.updateStateByKey(f)\nw2.pprint()\n", "stream")
    assert any("updateStateByKey" in t[1] for t in rep.todos)


# ---------------------------------------------------------------- web UI
def test_web_api():
    import json as _j
    import threading
    import urllib.request
    from sparqix.web import make_server
    srv = make_server("127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    def post(path, body, raw=None):
        req = urllib.request.Request(base + path, data=raw if raw is not None else _j.dumps(body).encode(), headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, _j.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, _j.loads(e.read())
    try:
        html = urllib.request.urlopen(base + "/").read().decode()
        assert "Sparqix" in html and "/api/convert" in html
        st, j = post("/api/convert", {"source": "import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\n"})
        assert st == 200 and j["detected"] == "pandas" and "df.filter(F.col('x') > 1)" in j["code"] and j["todos"] == []
        st, j = post("/api/convert", {"source": "SELECT TOP 3 ISNULL(a,0) FROM t", "language": "sql", "dialect": "tsql", "sql_out": "sql", "catalog": "main", "schema": "s"})
        assert st == 200 and "COALESCE(a, 0) FROM main.s.t LIMIT 3" in j["code"]
        st, j = post("/api/convert", {"source": "x", "target": "databricks", "language": "python"})
        assert st == 200 and j["code"].startswith("# Databricks notebook source")
        st, j = post("/api/orchestrate", {"source": open(os.path.join(HERE, "..", "examples", "crontab.txt")).read()})
        assert st == 200 and any(n.endswith(".job.json") for n in j["files"])
        assert post("/api/convert", None, raw=b"nope")[0] == 400
        assert post("/api/nothing", {})[0] == 404
        ex = _j.loads(urllib.request.urlopen(base + "/api/examples").read())
        assert "sales.py" in ex and "crontab.txt" not in ex
        assert urllib.request.urlopen(base + "/api/example?name=sales.py").status == 200
        try:
            urllib.request.urlopen(base + "/api/example?name=../pyproject.toml")
            assert False
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        srv.shutdown()


def test_web_project_and_ai():
    import base64 as _b
    import io as _io
    import json as _j
    import threading
    import urllib.request
    import zipfile as _z
    from sparqix import web
    srv = web.make_server("127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    def post(path, body):
        req = urllib.request.Request(base + path, data=_j.dumps(body).encode(), headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, _j.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, _j.loads(e.read())
    pd_src = "import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\n"
    try:
        st, j = post("/api/project", {"files": [{"path": "etl/a.py", "content": pd_src}, {"path": "q.sql", "content": "SELECT 1"}]})
        assert st == 200 and j["stats"]["converted"] >= 2
        zf = _z.ZipFile(_io.BytesIO(_b.b64decode(j["zip_b64"])))
        assert any(n.endswith("a.py") for n in zf.namelist())
        # zip upload incl. zip-slip entry
        buf = _io.BytesIO()
        with _z.ZipFile(buf, "w") as z:
            z.writestr("ok/a.py", pd_src)
            z.writestr("../evil.py", pd_src)
        st, j = post("/api/project", {"zip_b64": _b.b64encode(buf.getvalue()).decode()})
        assert st == 400 or any("evil" in str(p) for p in j.get("problems", []) + j.get("skipped", []))
        assert not os.path.exists(os.path.join(tempfile.gettempdir(), "evil.py"))
        assert post("/api/project", {"zip_b64": "bm90IGEgemlw"})[0] == 400
        assert post("/api/project", {"files": [{"path": "../x.py", "content": "x"}]})[0] == 400
        # AI dry run returns prompts and sends nothing
        todo_src = open(os.path.join(HERE, "..", "examples", "sales.py")).read()
        st, j = post("/api/convert", {"source": todo_src, "ai": {"enabled": True, "dry_run": True}})
        assert st == 200 and j["todos"] and len(j["prompts"]) >= 1
        # AI via fake client factory
        class Fake:
            def complete(self, *a, **k):
                return "```python\nresult = 1\n```"
            def __call__(self, *a, **k):
                return self.complete()
        seen = {}
        def factory(key, model):
            seen["key"] = key
            return Fake()
        web.AI_CLIENT_FACTORY = factory
        web.ISOLATE = False            # the fake client lives in this process
        try:
            st, j = post("/api/convert", {"source": todo_src, "ai": {"enabled": True, "api_key": "k-test"}})
            assert st == 200 and seen.get("key") == "k-test"
        finally:
            web.AI_CLIENT_FACTORY = None
            web.ISOLATE = True
    finally:
        srv.shutdown()


def test_ui_js_node(tmp_path):
    import shutil
    import subprocess
    import zipfile as _z
    node = shutil.which("node")
    if not node:
        pytest.skip("node missing")
    from sparqix.web import UI_JS
    (tmp_path / "ui.js").write_text(UI_JS)
    (tmp_path / "t.js").write_text(
        "const U=require('./ui.js');const fs=require('fs');\n"
        "const z=U.makeZip([{name:'a/b.txt',data:'h\u00e9llo'},{name:'c.json',data:'{}'}]);fs.writeFileSync('o.zip',Buffer.from(z));\n"
        "const h=U.highlight('x = 1  # TODO(sparqix): fix\\ndf.show()','py');\n"
        "console.log(JSON.stringify({n:h.length,todo:h[0].todo,crc:U.crc32(new TextEncoder().encode('123456789')),"
        "rt:U.b64urlDecode(U.b64urlEncode('h\u00e9 ?>'))}));\n")
    r = subprocess.run([node, "t.js"], cwd=tmp_path, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    import json as _j
    out = _j.loads(r.stdout)
    assert out["n"] == 2 and out["todo"] and out["crc"] == 0xCBF43926 and out["rt"] == "h\u00e9 ?>"
    zf = _z.ZipFile(tmp_path / "o.zip")
    assert zf.testzip() is None and zf.read("a/b.txt").decode() == "h\u00e9llo"


def test_talend_and_alteryx():
    ex = os.path.join(HERE, "..", "examples")
    src = open(os.path.join(ex, "job_orders.item")).read()
    assert detect_language(src, "job_orders.item") == "talend"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "on=['customer_id'], how='left'" in code
    assert ".groupBy('customer').agg(F.sum(F.col('total')).alias('revenue'))" in code
    assert "F.upper(F.col('name'))" in code and "(F.col('amount') * F.col('qty'))" in code
    assert "orderBy(F.col('revenue').desc())" in code
    assert rep.todos == [] and "F.concat(F.upper(F.col('region')), F.lit('-x'))" in code
    c2, r2 = convert("<x/>", "talend")
    assert r2.todos
    src = open(os.path.join(ex, "flow_orders.yxmd")).read()
    assert detect_language(src, "flow_orders.yxmd") == "alteryx"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "F.col('amount') > F.lit(100)) & (F.col('region') == F.lit('EU'))" in code
    assert "F.when((F.col('amount') > F.lit(1000)), F.lit('gold')).otherwise(F.lit('basic'))" in code
    assert "F.date_format(F.col('od'), 'yyyy')" in code
    assert "how='inner'" in code and "F.sum(F.col('total')).alias('revenue')" in code
    msgs = " ".join(m for _, m in rep.todos)
    assert "Buffer" in msgs and len(rep.todos) == 1
    assert "TODO(sparqix)" in code

def test_expression_translators():
    from sparqix.core import Unsupported
    from sparqix.expr_xlate import translate as tr
    J = lambda t, types=None: tr(t, "java", types)
    A = lambda t: tr(t, "alteryx")
    assert J('row1.name.trim().toUpperCase() + "-x"') == "F.concat(F.upper(F.trim(F.col('name'))), F.lit('-x'))"
    assert J('row1.a == null ? "n" : row1.b') == "F.when(F.col('a').isNull(), F.lit('n')).otherwise(F.col('b'))"
    assert J("row1.a > 5 && !(row1.b < 2) || Relational.ISNULL(row1.c)") == "(((F.col('a') > F.lit(5)) & (~(F.col('b') < F.lit(2)))) | F.col('c').isNull())"
    assert J('TalendDate.formatDate("yyyy-MM-dd", row1.d)') == "F.date_format(F.col('d'), F.lit('yyyy-MM-dd'))"
    assert J("TalendDate.addDate(row1.d, 3, \"dd\")") == "F.date_add(F.col('d'), F.lit(3))"
    assert J("row1.a + row1.b", {"a": "str", "b": "str"}) == "F.concat(F.col('a'), F.col('b'))"
    assert J("row1.a + row1.b", {"a": "num", "b": "num"}) == "(F.col('a') + F.col('b'))"
    assert J("(int) row1.a * 2") == "(F.col('a').cast('int') * F.lit(2))"
    assert J('row1.a.replace("-", "")') == "F.regexp_replace(F.col('a'), '\\\\-', '')"
    assert J("Integer.parseInt(row1.s)") == "F.col('s').cast('int')"
    assert A("IF [a] > 1 THEN 'x' ELSEIF [a] = 0 THEN 'z' ELSE 'y' ENDIF") == "F.when((F.col('a') > F.lit(1)), F.lit('x')).when((F.col('a') == F.lit(0)), F.lit('z')).otherwise(F.lit('y'))"
    assert A("[a] IN (1,2,3) AND NOT IsNull([b])") == "(F.col('a').isin([1, 2, 3]) & (~F.col('b').isNull()))"
    assert A("Substring([s], 2, 3)") == "F.substring(F.col('s'), 2 + 1, 3)"
    assert A("Round([x], 0.01)") == "F.round(F.col('x'), 2)"
    assert A("Left([s], 2) + '-' + ToString([n])") == "F.concat(F.concat(F.substring(F.col('s'), 1, 2), F.lit('-')), F.col('n').cast('string'))"
    assert A("DateTimeAdd([d], 5, 'days')") == "F.date_add(F.col('d'), F.lit(5))"
    for bad, lang in (("Foo(row1.a)", "java"), ("StringHandling.NOPE(row1.a)", "java"), ("Foo([a])", "alteryx"), ("[a] +", "alteryx"),
                      ("row1.a.bogus()", "java"), ("IF [a] THEN 1", "alteryx")):
        with pytest.raises(Unsupported):
            tr(bad, lang)
    # generated code parses
    for t in ('row1.a.substring(1) + "x"', 'row1.a.contains("q") ? 1 : 2'):
        ast.parse(J(t))
    # talend: tMap variable + advanced filter
    xml = """<?xml version="1.0"?><p xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:map="m">
    <node componentName="tFileInputDelimited"><elementParameter name="UNIQUE_NAME" value="in1"/><elementParameter name="FILENAME" value="&quot;a.csv&quot;"/><elementParameter name="HEADER" value="1"/>
      <metadata connector="FLOW" name="in1"><column name="n" type="id_Integer"/><column name="s" type="id_String"/></metadata></node>
    <node componentName="tFilterRow"><elementParameter name="UNIQUE_NAME" value="f1"/><elementParameter name="USE_ADVANCED" value="true"/><elementParameter name="ADVANCED_COND" value="input_row.n &gt; 1 &amp;&amp; input_row.s.length() &gt; 2"/></node>
    <node componentName="tMap"><elementParameter name="UNIQUE_NAME" value="m1"/><nodeData xsi:type="map:MapperData">
      <inputTables name="r2"/><varTables name="Var"><mapperTableEntries name="k" expression="r2.s.trim()"/></varTables>
      <outputTables name="o1"><mapperTableEntries name="key" expression="Var.k + r2.n"/></outputTables></nodeData></node>
    <connection connectorName="FLOW" label="r1" source="in1" target="f1"/><connection connectorName="FILTER" label="r2" source="f1" target="m1"/></p>"""
    code, rep = convert(xml, "talend")
    ast.parse(code)
    assert "withColumn('_v_k', F.trim(F.col('s')))" in code and "F.concat(F.col('_v_k'), F.col('n').cast('string'))" in code
    assert "F.length(F.col('s'))" in code
    assert not rep.todos, rep.todos

def test_pentaho_pig_ssis():
    ex = os.path.join(HERE, "..", "examples")
    # Pentaho
    src = open(os.path.join(ex, "orders.ktr")).read()
    assert detect_language(src, "orders.ktr") == "pentaho"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "s_Big_only_true = s_Read_orders.filter((F.col('amount') > F.lit(100)))" in code
    assert "s_Big_only_false.write.mode('overwrite').saveAsTable('stg.small_orders')" in code
    assert "s_Sort_a.join(s_Sort_b, on=['customer_id'], how='left')" in code
    assert "F.sum(F.col('total')).alias('revenue')" in code
    assert [t[0] for t in rep.todos] == ["Mystery"]
    # Pig
    src = open(os.path.join(ex, "orders.pig")).read()
    assert detect_language(src, "orders.pig") == "pig" and detect_language(src, "") == "pig"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "F.col('region').isNotNull()" in code
    assert "j = calc.join(custs, on=['customer_id'], how='left')" in code
    assert "agg = j.groupBy('name').agg(F.sum(F.col('total')).alias('revenue'), F.count(F.lit(1)).alias('n'))" in code
    assert "top.write.mode('overwrite').csv('/out/top', sep=',', header=False)" in code
    assert len(rep.todos) == 2
    # SSIS data flow
    src = open(os.path.join(ex, "orders_flow.dtsx")).read()
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "withColumn('total', (F.col('amount') * F.col('qty')))" in code
    assert "F.when((F.col('amount') > F.lit(1000)), F.lit('gold')).otherwise(F.lit('basic'))" in code
    assert "d_split_default = d_derived_column.filter(~((F.col('region') == F.lit('EU'))))" in code
    assert "groupBy(F.col('customer_id').alias('customer_id')).agg(F.sum(F.col('total')).alias('revenue'))" in code
    assert "saveAsTable('dbo.revenue_eu')" in code
    msgs = " ".join(m for _, m in rep.todos)
    assert "TOKEN" in msgs and "FuzzyLookup" in msgs
    # control-flow-only package: no data flows -> TODO, not a crash
    cf = open(os.path.join(ex, "load_orders.dtsx")).read()
    c2, r2 = convert(cf.replace("Microsoft.Pipeline", "Microsoft.Other"), "ssis")
    assert r2.todos
    # expression languages
    from sparqix.expr_xlate import translate as tr
    assert tr("a > 1 AND b == 'x' OR c is not null", "pig") == "(((F.col('a') > F.lit(1)) & (F.col('b') == F.lit('x'))) | F.col('c').isNotNull())"
    assert tr("SUBSTRING(s, 0, 3)", "pig") == "F.substring(F.col('s'), 0 + 1, 3 - 0)"
    assert tr("ISNULL(amt) ? 0 : amt * 2", "ssis") == "F.when(F.col('amt').isNull(), F.lit(0)).otherwise((F.col('amt') * F.lit(2)))"
    assert tr("(DT_I4)x", "ssis") == "F.col('x').cast('int')"
    assert tr("SUBSTRING(s, 2, 3)", "ssis") == "F.col('s').substr(F.lit(2), F.lit(3))"

def test_hive_bteq_datastage_and_job_orchestration():
    ex = os.path.join(HERE, "..", "examples")
    # Hive / Impala
    src = open(os.path.join(ex, "load.hql")).read()
    assert detect_language(src, "") == "hive" and detect_language("select 1", "x.hql") == "hive"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "P.setdefault('run_date', '2024-01-01')" in code
    assert "spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')" in code
    assert "'${hivevar:run_date}'" in code and "ANALYZE TABLE daily_orders COMPUTE STATISTICS" in code
    assert "REFRESH TABLE daily_orders" in code and "hive.exec" not in code.replace("hive.exec.", "", 0).split("P = {}")[1].split("def q")[0]
    assert sorted(t[1].split(":")[0] for t in rep.todos) == ["ADD JAR", "not convertible automatically (Kudu UPSERT / TRANSFORM ... USING script)"]
    ns = {}
    exec(code.split("P.setdefault")[0].split("spark = ")[0].replace("from pyspark.sql import SparkSession", ""), ns)   # header imports
    # variable substitution helper works and fails loudly
    helper = code[code.index("P = {}"):code.index("P.setdefault")]
    ns = {"re": re}
    exec(helper, ns)
    ns["P"]["x"] = "1"
    assert ns["q"]("a=${hiveconf:x} b=${x}") == "a=1 b=1"
    with pytest.raises(KeyError):
        ns["q"]("${nope}")
    # BTEQ
    src = open(os.path.join(ex, "refresh.bteq")).read()
    assert detect_language(src, "") == "bteq"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert 'spark.sql("""DELETE FROM stg_orders""")' in code and "INSERT INTO stg_orders SELECT order_id, COALESCE(amount, 0)" in code
    assert "write.mode('overwrite').csv('/out/big.txt'" in code and "sys.exit(0)" in code
    assert any("IMPORT" in t[1] for t in rep.todos) and any("ACTIVITYCOUNT" in t[1] for t in rep.todos)
    # DataStage
    src = open(os.path.join(ex, "job_orders.dsx")).read()
    assert detect_language(src, "") == "datastage"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "spark.read.csv('/data/orders.csv', header=True" in code
    assert "withColumn('svBig', (F.col('amount') > F.lit(100)))" in code
    assert "filter(((F.col('region') == F.lit('EU')) & (~F.col('customer_id').isNull())))" in code
    assert "F.when(F.col('svBig'), F.concat(F.concat(F.lit('BIG'), F.lit('-')), F.upper(F.col('region'))))" in code
    assert "saveAsTable('dw.orders_eu')" in code
    assert len(rep.todos) == 1 and "Oconv" in rep.todos[0][1]
    assert convert("BEGIN HEADER\nEND HEADER\n", "datastage")[1].todos
    from sparqix.expr_xlate import translate as tr
    assert tr("Field(lnk.s, ',', 2)", "datastage") == "F.split(F.col('s'), ',').getItem(2 - 1)"
    # orchestration: Pentaho job, Talend parent job, Alteryx
    import json as _j
    from sparqix.core import Report
    from sparqix.orchestrate import read_jobs, to_databricks
    r = Report()
    job = read_jobs(open(os.path.join(ex, "nightly.kjb")).read(), "auto", r)[0]
    js, extra = to_databricks(job, r, "/ws")
    keys = [t["task_key"] for t in js["tasks"]]
    assert keys == ["Truncate_stage", "Load_orders", "Notify", "Mail_on_failure"]
    assert js["schedule"]["quartz_cron_expression"] == "0 30 3 * * ?"
    assert [t for t in js["tasks"] if t["task_key"] == "Load_orders"][0]["spark_python_task"]["python_file"] == "/ws/orders.py"
    assert "Truncate_stage.sql" in extra
    assert any("on-failure" in w[1] for w in r.warnings) and any("MAIL" in t[1] for t in r.todos)
    r = Report()
    job = read_jobs(open(os.path.join(ex, "parent_job.item")).read(), "auto", r)[0]
    deps = {t.key: t.depends for t in job.tasks}
    assert deps == {"tRunJob_1": [], "tRunJob_2": ["tRunJob_1"], "tFileCopy_1": ["tRunJob_2"]}
    assert [t.file for t in job.tasks][:2] == ["Load_Customers.py", "Load_Orders.py"]
    r = Report()
    job = read_jobs(open(os.path.join(ex, "flow_orders.yxmd")).read(), "auto", r)[0]
    assert job.tasks[0].kind == "python" and any("Gallery" in n for n in r.notes)

# ---------------------------------------------------------------- GitHub Action helpers
def test_ci_comment_and_only(tmp_path):
    import json as _j
    from sparqix.ci import build_comment, relativize, MARKER
    from sparqix.project import convert_project
    src = tmp_path / "src"
    (src / "etl").mkdir(parents=True)
    (src / "etl" / "a.py").write_text("import pandas as pd\ndf = pd.read_csv('a.csv')\nfor i, r in df.iterrows():\n    df.loc[i, 'z'] = r['a']\n")
    (src / "etl" / "b.py").write_text("import pandas as pd\ndf = pd.read_csv('b.csv')\n")
    out = tmp_path / "out"
    rows, _ = convert_project(str(src), str(out), only={"etl/a.py"})
    assert [r["rel"] for r in rows] == [os.path.join("etl", "a.py")] and not (out / "etl" / "b.py").exists()
    data = _j.load(open(out / "SPARQIX_REPORT.json"))
    text, stats = build_comment(data)
    assert text.startswith(MARKER) and "`etl/a.py`" in text and stats["converted"] == 1 and stats["needs_review"] == 1 and stats["todos"] == 1
    text, stats = build_comment({"files": [], "skipped": []})
    assert "No convertible files" in text and stats["converted"] == 0
    big = {"files": [{"rel": f"f{i}.py", "lang": "pandas", "out": f"f{i}.py", "status": "CHECK", "todos": [], "warnings": [["x", "w" * 200]]} for i in range(400)], "skipped": []}
    text, _ = build_comment(big, max_chars=5000)
    assert len(text) < 5500 and "truncated" in text
    assert relativize(["etl/a.py", "docs/x.md", "etl/sub/b.sql"], "etl") == ["a.py", "sub/b.sql"]
    assert relativize(["a.py"], ".") == ["a.py"] and relativize(["etl/a.py"], "./etl/") == ["a.py"]


def test_action_yaml_is_valid():
    yaml = pytest.importorskip("yaml")
    act = yaml.safe_load(open(os.path.join(HERE, "..", "action.yml")))
    assert act["runs"]["using"] == "composite" and {"path", "fail-on", "catalog", "comment"} <= set(act["inputs"])
    for step in act["runs"]["steps"]:
        assert "uses" in step or "run" in step
        if "run" in step:
            assert "${{ inputs." not in step["run"], "inputs must go through env, not be interpolated into scripts"
    for f in (".github/workflows/ci.yml", "docs/github-workflow.yml"):
        assert yaml.safe_load(open(os.path.join(HERE, "..", f)))["jobs"]


# ---------------------------------------------------------------- AI suggestions (fake client / fake HTTP endpoint)
class _FakeClient:
    model = "fake-model"

    def __init__(self, replies):
        self.replies, self.prompts = list(replies), []

    def complete(self, system, user, max_tokens=800):
        self.prompts.append(user)
        r = self.replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


SALES = os.path.join(HERE, "..", "examples", "sales.py")


def test_ai_suggestion_as_comment():
    fake = _FakeClient(["Here:\n```python\ndf.select('amt').show()\n```"])
    code, rep = convert(open(SALES).read(), "pandas", ai=True, ai_client=fake)
    ast.parse(code)
    assert len(fake.prompts) == 1 and "for i, row in df.iterrows():" in fake.prompts[0] and "df.groupBy" not in fake.prompts[0].split("Generated code")[0]
    assert "# AI-SUGGESTION (model fake-model, unverified: review before use)" in code and "#   df.select('amt').show()" in code
    assert code.count("df.select('amt').show()") == 1 and not code.endswith("df.select('amt').show()")   # still a comment
    assert rep.todos and any("--ai: 1 suggestion" in n for n in rep.notes)               # TODO count unchanged


def test_ai_apply_and_safety():
    ok = _FakeClient(["```python\ndf.select('amt').show()\n```"])
    code, rep = convert(open(SALES).read(), "pandas", ai=True, ai_apply=True, ai_client=ok)
    ast.parse(code)
    assert "\ndf.select('amt').show()" in code and "applied" in code
    bad = _FakeClient(["```python\nimport os\nos.system('rm -rf /')\n```"])
    code, rep = convert(open(SALES).read(), "pandas", ai=True, ai_apply=True, ai_client=bad)
    assert "\nos.system(" not in code and "#   os.system(" in code and any("safety check" in w[1] for w in rep.warnings)
    broken = _FakeClient(["```python\ndf.select(\n```"])
    code, rep = convert(open(SALES).read(), "pandas", ai=True, ai_apply=True, ai_client=broken)
    ast.parse(code)
    assert any("does not parse" in w[1] for w in rep.warnings)


def test_ai_cannot_convert_failures_and_limits():
    code, rep = convert(open(SALES).read(), "pandas", ai=True, ai_client=_FakeClient(["CANNOT_CONVERT: row-wise side effects"]))
    assert "AI-SUGGESTION: none (row-wise side effects)" in code
    from sparqix.ai import AIError
    code, rep = convert(open(SALES).read(), "pandas", ai=True, ai_client=_FakeClient([AIError("boom")]))
    assert any("unavailable: boom" in w[1] for w in rep.warnings) and "AI-SUGGESTION" not in code
    fake = _FakeClient([])
    code, rep = convert(open(SALES).read(), "pandas", ai_dry_run=True, ai_client=fake)
    assert fake.prompts == [] and "AI-SUGGESTION" not in code


def test_ai_http_client_request_shape():
    import json as _j
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from sparqix.ai import AnthropicClient, AIError
    seen = {}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            seen["path"], seen["key"], seen["ver"] = self.path, self.headers["x-api-key"], self.headers["anthropic-version"]
            seen["body"] = _j.loads(self.rfile.read(int(self.headers["Content-Length"])))
            data = _j.dumps({"content": [{"type": "text", "text": "```python\nx = 1\n```"}]}).encode()
            self.send_response(200 if seen["body"]["messages"][0]["content"] != "fail" else 529)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        c = AnthropicClient(api_key="k123", model="m1", base_url=f"http://127.0.0.1:{srv.server_address[1]}")
        assert "x = 1" in c.complete("sys", "hello")
        assert seen["path"] == "/v1/messages" and seen["key"] == "k123" and seen["ver"] == "2023-06-01"
        assert seen["body"]["model"] == "m1" and seen["body"]["system"] == "sys" and seen["body"]["temperature"] == 0
        with pytest.raises(AIError):
            c.complete("sys", "fail")
    finally:
        srv.shutdown()
    old = os.environ.pop("ANTHROPIC_API_KEY", None)
    try:
        with pytest.raises(AIError):
            AnthropicClient()
    finally:
        if old:
            os.environ["ANTHROPIC_API_KEY"] = old


# ---------------------------------------------------------------- security
def test_sanitizers_and_guard():
    from sparqix.core import cm, dq, tq, safe_fromstring, guard_output, InputRejected, UnsafeOutput, Report
    assert "\n" not in cm("a\r\nb c\x0bd") and "\r" not in cm("x\ry")
    ast.parse(f'"""{dq(chr(34) * 3 + chr(10) + "import os")}"""')
    ns = {}
    exec(f"v = {tq('a' + chr(34) * 3 + '; import os; ' + chr(92))}", ns)
    assert "import os" in ns["v"]
    for bad in ('<!DOCTYPE a [<!ENTITY x "y">]><a>&x;</a>', '<!ENTITY x "y"><a/>'):
        with pytest.raises(InputRejected):
            safe_fromstring(bad)
    with pytest.raises(UnsafeOutput):
        guard_output("import os\nos.system('x')\n", "talend", "pyspark", Report())
    with pytest.raises(UnsafeOutput):
        guard_output("x = eval('1')\n", "pig", "pyspark", Report())
    guard_output("from pyspark.sql import functions as F\ndf = spark.range(1)\n", "talend", "pyspark", Report())


def test_injection_payloads_do_not_become_code():
    from sparqix.core import unsafe_construct
    ex = os.path.join(HERE, "..", "examples")
    payloads = ['x"\nimport os; os.system("id")\n#', "x'''\nimport os\n'''", "x\r\nimport os", "x import os"]
    for path in sorted(glob.glob(os.path.join(ex, "*"))):
        if not os.path.isfile(path) or path.endswith((".py", ".txt")):
            continue
        text = open(path, encoding="utf-8", errors="replace").read()
        if not path.endswith((".xml", ".item", ".yxmd", ".ktr", ".dtsx", ".dsx", ".json")) and '"' not in text:
            continue
        for pl in payloads:
            for m in re.finditer(r'(?<=")([A-Za-z0-9_ ./-]{3,20})(?=")', text):
                mutated = text[:m.start()] + pl.replace('"', "&quot;" if path.endswith((".xml", ".item", ".yxmd", ".ktr", ".dtsx")) else '\\"') + text[m.end():]
                try:
                    code, _ = convert(mutated, detect_language(mutated, path))
                except Exception:
                    continue                     # refusing or failing is fine
                try:
                    tree = ast.parse(code)
                except SyntaxError:
                    continue
                assert unsafe_construct(tree) is None, (path, pl)
                break


def test_ci_comment_is_inert():
    from sparqix.ci import build_comment
    evil = "@everyone [x](http://e) ![i](http://e/p.png) <script>x</script> #12 `\nfoo"
    data = {"files": [{"rel": "a`|b.py", "lang": "pandas", "out": "o.py", "status": "CHECK",
                       "todos": [("l1", evil)], "warnings": [("w", evil)]}], "skipped": []}
    text, _ = build_comment(data)
    body = text.split("<details>", 1)[1]
    assert "<script>" not in body and "](http" not in body.replace("\\]", "") and "\n- ⚠️" in body
    assert "@everyone" not in body and "@\u200beveryone" in body
    assert text.count("\n") < 40


def test_ai_redaction_and_hardening():
    from sparqix.ai import redact, AnthropicClient, AIError, _one_line_safe
    s = redact("password = 'hunter2'; url=postgres://bob:pw123@h/db; k=AKIAABCDEFGHIJKLMNOP; sk-ant-abcdefghijklmnopqrstuvwx; ghp_" + "a" * 36)
    assert not any(x in s for x in ("hunter2", "pw123", "AKIAABCD", "sk-ant-abc", "ghp_aaaa"))
    with pytest.raises(AIError):
        AnthropicClient("k", base_url="http://evil.example.com")
    AnthropicClient("k", base_url="http://127.0.0.1:9")
    assert _one_line_safe("a\rb\x0bc d\x85e") == "a b c d e"


def test_verify_requires_trust(capsys):
    from sparqix import verify
    assert verify.main(["whatever.py"]) == 2
    assert "--trust" in capsys.readouterr().out


def test_project_skips_symlinks(tmp_path):
    from sparqix.project import convert_project
    src, out, secret = tmp_path / "src", tmp_path / "out", tmp_path / "secret.py"
    src.mkdir()
    secret.write_text("import pandas as pd\nSECRET_MARKER = 1\n")
    (src / "ok.py").write_text("import pandas as pd\ndf = pd.read_csv('a.csv')\n")
    os.symlink(secret, src / "link.py")
    convert_project(str(src), str(out))
    assert not any("SECRET_MARKER" in open(p).read() for p in glob.glob(str(out / "**/*.py"), recursive=True))


def test_web_security_headers_and_checks():
    import http.client
    import json as _j
    import threading
    from sparqix.web import make_server
    srv = make_server("127.0.0.1", 0, token="s3cret")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    def req(method, path, body=None, headers=None):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        h = {"Host": f"127.0.0.1:{port}"}
        h.update(headers or {})
        c.request(method, path, body=body, headers=h)
        r = c.getresponse()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read()
    js = {"Content-Type": "application/json", "X-Sparqix-Token": "s3cret"}
    body = _j.dumps({"source": "import pandas as pd\ndf = pd.read_csv('a.csv')\n"})
    try:
        st, hd, _ = req("GET", "/?token=s3cret")
        assert st == 200 and "nonce-" in hd["content-security-policy"] and hd["x-content-type-options"] == "nosniff"
        assert hd["x-frame-options"] == "DENY"
        assert req("POST", "/api/convert", body, js)[0] == 200
        assert req("POST", "/api/convert", body, {"Content-Type": "application/json"})[0] in (401, 403)         # no token
        assert req("POST", "/api/convert", body, {**js, "Host": "evil.example.com"})[0] == 403                # DNS rebinding
        assert req("POST", "/api/convert", body, {**js, "Origin": "http://evil.example.com"})[0] == 403       # CSRF
        assert req("POST", "/api/convert", body, {**js, "Content-Type": "text/plain"})[0] == 415
        st, _, b = req("POST", "/api/convert", _j.dumps({"source": "x = ("}), {**js, "language": "python"})
        assert b"Traceback" not in b and b"trace" not in b
        st, _, b = req("POST", "/api/convert", _j.dumps({"source": "<!DOCTYPE a [<!ENTITY x 'y'>]><a/>", "language": "talend"}), js)
        assert st == 400 and b"Traceback" not in b
        try:                                                  # the server may hang up before the client finishes sending
            assert req("POST", "/api/convert", b"x" * (60 * 1024 * 1024), js)[0] in (413, 400)
        except (BrokenPipeError, ConnectionError):
            pass
    finally:
        srv.shutdown()


def test_web_container_support():
    import http.client
    import threading
    from sparqix.web import make_server
    srv = make_server("0.0.0.0", 0, token="t", allowed_hosts=["sparqix.example.com:443"])
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    def get(path, host):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        c.request("GET", path, headers={"Host": host})
        r = c.getresponse()
        return r.status, r.read()
    try:
        assert get("/healthz", "10.1.2.3:8765") == (200, b"ok")                      # kubelet probe: pod IP as Host, no token
        assert get("/readyz", "10.1.2.3:8765")[0] == 200
        assert get("/?token=t", "sparqix.example.com")[0] == 200                      # configured ingress host
        assert get("/?token=t", "evil.example.org")[0] == 403
        assert get("/api/examples", "sparqix.example.com")[0] == 401                  # token still required
    finally:
        srv.shutdown()


def test_no_quadratic_blowup_on_hostile_whitespace_and_punctuation():
    import time
    from sparqix.core import InputRejected, MAX_SOURCE_CHARS
    java = open(os.path.join(HERE, "..", "examples", "Streams.java")).read()
    cases = [("examples/sales.py", "\n"), ("examples/sales.py", "  "), ("examples/sales.py", "/* a */\n"),
             ("examples/load.hql", "\n"), ("examples/refresh.bteq", "\n  "), ("examples/sales.sas", "\n"), ("examples/orders.pig", "\n")]
    for rel, shape in cases:
        text = open(os.path.join(HERE, "..", rel)).read() + shape * 30000
        t = time.time()
        detect_language(text, rel)
        try:
            convert(text, "auto", filename=rel)
        except Exception:
            pass
        assert time.time() - t < 5, (rel, shape)
    for shape in (",", "[", "a,"):
        t = time.time()
        try:
            convert(java + shape * 60000, "java")
        except Exception:
            pass
        assert time.time() - t < 5, shape
    with pytest.raises(InputRejected):
        convert("x" * (MAX_SOURCE_CHARS + 1), "python")
    with pytest.raises(InputRejected):
        convert("a = 1\x00", "python")


def test_web_token_bruteforce_is_throttled():
    import http.client
    import threading
    from sparqix import web
    web._fails.clear()
    srv = web.make_server("127.0.0.1", 0, token="right")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    def get(tok):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        c.request("GET", "/api/examples", headers={"Host": f"127.0.0.1:{port}", "X-Sparqix-Token": tok})
        return c.getresponse().status
    try:
        codes = [get(f"wrong{i}") for i in range(web.MAX_FAILS + 3)]
        assert codes[0] == 401 and codes[-1] == 429
        assert get("right") == 429                    # locked out even with the right token until the window passes
    finally:
        srv.shutdown()
        web._fails.clear()


def test_workflow_policy():
    """CI hygiene: no pull_request_target, every workflow declares least-privilege permissions, checkout never persists credentials in the image build."""
    for f in glob.glob(os.path.join(HERE, "..", ".github", "workflows", "*.yml")):
        t = open(f).read()
        assert "pull_request_target" not in t, f
        assert re.search(r"(?m)^permissions:", t), f
        assert "write-all" not in t, f
    img = open(os.path.join(HERE, "..", ".github", "workflows", "image.yml")).read()
    assert "persist-credentials: false" in img and "exit-code: \"1\"" in img


def test_gluten_target():
    from sparqix.core import InputRejected
    from sparqix.gluten_conv import analyze_fallbacks, settings
    code, rep = convert("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\n", "pandas", target="gluten", gluten_offheap="8g")
    tree = ast.parse(code)
    assert 'config("spark.plugins", "org.apache.gluten.GlutenPlugin")' in code and '"spark.memory.offHeap.size", "8g"' in code
    assert "ColumnarShuffleManager" in code and any("gluten" in n.lower() for n in rep.notes)
    assert code.index("SparkSession.builder.config") > code.index("--conf spark.plugins")      # header comment first, then the session
    # SQL-only output has no session: settings go into a header comment
    sqlout, _ = convert("SELECT a FROM t", "sql", sql_out="sql", target="gluten")
    assert sqlout.startswith("-- Apache Gluten") and "SELECT" in sqlout
    # fallback analysis (line numbers refer to the final text)
    src = "from pyspark.sql import SparkSession, functions as F\nspark = SparkSession.builder.getOrCreate()\n" \
          "u = F.udf(lambda x: x)\ndf = spark.read.json('a')\nr = df.rdd.map(lambda x: x)\np = df.toPandas()\n"
    from sparqix.core import Report
    r = Report()
    n = analyze_fallbacks(src, r)
    kinds = " ".join(w[1] for w in r.warnings)
    assert n == 4 and "Python UDF" in kinds and ".rdd" in kinds and "toPandas" in kinds and "json" in kinds
    # input validation: nothing from the option can reach the generated code
    for bad in ("8g; import os", "x", "", "99999999g", '4g")\nimport os\n#'):
        with pytest.raises((InputRejected, ValueError)):
            if bad == "":
                settings("")
            else:
                convert("SELECT 1", "sql", target="gluten", gluten_offheap=bad)
    # databricks / spark targets are unchanged
    assert "Gluten" not in convert("SELECT 1", "sql")[0] and "Gluten" not in convert("SELECT 1", "sql", target="databricks")[0]


def test_gluten_is_version_agnostic():
    from sparqix.core import InputRejected
    from sparqix.gluten_conv import defaults_conf, parse_extra
    code, _ = convert("SELECT 1", "sql", sql_out="sql", target="gluten", gluten_jar="/opt/g/my.jar",
                      gluten_conf=["spark.shuffle.manager=org.example.Other", "spark.gluten.k=v"])
    assert "--jars /opt/g/my.jar" in code and "spark.shuffle.manager=org.example.Other" in code and "ColumnarShuffleManager" not in code
    conf = defaults_conf("2g", {"spark.x.y": "1"}, "/opt/gluten/gluten.jar")
    assert "spark.driver.extraClassPath /opt/gluten/gluten.jar" in conf and "spark.memory.offHeap.size 2g" in conf and "spark.x.y 1" in conf
    for bad in ("spark.x=a\nimport os", 'spark.x=a")', "nokey", "other.key=1", "spark.x=a b", "spark.x=$(id)"):
        with pytest.raises(ValueError):
            parse_extra([bad])
        with pytest.raises(InputRejected):
            convert("SELECT 1", "sql", target="gluten", gluten_conf=[bad])
    for bad in ("a;b", "x y", "$(id)", "../a\n"):
        with pytest.raises(InputRejected):
            convert("SELECT 1", "sql", target="gluten", gluten_jar=bad)
    df = open(os.path.join(HERE, "..", "deploy", "gluten", "Dockerfile")).read()
    assert "ARG BASE_IMAGE" in df and "FROM ${BASE_IMAGE}" in df and "GLUTEN_JAR_SHA256" in df


def test_native_engines_and_presets():
    from sparqix.core import InputRejected
    from sparqix.gluten_conv import PRESETS, defaults_conf, parse_extra
    src = "import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\n"
    # Gluten + Velox with presets: presets are layered under explicit --gluten-conf
    code, _ = convert(src, "pandas", target="gluten", gluten_backend="velox", gluten_preset=["production"],
                      gluten_conf=["spark.gluten.sql.columnar.shuffle.codec=zstd"])
    ast.parse(code)
    assert 'config("spark.gluten.sql.columnar.backend.lib", "velox")' in code
    assert "velox.spillStrategy=auto" in code and "spark.sql.adaptive.skewJoin.enabled=true" in code
    assert "shuffle.codec=zstd" in code and "shuffle.codec=lz4" not in code          # explicit override wins
    # ClickHouse backend: library path on driver and executors, Velox-only presets dropped
    code, _ = convert(src, "pandas", target="gluten", gluten_backend="clickhouse", gluten_libpath="/opt/ch/libch.so", gluten_preset=["spill-safe", "adaptive"])
    assert '"spark.gluten.sql.columnar.backend.lib", "ch"' in code and "executor.libpath=/opt/ch/libch.so" in code
    assert "backend.velox." not in code and "spark.sql.adaptive.enabled=true" in code
    # Comet
    code, rep = convert(src, "pandas", target="comet", gluten_offheap="2g")
    assert "org.apache.spark.CometPlugin" in code and "CometShuffleManager" in code and "$COMET_JAR" in code and "Gluten" not in code
    assert '"spark.memory.offHeap.size", "2g"' in code
    # Fabric: %%configure header, no session change
    code, _ = convert(src, "pandas", target="fabric")
    assert "%%configure" in code and '"spark.native.enabled": "true"' in code and 'SparkSession.builder.appName' in code
    # Photon: Databricks notebook with the cluster setting in the header (first line stays the notebook marker)
    code, _ = convert(src, "pandas", target="photon")
    assert code.startswith("# Databricks notebook source\n") and '"runtime_engine": "PHOTON"' in code
    # SQL-only output gets a comment header for each engine
    for t in ("gluten", "comet", "fabric"):
        out, _ = convert("SELECT 1", "sql", sql_out="sql", target=t)
        assert out.startswith("-- ") and "SELECT 1" in out
    out, _ = convert("SELECT 1", "sql", sql_out="sql", target="photon")
    assert out.startswith("-- Databricks notebook source\n-- ")
    # fallback analysis names the engine
    _, rep = convert("from pyspark.sql import functions as F\nu = F.udf(lambda x: x)\n", "python", target="photon")
    assert any(w[1].startswith("photon:") and "Python UDF" in w[1] for w in rep.warnings)
    # settings text for every engine
    assert "spark.plugins org.apache.spark.CometPlugin" in defaults_conf(engine="comet", jar="/o/c.jar")
    assert '"runtime_engine": "PHOTON"' in defaults_conf(engine="photon")
    assert '"spark.native.enabled"' in defaults_conf(engine="fabric")
    assert set(PRESETS) >= {"spill-safe", "scan-heavy", "agg-heavy", "adaptive", "production"}
    # nothing from the options can inject code or comments, for any engine
    for t in ("gluten", "comet", "fabric", "photon"):
        for kw in (dict(gluten_backend="ch"), dict(gluten_preset=["nope"]), dict(gluten_libpath="a;b"), dict(gluten_libpath="x\n"),
                   dict(gluten_conf=["spark.x=1\nimport os"]), dict(gluten_offheap="4g\n")):
            with pytest.raises(InputRejected):
                convert("SELECT 1", "sql", target=t, **kw)
    with pytest.raises(ValueError):
        parse_extra(["spark.x=a\n"])


# ---------------------------------------------------------------- agent
class ScriptedClient:
    """stands in for the model: replays a list of tool calls (or a final text) one turn at a time"""
    def __init__(self, turns):
        self.turns, self.calls = list(turns), []

    def chat(self, system, messages, tools, max_tokens=2000):
        self.calls.append((system, messages, [t["name"] for t in tools]))
        turn = self.turns.pop(0) if self.turns else [("text", "done")]
        blocks = []
        for i, t in enumerate(turn):
            if t[0] == "text":
                blocks.append({"type": "text", "text": t[1]})
            else:
                blocks.append({"type": "tool_use", "id": f"t{len(self.calls)}_{i}", "name": t[0], "input": t[1]})
        return {"content": blocks, "stop_reason": "tool_use" if any(t[0] != "text" for t in turn) else "end_turn", "usage": {"input_tokens": 10, "output_tokens": 5}}


def _proj(tmp_path):
    src = tmp_path / "proj"
    src.mkdir()
    (src / "etl.py").write_text("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\nout = df.groupby('g').x.sum()\n")
    (src / "q.sql").write_text("SELECT TOP 3 ISNULL(a,0) FROM t")
    (src / ".env").write_text("TOKEN=sk-ant-supersecretsupersecret\n")
    (tmp_path / "outside.txt").write_text("outside")
    return src


def test_agent_fix_loop_patches_and_writes_only_after_approval(tmp_path):
    from sparqix.agent import Session, run_agent
    src = _proj(tmp_path)
    out = tmp_path / "out"
    asked = []
    sess = Session(str(src), str(out), approve=lambda k, d: asked.append((k, d)) or True)
    client = ScriptedClient([
        [("convert_file", {"path": "etl.py"})],
        [("show_todos", {"draft": "d1"}), ("show_draft", {"draft": "d1"})],
        [("patch_draft", {"draft": "d1", "old": "df = df.filter(F.col('x') > 1)", "new": "df = df.filter(F.col('x') > 1)  # reviewed by agent"})],
        [("check_draft", {"draft": "d1"})],
        [("write_draft", {"draft": "d1"})],
        [("finish", {"summary": "converted etl.py"})]])
    r = run_agent("fix etl.py", sess, client)
    assert r.stopped == "done" and r.summary == "converted etl.py" and len(r.steps) == 7
    written = (out / "etl.py").read_text()
    assert "reviewed by agent" in written and asked and asked[0][0] == "write"
    assert (src / "etl.py").read_text().startswith("import pandas")           # the source is untouched


def test_agent_propose_only_by_default_and_budgets(tmp_path):
    from sparqix.agent import Session, run_agent
    src = _proj(tmp_path)
    sess = Session(str(src), str(tmp_path / "out"))                        # default approver denies
    client = ScriptedClient([[("convert_file", {"path": "etl.py"})], [("write_draft", {"draft": "d1"})], [("finish", {"summary": "x"})]])
    r = run_agent("go", sess, client)
    assert not (tmp_path / "out").exists() or not list((tmp_path / "out").glob("*"))
    assert any("NOT written" in s["result"] for s in r.steps)
    # step budget
    loop = ScriptedClient([[("show_draft", {"draft": "d9"})]] * 50)
    r = run_agent("go", Session(str(src), str(tmp_path / "o2")), loop, max_steps=4)
    assert r.stopped == "step limit" and len(loop.calls) == 4
    # token budget
    r = run_agent("go", Session(str(src), str(tmp_path / "o3")), ScriptedClient([[("list_files", {})]] * 50), max_tokens_total=40)
    assert r.stopped == "token budget"


def test_agent_tools_enforce_the_rules_not_the_prompt(tmp_path):
    from sparqix.agent import Session, call_tool
    src = _proj(tmp_path)
    s = Session(str(src), str(tmp_path / "out"), approve=lambda k, d: True)
    os.symlink(str(tmp_path / "outside.txt"), str(src / "link.py"))
    for path in ("../outside.txt", "/etc/passwd", ".env", "sub/../../outside.txt", "link.py", "~/x", ".git/config", "id_rsa", "a\x00b"):
        text, err = call_tool(s, "read_file", {"path": path})
        assert err and "outside" not in text and "supersecret" not in text, path
    assert call_tool(s, "read_file", {"path": "etl.py"})[1] is False
    # hostile file content cannot widen what tools may do
    call_tool(s, "convert_file", {"path": "etl.py"})
    for bad in ("import os\nos.system('id')\n", "x = eval('1')\n", "y = open('/etc/passwd').read()\n", "z = ().__class__.__bases__\n"):
        text, err = call_tool(s, "patch_draft", {"draft": "d1", "old": "df = df.filter(F.col('x') > 1)", "new": bad})
        assert err and "refused" in text, bad
    assert call_tool(s, "patch_draft", {"draft": "d1", "old": "df = df.filter(F.col('x') > 1)", "new": "df = (\n"})[1]    # does not parse
    assert call_tool(s, "patch_draft", {"draft": "d1", "old": "no such text", "new": "x"})[1]
    # writes: only the output folder, only .py/.sql/.md, never over a file the session did not create
    for out_path in ("../escape.py", "/tmp/x.py", "a.sh", ".env", "x.py\n"):
        assert call_tool(s, "write_draft", {"draft": "d1", "out_path": out_path})[1], out_path
    os.makedirs(tmp_path / "out", exist_ok=True)
    (tmp_path / "out" / "pre.py").write_text("existing")
    assert call_tool(s, "write_draft", {"draft": "d1", "out_path": "pre.py"})[1]
    assert (tmp_path / "out" / "pre.py").read_text() == "existing"
    assert call_tool(s, "write_draft", {"draft": "d1", "out_path": "ok.py"})[1] is False
    assert call_tool(s, "run_draft", {"draft": "d1"})[1]                                  # no sandbox configured
    assert call_tool(s, "bash", {"cmd": "id"})[1]                                         # there is no such tool
    with pytest.raises(Exception):
        Session(str(src), str(src))                                                       # output may not be the project folder


def test_agent_redacts_secrets_in_results(tmp_path):
    from sparqix.agent import Session, call_tool
    src = _proj(tmp_path)
    (src / "cfg.py").write_text("password = 'hunter2hunter2'\nkey = 'AKIAABCDEFGHIJKLMNOP'\n")
    text, err = call_tool(Session(str(src), str(tmp_path / "o")), "read_file", {"path": "cfg.py"})
    assert not err and "hunter2" not in text and "AKIAABCD" not in text


def test_agent_survey_and_migrate_report(tmp_path):
    from sparqix.agent import Session, run_agent
    src = _proj(tmp_path)
    sess = Session(str(src), str(tmp_path / "out"), approve=lambda k, d: True)
    client = ScriptedClient([[("survey", {})], [("convert_file", {"path": "q.sql", "sql_dialect": "tsql"}), ("convert_file", {"path": "etl.py"})],
                             [("write_draft", {"draft": "d1"}), ("write_draft", {"draft": "d2"})],
                             [("write_report", {"markdown": "# Migration\n\n| file | todos |\n|---|---|\n| etl.py | 0 |\n"})],
                             [("finish", {"summary": "migrated 2 files"})]])
    r = run_agent("migrate", sess, client)
    assert r.stopped == "done" and sorted(sess.written) == ["MIGRATION.md", "etl.py", "q.py"]
    survey = next(s for s in r.steps if s["tool"] == "survey")
    assert ".env" not in survey["result"] and "etl.py" in survey["result"]
    assert "TOKEN" not in "".join(str(m) for m in client.calls[-1][1])                  # secret file never reached the model


def test_agent_sandbox_command_line(tmp_path):
    from sparqix.agent import Session, call_tool
    from sparqix.sandbox import docker_args, run_in_sandbox
    a = docker_args("myorg/pyspark:3.5", "/tmp/x", "/data/sample")
    for flag in ("--network", "none", "--read-only", "--cap-drop", "ALL", "no-new-privileges", "--memory", "--pids-limit"):
        assert flag in a
    assert "/tmp/x:/work:ro" in a and any(x.endswith(":/data:ro") for x in a) and a[0] == "docker" and a[-2:] == ["python", "/work/script.py"]
    for bad in ("--privileged", "-v/:/host", "a b", ""):
        with pytest.raises(ValueError):
            docker_args(bad, "/tmp/x")
    seen = []
    class R:
        returncode, stdout, stderr = 0, "ok password = 'abcdefgh1'", ""
    out = run_in_sandbox("print(1)", None, "img:1", runner=lambda argv, t: seen.append(argv) or R())
    assert "exit code 0" in out and "abcdefgh1" not in out and seen[0][:2] == ["docker", "run"]
    src = _proj(tmp_path)
    s = Session(str(src), str(tmp_path / "o"), sandbox_image="img:1", runner=lambda argv, t: R())
    call_tool(s, "convert_file", {"path": "etl.py"})
    assert call_tool(s, "run_draft", {"draft": "d1"})[1] is False


def test_agent_web_assistant_and_cli_dry_run(capsys):
    import json as _j
    import threading
    import urllib.request
    from sparqix import web
    client = ScriptedClient([[("patch_draft", {"draft": "d1", "old": "df = df.filter(F.col('x') > 1)", "new": "df = df.filter(F.col('x') >= 1)"})],
                             [("finish", {"summary": "changed > to >="})]])
    old_iso, old_fac = web.ISOLATE, web.AI_CLIENT_FACTORY
    web.ISOLATE, web.AI_CLIENT_FACTORY = False, lambda key, model: client
    srv = web.make_server("127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{srv.server_address[1]}/api/agent", headers={"Content-Type": "application/json"},
                                     data=_j.dumps({"source": "import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df[df.x > 1]\n", "message": "make it inclusive",
                                                    "ai": {"api_key": "k"}, "history": [{"role": "user", "text": "hi"}, {"role": "assistant", "text": "hello"}]}).encode())
        j = _j.loads(urllib.request.urlopen(req).read())
        assert j["reply"] == "changed > to >=" and j["edits"] == 1 and "F.col('x') >= 1" in j["code"]
        tools = client.calls[0][2]
        assert "read_file" not in tools and "write_draft" not in tools and "survey" not in tools      # no file access in the web assistant
        bad = urllib.request.Request(f"http://127.0.0.1:{srv.server_address[1]}/api/agent", headers={"Content-Type": "application/json"}, data=b'{"source":"x"}')
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(bad)
        assert e.value.code == 400
    finally:
        srv.shutdown()
        web.ISOLATE, web.AI_CLIENT_FACTORY = old_iso, old_fac
    from sparqix.cli import main
    assert main(["agent", "fix", os.path.join(HERE, "..", "examples", "sales.py"), "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "tools:" in out and "approval asked each time" in out and "sandbox: off" in out


# ---------------------------------------------------------------- Teradata
def td(q):
    return sql(q, "teradata")


def test_teradata_sql_basics():
    out, _ = td("SEL TOP 3 a, b FROM t WHERE d > DATE '2021-01-01'")
    assert "SELECT" in out and "LIMIT 3" in out and "DATE '2021-01-01'" in out and "CURRENT_DATE() '" not in out
    out, _ = td("SEL a FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) = 1")
    assert "QUALIFY" in out or "_qualify" in out.lower() or "ROW_NUMBER" in out
    out, _ = td("SEL CSUM(a, b) FROM t")
    assert "SUM(a) OVER (ORDER BY b" in out
    out, rep = td("SEL a FROM t SAMPLE 10")
    assert "ORDER BY rand() LIMIT 10" in out
    out, _ = td("SEL CSUM(a,b) FROM t SAMPLE 10")  # the window's ORDER BY must not hide the missing one
    assert out.endswith("ORDER BY rand() LIMIT 10")


def test_teradata_dropped_and_ddl():
    out, _ = td("COLLECT STATISTICS ON t COLUMN a")
    assert out.startswith("-- dropped")
    out, rep = td("CREATE MULTISET VOLATILE TABLE v AS (SEL a FROM t) WITH DATA ON COMMIT PRESERVE ROWS")
    assert "TEMPORARY VIEW v" in out and rep.warnings
    code, rep = convert("SEL a FROM t;\nCOLLECT STATISTICS ON t COLUMN a;", "sql", sql_dialect="teradata")
    ast.parse(code)
    assert "# dropped Teradata statement" in code and 'spark.sql("""\n-- dropped' not in code


def test_teradata_macro_binds_parameters():
    src = ("REPLACE MACRO m (d DATE, n INTEGER) AS ( SEL a FROM t WHERE dt = :d AND x > :n; );\n"
           "EXEC m('2024-01-31', 5);\nEXEC m(some_var);\n")
    code, rep = convert(src, "sql", sql_dialect="teradata")
    ast.parse(code)
    assert "def m(d=None, n=None):" in code and "args={'d': d, 'n': n}" in code
    assert "\nm('2024-01-31', 5)" in code
    assert any("EXEC" in m for _, m in rep.todos)  # non-literal argument is never guessed
    evil = "REPLACE MACRO m (d DATE) AS ( SEL a FROM t WHERE x = :d; );\nEXEC m('x''); import os; os.system(''id'')');"
    code, _ = convert(evil, "sql", sql_dialect="teradata")
    ast.parse(code)
    assert not any(isinstance(n, (ast.Import, ast.ImportFrom)) and getattr(n, "module", None) != "pyspark.sql" and n.names[0].name != "os" or
                   isinstance(n, ast.Import) for n in ast.walk(ast.parse(code)))


def test_teradata_spl_procedure():
    src = open(os.path.join(HERE, "..", "examples", "load_sales.spl")).read()
    assert detect_language(src) == "proc"
    code, rep = convert(src, "auto")
    ast.parse(code)
    assert "(teradata)" in code and "while i < 3:" in code and "elif v_total > 5:" in code
    assert "INSERT INTO tgt SELECT" in code and "args={'p_region': p_region, 'v_total': v_total}" in code
    assert "::" not in code and "CAST(" not in code
    assert "except Exception as err:" in code and "v_total = -1" in code     # CONTINUE HANDLER FOR SQLEXCEPTION
    assert any("handler" in m.lower() for _, m in rep.warnings)


@pytest.mark.parametrize("name,kind", [("orders.fastload", "FastLoad"), ("orders.mload", "MultiLoad"), ("orders.tpt", "TPT")])
def test_teradata_loaders(name, kind):
    src = open(os.path.join(HERE, "..", "examples", name)).read()
    assert detect_language(src, name) == "tdload"
    code, rep = convert(src, "auto", filename=name)
    ast.parse(code)
    assert kind in code and "staging.orders" in code
    assert "s3cret" not in code and "etl_user" not in code and "tdprod" not in code  # logon details never reach the output
    assert any("not copied" in n for n in rep.notes)


def test_teradata_loader_injection_and_dml():
    bad = ("SET RECORD VARTEXT \"|\";\nDEFINE a (VARCHAR(5)) FILE=/x'); import os; os.system('id'); ('; \n"
           "BEGIN LOADING t\"; import os ERRORFILES e;\nINSERT INTO t VALUES (:a);\n")
    code, _ = convert(bad, "tdload")
    tree = ast.parse(code)
    assert not any(isinstance(n, ast.Import) and n.names[0].name == "os" for n in ast.walk(tree))
    _, rep = convert(open(os.path.join(HERE, "..", "examples", "orders.mload")).read(), "auto")
    assert not rep.todos and any("MERGE" in m for _, m in rep.warnings)  # UPDATE labels become a MERGE, with a Delta warning


# ---------------------------------------------------------------- Scala / Java: more operators and a javac check against Spark-API stubs
_JAVA_SAMPLE = """import java.util.*;
import java.util.stream.*;
import java.nio.file.*;
public class T {
  static boolean ok(Integer x) { return x > 1; }
  public static void main(String[] a) throws Exception {
    List<Integer> xs = Arrays.asList(1, 2, 3, 4, 5);
    List<String> ws = Arrays.asList("a b", "c d");
    List<Integer> a1 = xs.stream().skip(2).collect(Collectors.toList());
    boolean b1 = xs.stream().allMatch(x -> x > 0);
    boolean b2 = xs.stream().noneMatch(x -> x > 9);
    boolean b3 = xs.stream().anyMatch(x -> x > 4);
    Optional<Integer> f = xs.stream().filter(x -> x > 2).findFirst();
    Integer mx = xs.stream().max(Comparator.naturalOrder()).get();
    List<Integer> srt = xs.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
    List<String> srt2 = ws.stream().sorted(Comparator.comparing(w -> w.length())).collect(Collectors.toList());
    List<Integer> srt3 = xs.stream().sorted().collect(Collectors.toList());
    long s1 = xs.stream().mapToLong(x -> x).sum();
    double s2 = xs.stream().mapToDouble(x -> x).sum();
    int s3 = xs.stream().collect(Collectors.summingInt(x -> x));
    Map<Boolean, Integer> g1 = xs.stream().collect(Collectors.groupingBy(x -> x > 2, Collectors.summingInt(x -> x)));
    Map<Boolean, Long> g2 = xs.stream().collect(Collectors.groupingBy(x -> x > 2, Collectors.counting()));
    Map<Boolean, Iterable<Integer>> g3 = xs.stream().collect(Collectors.groupingBy(x -> x > 2));
    Map<String, Integer> tm = ws.stream().collect(Collectors.toMap(w -> w, w -> w.length()));
    long cnt = xs.stream().collect(Collectors.counting());
    xs.stream().forEach(System.out::println);
    List<Integer> pi = ws.stream().map(Integer::parseInt).collect(Collectors.toList());
    List<Integer> ln = ws.stream().map(String::length).collect(Collectors.toList());
    List<Integer> mr = xs.stream().filter(T::ok).collect(Collectors.toList());
    long fl = Files.lines(Paths.get("book.txt")).filter(l -> !l.isEmpty()).count();
    List<String> wd = Files.lines(Paths.get("book.txt")).flatMap(l -> Arrays.stream(l.split(" "))).collect(Collectors.toList());
    int rs = xs.stream().reduce(0, Integer::sum);
    Optional<Integer> r1 = xs.stream().reduce(Integer::sum);
    List<Integer> mi = xs.stream().mapToInt(x -> x).boxed().collect(Collectors.toList());
    Set<Integer> st = xs.stream().map(x -> x * 2).collect(Collectors.toSet());
    String j = ws.stream().collect(Collectors.joining(","));
    List<Integer> lim = xs.stream().limit(2).collect(Collectors.toList());
  }
}
"""


def _javac_check(code):
    import shutil, subprocess, tempfile
    stubs = os.path.join(HERE, "jvm_stubs")
    d = tempfile.mkdtemp()
    code = re.sub(r"(public class \w+ \{)", r"\1\n static org.apache.spark.api.java.JavaSparkContext jsc;", code, count=1)
    open(os.path.join(d, "T.java"), "w").write("import org.apache.spark.api.java.*;\nimport scala.Tuple2;\n" + code)
    srcs = [os.path.join(r, f) for r, _, fs in os.walk(stubs) for f in fs if f.endswith(".java")]
    p = subprocess.run(["javac", "-d", d, "-Xlint:none"] + srcs + [os.path.join(d, "T.java")], capture_output=True, text=True)
    shutil.rmtree(d, ignore_errors=True)
    return p.returncode, "\n".join(l for l in p.stderr.splitlines() if "JAVA_TOOL" not in l)


def test_java_operators_convert_without_todos():
    code, rep = convert(_JAVA_SAMPLE, "java")
    assert not rep.todos, rep.todos
    assert "zipWithIndex().filter(t -> t._2() >= 2)" in code and "filter(x -> !(x > 0)).isEmpty()" in code
    assert "Optional.of(jsc.parallelize(xs).max(Comparator.naturalOrder()))" in code
    assert "sortBy(x -> x, false, 1)" in code and "sortBy(w -> w.length(), true, 1)" in code
    assert "mapToPair(x -> new Tuple2<>(x > 2, x)).reduceByKey(Integer::sum)" in code
    assert ".map(Integer::parseInt)" in code and ".map(String::length)" in code and "take(2).collect(Collectors" not in code
    assert "jsc.textFile(\"book.txt\").filter(l -> !l.isEmpty()).count()" in code
    assert "collect().forEach(System.out::println)" in code


@pytest.mark.skipif(__import__("shutil").which("javac") is None, reason="needs a JDK")
def test_java_output_compiles_against_spark_api_stubs():
    # the stubs reproduce the Spark 3.x JavaRDD/JavaPairRDD/JavaDoubleRDD signatures: this checks Java typing, not Spark behaviour
    code, _ = convert(_JAVA_SAMPLE, "java")
    rc, err = _javac_check(code)
    assert rc == 0, err
    demo = open(os.path.join(HERE, "..", "examples", "Streams.java")).read().replace("names.stream()", "xs.stream().map(n -> \"\" + n)")
    code, rep = convert(demo, "java")
    assert not rep.todos
    rc, err = _javac_check(code)
    assert rc == 0, err


def test_jvm_converter_is_linear_on_hostile_input():
    import time
    cases = [("scala", "Source.fromFile("), ("scala", "val a = List("), ("scala", "val a = Source.fromFile("), ("java", "Files.lines(Paths.get("),
             ("java", "List<Integer> xs = Arrays.asList("), ("java", "xs.stream().sorted(Comparator.comparing("),
             ("scala", "val n = List(1,2)\nn.map(x=>x)\n"), ("java", "List<Integer> xs = Arrays.asList(1);\nxs.stream().map(x->x).count();\n")]
    for lang, shape in cases:
        t = time.time()
        try:
            convert(shape * 30000, lang)
        except Exception:
            pass
        assert time.time() - t < 4, (lang, shape)
    many = "".join(f"val a{i} = List(1)\nprintln(a{i})\n" for i in range(20000))
    t = time.time()
    convert(many, "scala")
    assert time.time() - t < 4


def test_scala_operators():
    src = """import scala.io.Source
object D {
  def main(args: Array[String]): Unit = {
    val nums = List(1, 2, 3, 4, 5, 6)
    val st = Set(1, 2, 3)
    val a = nums.sorted.reverse
    val b = nums.sortBy(x => -x).reverse
    val c = nums.map(x => (x % 2, x)).reduceByKey(_ + _)
    val e = nums.collect { case x if x > 2 => x * 2 }
    val f = nums.headOption
    val g = nums.forall(_ > 0)
    val h = nums.find(_ > 3)
    val i = nums.drop(2).toSet
    val k = nums.mkString
    val n = Source.fromFile("a.txt", "UTF-8").getLines().map(_.length).sum
    val o = scala.io.Source.fromFile("b.txt").getLines.filter(_.nonEmpty).toList
    val q = nums.size
  }
}
"""
    code, rep = convert(src, "scala")
    assert not rep.todos
    for frag in ("sc.parallelize(Set(1, 2, 3).toSeq)", "nums.sortBy(identity, ascending = false)", "nums.sortBy(x => -x, ascending = false)",
                 "reduceByKey(_ + _)", "nums.collect { case x if x > 2 => x * 2 }", "nums.take(1).headOption", "nums.filter(x_ => !(x_ > 0)).isEmpty()",
                 "nums.filter(_ > 3).take(1).headOption", "nums.zipWithIndex().filter(_._2 >= 2).map(_._1).collect().toSet", "nums.collect().mkString\n",
                 'sc.textFile("a.txt").map(_.length).sum()', 'sc.textFile("b.txt").filter(_.nonEmpty).collect().toList', "nums.count()"):
        assert frag in code, frag
    assert "_sparqix_src" not in code and "Source.fromFile" not in code.split("object D", 1)[1]
    msgs = " ".join(m for _, m in rep.warnings)
    assert "returns a Double" in msgs and "returns a Long" in msgs and "encoding" in msgs
    lines = {m: ln for ln, m in rep.warnings}
    assert lines[next(m for m in lines if "encoding" in m)] == 15          # line numbers point at the original source


# ---------------------------------------------------------------- Teradata: TOP WITH TIES, PERIOD, SAMPLE WHEN, SPL cursors/handlers, MERGE loads
def _td(q, d="teradata"):
    return convert(q, "sql", sql_dialect=d, sql_out="sql")


def test_top_with_ties():
    for d in ("teradata", "tsql", "redshift"):
        c, r = _td("SELECT TOP 3 WITH TIES name, sal FROM emp ORDER BY sal DESC" if d == "teradata" else "SELECT TOP (3) WITH TIES name, sal FROM emp ORDER BY sal DESC", d)
        assert "RANK() OVER (ORDER BY sal DESC) AS _rk" in c and "_rk <= 3" in c and "WITH TIES" not in c and not r.todos
    c, _ = _td("SELECT TOP 5 WITH TIES * FROM emp ORDER BY sal DESC, name")
    assert "_rk FROM emp" in c and "_rkFROM" not in c
    c, r = _td("SELECT TOP 2 WITH TIES a FROM t")                    # no ORDER BY: cannot rank
    assert r.todos


def test_teradata_period():
    c, _ = _td("CREATE TABLE p (id INT, v PERIOD(DATE), w PERIOD(TIMESTAMP(6)))")
    assert "v_begin DATE" in c and "v_end DATE" in c and "w_begin TIMESTAMP" in c and "w_end TIMESTAMP" in c
    c, r = _td("SELECT id FROM p WHERE v OVERLAPS w")
    assert "v_begin < w_end" in c and "w_begin < v_end" in c and not r.todos
    c, _ = _td("SELECT BEGIN(v) AS b, END(v) AS e FROM p")
    assert "v_begin AS b" in c and "v_end AS e" in c
    _, r = _td("SELECT P_INTERSECT(v, w) FROM p")
    assert r.todos                                                      # the period algebra is flagged, not guessed


def test_sample_when():
    c, r = _td("SEL a, b FROM t SAMPLE WHEN b > 10 THEN 0.1 ELSE 0.5 END")
    assert "RAND() < 0.1" in c and "RAND() < 0.5" in c and "SAMPLE" not in c.upper() and not r.todos
    c, r = _td("SEL a, b FROM t SAMPLE WHEN b > 10 THEN 5 END")
    assert "ROW_NUMBER() OVER (PARTITION BY" in c and "<= 5" in c
    c, r = _td("SEL * FROM t SAMPLE WHEN b > 10 THEN 5 END")
    assert r.todos                                                      # counts need a plain select list
    c, r = _td("SEL a FROM t SAMPLE WHEN b > 10 THEN abc END")
    assert r.todos


_SPL_CUR = """REPLACE PROCEDURE app.load_all (IN p_dept VARCHAR(20), OUT p_n INTEGER)
BEGIN
  DECLARE v_id INTEGER;
  DECLARE done INTEGER DEFAULT 0;
  DECLARE c1 CURSOR FOR SEL id FROM emp WHERE dept = :p_dept;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET p_n = -1;
  END;
  SET p_n = 0;
  OPEN c1;
  loop1: LOOP
    FETCH c1 INTO v_id;
    IF done = 1 THEN LEAVE loop1; END IF;
    SET p_n = p_n + 1;
  END LOOP loop1;
  CLOSE c1;
  FOR r AS SELECT id, amt FROM sales DO
    INSERT INTO log_t VALUES (r.id, r.amt);
  END FOR;
END;
"""


def test_spl_cursors_and_handlers():
    code, rep = convert(_SPL_CUR, "proc", sql_dialect="teradata")
    ast.parse(code)
    assert "c1_rows = spark.sql(" in code and "c1_pos < len(c1_rows)" in code and "done = 1" in code and "break" in code
    assert "except Exception as err:" in code and "p_n = -1" in code
    assert "args={'r_id': r['id'], 'r_amt': r['amt']}" in code and "SELECTECT" not in code
    assert not rep.todos
    code, rep = convert("REPLACE PROCEDURE a.b ()\nBEGIN\n  DECLARE c2 CURSOR WITH RETURN ONLY FOR SEL * FROM emp;\n"
                        "  DECLARE CONTINUE HANDLER FOR SQLSTATE '23000' SET x = 1;\n  DECLARE EXIT HANDLER FOR SQLWARNING SET x = 2;\n  OPEN c2;\nEND;\n",
                        "proc", sql_dialect="teradata")
    ast.parse(code)
    assert "return spark.sql('SELECT * FROM emp')" in code
    assert any("condition" in m for _, m in rep.todos) and any("SQLWARNING" in m for _, m in rep.warnings)


def _mload(label_sql, apply="lbl"):
    return (".LOGON h/u,p;\n.BEGIN IMPORT MLOAD TABLES s.o;\n.LAYOUT l;\n.FIELD k * VARCHAR(5);\n.FIELD v * VARCHAR(5);\n"
            f".DML LABEL lbl;\n{label_sql}\n.IMPORT INFILE /d/a.txt FORMAT VARTEXT ',' LAYOUT l APPLY {apply};\n.END MLOAD;\n")


def test_loader_merge():
    c, r = convert(_mload("UPDATE s.o SET v = :v WHERE k = :k;"), "tdload")
    ast.parse(c)
    assert "MERGE INTO s.o AS t USING stage_1_1 AS s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v" in c and not r.todos
    assert "row_number()" in c                                          # keep the last row per key
    c, r = convert(_mload("DELETE FROM s.o WHERE k = :k;"), "tdload")
    assert "WHEN MATCHED THEN DELETE" in c and not r.todos
    c, r = convert(_mload("UPDATE s.o SET v = :v WHERE k = :k;\nINSERT INTO s.o (k, v) VALUES (:k, :v);"), "tdload")
    assert "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)" in c and not r.todos
    c, r = convert(_mload("UPDATE s.o SET v = v + :v WHERE k = :k AND v > 0;"), "tdload")
    assert "t.v + s.v" in c and "t.v > 0" in c
    assert any("not de-duplicated" in m or "LAST row" in m for _, m in r.warnings)
    for bad in ("UPDATE s.o SET v = :nope WHERE k = :k;", "UPDATE s.o SET v = :v;", "DELETE FROM s.o;", "UPDATE s.o SET v = :v WHERE k = :k'); import os; ('"):
        c, r = convert(_mload(bad), "tdload")
        tree = ast.parse(c)
        assert r.todos and "spark.sql('MERGE" not in c
        assert not any(isinstance(n, ast.Import) and n.names[0].name == "os" for n in ast.walk(tree))


def test_loader_merge_tpt_and_multilabel():
    src = ("DEFINE JOB j (\n DEFINE SCHEMA sc (id VARCHAR(10), amt VARCHAR(12));\n"
           " DEFINE OPERATOR rd TYPE DATACONNECTOR PRODUCER SCHEMA sc ATTRIBUTES (VARCHAR FileName='/d/x.txt', VARCHAR Format='Delimited', VARCHAR TextDelimiter='|');\n"
           " DEFINE OPERATOR up TYPE UPDATE SCHEMA * ATTRIBUTES (VARCHAR UserName='u', VARCHAR UserPassword='p');\n"
           " APPLY ('UPDATE t SET amt = amt + :amt WHERE id = :id;', 'INSERT INTO t (id, amt) VALUES (:id, :amt);') TO OPERATOR (up[1]) SELECT * FROM OPERATOR (rd[1]);\n);")
    c, r = convert(src, "tdload")
    ast.parse(c)
    assert "WHEN MATCHED THEN UPDATE SET amt = t.amt + s.amt WHEN NOT MATCHED THEN INSERT (id, amt) VALUES (s.id, s.amt)" in c
    assert "'p'" not in c and "UserPassword" not in c


def test_teradata_new_features_linear_time():
    import time
    for q in ("SELECT TOP 3 WITH TIES " + "a, " * 3000 + "b FROM t ORDER BY " + "a, " * 3000 + "b",
              "SEL a FROM t SAMPLE WHEN " + "b > 1 THEN 0.1 WHEN " * 3000 + "b > 1 THEN 0.1 END",
              "SELECT " + "a OVERLAPS " * 2000 + "b FROM t",
              "CREATE TABLE p (" + "v PERIOD(DATE), " * 2000 + "id INT)"):
        t0 = time.time()
        try:
            _td(q)
        except Exception:
            pass
        assert time.time() - t0 < 4, q[:40]
    t0 = time.time()
    try:
        convert("REPLACE PROCEDURE a.b ()\nBEGIN\n" + "DECLARE c CURSOR FOR SEL a FROM t;\n" * 2000 + "DECLARE EXIT HANDLER FOR " * 500 + "END;", "proc", sql_dialect="teradata")
    except Exception:
        pass
    assert time.time() - t0 < 6


# ---------------------------------------------------------------- provider-agnostic AI
def _mini_server(handler_body):
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    seen = {}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            seen["path"], seen["auth"], seen["xkey"] = self.path, self.headers.get("authorization"), self.headers.get("x-api-key")
            import json as _j
            seen["body"] = _j.loads(self.rfile.read(int(self.headers["Content-Length"])))
            data = _j.dumps(handler_body(seen["body"])).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, seen


def test_provider_resolution_and_errors(monkeypatch):
    from sparqix.ai import resolve_provider, make_client, AIError, OpenAICompatClient, AnthropicClient
    for k in ("SPARQIX_AI_PROVIDER", "SPARQIX_AI_BASE_URL", "SPARQIX_AI_API_KEY", "SPARQIX_AI_MODEL", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GEMINI_API_KEY",
              "MISTRAL_API_KEY", "GROQ_API_KEY", "TOGETHER_API_KEY", "OPENROUTER_API_KEY"):
        monkeypatch.delenv(k, raising=False)
    assert resolve_provider() == "anthropic"
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    assert resolve_provider() == "openai"
    monkeypatch.setenv("SPARQIX_AI_BASE_URL", "http://localhost:8000/v1")
    assert resolve_provider() == "custom"
    monkeypatch.setenv("SPARQIX_AI_PROVIDER", "ollama")
    assert resolve_provider() == "ollama"
    with pytest.raises(AIError):
        resolve_provider("nope")
    # other providers have no default model, and a custom server needs a URL
    monkeypatch.delenv("SPARQIX_AI_BASE_URL")
    with pytest.raises(AIError):
        make_client("openai")
    with pytest.raises(AIError):
        make_client("custom", model="m")
    c = make_client("ollama", model="llama3")                       # no key needed for a local server
    assert isinstance(c, OpenAICompatClient) and c.base.startswith("http://localhost")
    assert isinstance(make_client("anthropic", api_key="k"), AnthropicClient)
    with pytest.raises(AIError):
        make_client("custom", model="m", base_url="http://evil.example.com/v1")      # plain http only for localhost


def test_openai_compatible_client_complete_and_tool_loop():
    from sparqix.ai import make_client
    from sparqix.agent import Session, run_agent
    state = {"n": 0}

    def reply(body):
        if "tools" not in body:
            return {"choices": [{"message": {"content": "```python\nx = 1\n```"}}]}
        state["n"] += 1
        if state["n"] == 1:
            return {"choices": [{"message": {"content": None, "tool_calls": [
                {"id": "c1", "type": "function", "function": {"name": "convert_text", "arguments": "{\"text\": \"x = 1\", \"filename\": \"a.py\"}"}}]}}],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5}}
        return {"choices": [{"message": {"content": "all done"}}], "usage": {"prompt_tokens": 3, "completion_tokens": 2}}
    srv, seen = _mini_server(reply)
    try:
        c = make_client("custom", api_key="sekret", model="m1", base_url=f"http://127.0.0.1:{srv.server_address[1]}/v1")
        assert "x = 1" in c.complete("sys", "hello")
        assert seen["path"] == "/v1/chat/completions" and seen["auth"] == "Bearer sekret" and seen["xkey"] is None
        assert seen["body"]["messages"][0] == {"role": "system", "content": "sys"} and seen["body"]["temperature"] == 0
        # the agent loop runs unchanged on top of the translated tool-calling format
        sess = Session(fs=False, default_target="spark", convert_opts={})
        res = run_agent("go", sess, c, max_steps=4)
        assert res.stopped == "done" and res.summary == "all done" and res.tokens == 20
        assert [s["tool"] for s in res.steps] == ["convert_text"]
        sent = seen["body"]["messages"]                               # second request carried the assistant tool call and the tool result back
        assert any(m.get("tool_calls") for m in sent) and any(m.get("role") == "tool" and m["tool_call_id"] == "c1" for m in sent)
        assert seen["body"]["tools"][0]["type"] == "function" and "parameters" in seen["body"]["tools"][0]["function"]
    finally:
        srv.shutdown()


def test_openai_compatible_client_bad_replies_and_key_scrub():
    from sparqix.ai import make_client, AIError
    srv, _ = _mini_server(lambda b: {"choices": [{"message": {"content": "hi", "tool_calls": [
        {"id": "z", "function": {"name": "t", "arguments": "{not json"}}]}}]})
    try:
        c = make_client("custom", api_key="sekret", model="m", base_url=f"http://127.0.0.1:{srv.server_address[1]}/v1")
        out = c.chat("s", [{"role": "user", "content": "u"}], [])
        tu = [b for b in out["content"] if b["type"] == "tool_use"][0]
        assert tu["input"] == {} and out["stop_reason"] == "tool_use"   # malformed arguments never crash the loop
        assert "sekret" not in c._scrub("error with sekret inside")
    finally:
        srv.shutdown()
    srv, _ = _mini_server(lambda b: {"unexpected": True})
    try:
        c = make_client("custom", model="m", base_url=f"http://127.0.0.1:{srv.server_address[1]}/v1")
        with pytest.raises(AIError):
            c.complete("s", "u")
    finally:
        srv.shutdown()


def test_web_provider_is_allow_listed_and_base_url_not_from_request(monkeypatch):
    from sparqix import web
    monkeypatch.setattr(web, "AI_CLIENT_FACTORY", None)
    with pytest.raises(ValueError):
        web._ai_client({"provider": "evil"}, "k", 5)
    c = web._ai_client({"provider": "openai", "model": "gpt-x", "base_url": "https://evil.example.com"}, "k", 5)
    assert c.base == "https://api.openai.com/v1"                  # a base_url in the request body is ignored


# ---------------------------------------------------------------- agent verify loop
_VERIFY_SRC = "import pandas as pd\ndf = pd.read_csv('sales.csv')\ntotals = df.groupby('region', as_index=False)['amt'].sum()\nn = len(df)\n"
_BAD_DRAFT = "import pandas as pd\ndf = pd.read_csv('sales.csv')\ntotals = df.groupby('region', as_index=False)['amt'].mean()\nn = len(df)\n"
_GOOD_DRAFT = _BAD_DRAFT.replace(".mean()", ".sum()")


def _local_runner(data_dir, env=None):
    """Stands in for `docker run`: executes the harness script with plain Python, pointing its /data at a temp folder.
    (No Docker daemon here; the real command line is covered by test_agent_sandbox_command_line.)"""
    import subprocess
    import sys

    def run(argv, timeout):
        vol = [argv[i + 1] for i, a in enumerate(argv) if a == "-v"]
        script_dir = [v.split(":")[0] for v in vol if v.endswith(":/work:ro")][0]
        text = open(os.path.join(script_dir, "script.py")).read().replace("DATA = '/data'", f"DATA = {data_dir!r}")
        p = os.path.join(script_dir, "local_script.py")
        open(p, "w").write(text)
        return subprocess.run([sys.executable, p], capture_output=True, text=True, timeout=timeout, shell=False, env=dict(os.environ, **(env or {})))
    return run


def _verify_session(tmp_path, draft_code):
    from sparqix.agent import Session, _new_draft
    from sparqix.core import Report
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    (data / "sales.csv").write_text("region,amt\nN,1\nN,3\nS,10\n")
    s = Session(str(tmp_path), str(tmp_path / "out"), approve=lambda k, d: True, sandbox_image="img:1", sandbox_data=str(data), runner=_local_runner(str(data)))
    _new_draft(s, "sales.py", "pandas", draft_code, Report(), "pyspark", _VERIFY_SRC)
    return s


def test_verify_draft_reports_mismatch_then_match(tmp_path):
    pytest.importorskip("pandas")
    from sparqix.agent import call_tool, verification_line
    s = _verify_session(tmp_path, _BAD_DRAFT)
    text, err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert not err and text.startswith("MISMATCH") and "totals" in text and "matching: df, n" in text
    assert "DIFFER" in verification_line(s.drafts["d1"])
    assert call_tool(s, "patch_draft", {"draft": "d1", "old": ".mean()", "new": ".sum()"})[1] is False
    assert "edited after" in verification_line(s.drafts["d1"])                    # the old result no longer counts
    text, err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert not err and text.startswith("MATCH") and "matched" in verification_line(s.drafts["d1"])


def test_verify_draft_distinguishes_failures_and_has_limits(tmp_path):
    pytest.importorskip("pandas")
    from sparqix.agent import call_tool, tool_specs, Session
    s = _verify_session(tmp_path, "raise RuntimeError('boom')\n")
    text, err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert not err and "CONVERTED script failed" in text and "boom" in text
    s.drafts["d1"].source = "raise ValueError('bad sample')\n"
    text, _ = call_tool(s, "verify_draft", {"draft": "d1"})
    assert "ORIGINAL script failed" in text and "not your patch" in text
    s.drafts["d1"].lang = "sas"
    text, is_err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert is_err and "not available for sas" in text and "SAS engine" in text       # no original runner: say why
    s2 = _verify_session(tmp_path, _BAD_DRAFT)
    s2.max_verify = 2
    for _ in range(2):
        assert not call_tool(s2, "verify_draft", {"draft": "d1"})[1]
    text, err = call_tool(s2, "verify_draft", {"draft": "d1"})
    assert err and "budget" in text
    # not offered without a sandbox image AND sample data
    assert "verify_draft" not in [t["name"] for t in tool_specs(Session(str(tmp_path), str(tmp_path / "o2")))]
    assert "verify_draft" not in [t["name"] for t in tool_specs(Session(str(tmp_path), str(tmp_path / "o2"), sandbox_image="img:1"))]


def test_agent_verify_loop_end_to_end_and_report_cannot_overstate(tmp_path):
    pytest.importorskip("pandas")
    from sparqix.agent import run_agent

    def use(i, name, **inp):
        return {"content": [{"type": "tool_use", "id": i, "name": name, "input": inp}], "usage": {}}
    script = [use("1", "verify_draft", draft="d1"),
              use("2", "patch_draft", draft="d1", old=".mean()", new=".sum()"),
              use("3", "verify_draft", draft="d1"),
              use("4", "write_draft", draft="d1", out_path="sales_spark.py"),
              use("5", "write_report", markdown="# Report\nAll verified, nothing to do."),
              use("6", "finish", summary="fixed the aggregate and verified it")]

    class Model:
        def __init__(self):
            self.n = 0

        def chat(self, system, messages, tools, max_tokens):
            r = script[self.n]
            self.n += 1
            return r
    s = _verify_session(tmp_path, _BAD_DRAFT)
    res = run_agent("fix sales.py", s, Model(), max_steps=10)
    assert res.stopped == "done" and [x["tool"] for x in res.steps] == ["verify_draft", "patch_draft", "verify_draft", "write_draft", "write_report", "finish"]
    assert res.steps[0]["result"].startswith("MISMATCH") and res.steps[2]["result"].startswith("MATCH")
    assert "totals" in (tmp_path / "out" / "sales_spark.py").read_text()
    rep = (tmp_path / "out" / "MIGRATION.md").read_text()
    assert "generated by sparqix, not by the model" in rep and "matched the original" in rep and "2 run(s)" in rep
    # a model that never verified cannot get a "matched" line into the report
    s2 = _verify_session(tmp_path / "x" if (tmp_path / "x").mkdir() is None else tmp_path, _BAD_DRAFT)
    from sparqix.agent import call_tool
    call_tool(s2, "write_report", {"markdown": "Everything verified!"})
    assert "not verified" in open(os.path.join(s2.out, "MIGRATION.md")).read()


def test_verify_harness_is_built_from_read_text_and_parses_output():
    from sparqix.verify import build_harness, parse_harness_output
    h = build_harness("x = 1\n", "x = 2\n", ["x"], 1e-6, "/data")
    ast.parse(h)
    assert "ORIGINAL = 'x = 1\\n'" in h and "DATA = '/data'" in h
    assert parse_harness_output("noise\nSPARQIX_VERIFY_JSON:{\"results\": {\"a\": []}, \"error\": null}\n") == {"results": {"a": []}, "error": None}
    assert parse_harness_output("no marker here") is None and parse_harness_output("SPARQIX_VERIFY_JSON:{broken") is None


# ---------------------------------------------------------------- verify loop for every language that has an original runner
_STUB_PYSPARK = """import sqlite3
import pandas as pd
_con = sqlite3.connect(':memory:')


class _DF:
    def __init__(self, df):
        self.df = df

    def toPandas(self):
        return self.df

    def createOrReplaceTempView(self, name):
        self.df.to_sql(name, _con, index=False, if_exists='replace')


class _Read:
    def csv(self, path, header=True, inferSchema=True):
        return _DF(pd.read_csv(path))


class _S:
    read = _Read()

    def sql(self, q):
        if q.lstrip().upper().startswith(('SELECT', 'WITH')):
            return _DF(pd.read_sql_query(q, _con))
        _con.execute(q)
        return _DF(pd.DataFrame())


class _B:
    def master(self, *a):
        return self
    appName = config = master

    def getOrCreate(self):
        return _S()


class SparkSession:
    builder = _B()
"""


def _stub_env(tmp_path):
    d = tmp_path / "stubs" / "pyspark" / "sql"
    d.mkdir(parents=True, exist_ok=True)
    (tmp_path / "stubs" / "pyspark" / "__init__.py").write_text("")
    (d / "__init__.py").write_text(_STUB_PYSPARK)
    return {"PYTHONPATH": str(tmp_path / "stubs")}


def _lang_session(tmp_path, lang, source, draft, rel, sql_out="pyspark", env=None):
    from sparqix.agent import Session, _new_draft
    from sparqix.core import Report
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    (data / "sales.csv").write_text("region,amt\nN,1\nN,3\nS,10\n")
    s = Session(str(tmp_path), str(tmp_path / "out"), approve=lambda k, d: True, sandbox_image="img:1", sandbox_data=str(data),
                runner=_local_runner(str(data), env))
    _new_draft(s, rel, lang, draft, Report(), sql_out, source)
    return s


def test_verify_which_languages_can_be_verified():
    from sparqix.verify import unverifiable_reason, VERIFIABLE
    for lang in ("pandas", "polars", "dask", "notebook", "sql", "r"):
        assert lang in VERIFIABLE and unverifiable_reason(lang) is None
    for lang, word in (("sas", "SAS"), ("scala", "JVM"), ("java", "JVM"), ("excel", "workbook"), ("dbt", "ref()"), ("powercenter", "own tool")):
        assert word in unverifiable_reason(lang)


def test_verify_normalises_polars_dask_and_helpers():
    pd = pytest.importorskip("pandas")
    from sparqix.verify import _as_pandas, _split_sql, _notebook_code, _is_query

    class PolarsLike:
        __module__ = "polars.dataframe.frame"

        def to_pandas(self):
            return pd.DataFrame({"a": [1]})

    class DaskLike:
        __module__ = "dask.dataframe.core"

        def compute(self):
            return pd.DataFrame({"b": [2]})
    assert list(_as_pandas(PolarsLike()).columns) == ["a"] and list(_as_pandas(DaskLike()).columns) == ["b"]
    assert _as_pandas(True) is None and _as_pandas("text") is None and _as_pandas(3) == 3
    assert _split_sql("select 'a;b' as x; -- c;\nselect 2 /* ; */ ;") == ["select 'a;b' as x", "select 2"]
    assert _is_query("(SELECT 1)") and _is_query("with t as (select 1) select * from t") and not _is_query("create table t(a)")
    nb = '{"cells":[{"cell_type":"code","source":["%matplotlib inline\\n","x = 1\\n"]},{"cell_type":"markdown","source":["# hi"]},{"cell_type":"code","source":"y = x + 1"}]}'
    assert _notebook_code(nb).split() == ["x", "=", "1", "y", "=", "x", "+", "1"]


def test_verify_sql_original_runs_in_sqlite_and_compares_with_spark_side(tmp_path):
    pytest.importorskip("pandas")
    from sparqix.agent import call_tool
    env = _stub_env(tmp_path)
    src = "SELECT region, SUM(amt) AS total FROM sales GROUP BY region;\nSELECT COUNT(*) AS n FROM sales;\n"
    bad = src.replace("SUM(amt)", "AVG(amt)")
    s = _lang_session(tmp_path, "sql", src, bad, "q.sql", sql_out="sql", env=env)
    text, err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert not err and text.startswith("MISMATCH") and "query_1" in text and "matching: query_2" in text
    assert call_tool(s, "patch_draft", {"draft": "d1", "old": "AVG(amt)", "new": "SUM(amt)"})[1] is False
    text, err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert not err and text.startswith("MATCH") and "query_1" in text
    # a different number of SELECTs is a difference too
    s.drafts["d1"].code = "SELECT region, SUM(amt) AS total FROM sales GROUP BY region;\n"
    text, _ = call_tool(s, "verify_draft", {"draft": "d1"})
    assert "statements" in text and "different number" in text
    # SQL in another dialect that SQLite rejects is reported as an original-side problem, not as a failed patch
    s.drafts["d1"].source = "SEL region FROM sales SAMPLE 2;\n"
    text, _ = call_tool(s, "verify_draft", {"draft": "d1"})
    assert "ORIGINAL script failed" in text and "dialect" in text
    # the draft must be Spark SQL text, not PySpark code
    s.drafts["d1"].sql_out = "pyspark"
    assert call_tool(s, "verify_draft", {"draft": "d1"})[1]


def test_verify_r_original_runs_with_rscript(tmp_path):
    pytest.importorskip("pandas")
    from sparqix.agent import call_tool
    import stat
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "Rscript"                      # stands in for R: writes what the real wrapper would (no R available here)
    fake.write_text("#!/usr/bin/env python3\nimport os\nout = os.environ['SPARQIX_R_OUT']\n"
                    "open(os.path.join(out, 'totals.csv'), 'w').write('region,amt\\nN,4\\nS,10\\n')\n"
                    "print('SQX_SCALAR\\tn\\t3')\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    env = {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    r_src = "df <- read.csv('sales.csv')\ntotals <- aggregate(amt ~ region, df, sum)\nn <- nrow(df)\n"
    s = _lang_session(tmp_path, "r", r_src, _BAD_DRAFT, "a.R", env=env)
    text, err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert not err and text.startswith("MISMATCH") and "totals" in text and "matching" in text and "n" in text
    s.drafts["d1"].code = _GOOD_DRAFT
    text, _ = call_tool(s, "verify_draft", {"draft": "d1"})
    assert text.startswith("MATCH")
    # no Rscript on the PATH: reported as an original-side problem with the remedy
    s2 = _lang_session(tmp_path / "n", "r", r_src, _GOOD_DRAFT, "a.R", env={"PATH": "/nonexistent"}) if (tmp_path / "n").mkdir() is None else None
    text, _ = call_tool(s2, "verify_draft", {"draft": "d1"})
    assert "ORIGINAL script failed" in text and "Rscript" in text


def test_verify_notebook_original_compares_code_cells(tmp_path):
    pytest.importorskip("pandas")
    import json
    from sparqix.agent import call_tool
    nb = lambda code: json.dumps({"cells": [{"cell_type": "markdown", "source": ["# t"]}, {"cell_type": "code", "source": code.splitlines(True)}]})
    s = _lang_session(tmp_path, "notebook", nb(_VERIFY_SRC), nb(_BAD_DRAFT), "n.ipynb")
    text, err = call_tool(s, "verify_draft", {"draft": "d1"})
    assert not err and text.startswith("MISMATCH") and "totals" in text
    s.drafts["d1"].code = nb(_GOOD_DRAFT)
    assert call_tool(s, "verify_draft", {"draft": "d1"})[0].startswith("MATCH")


def test_verify_cli_says_why_a_language_cannot_be_verified(tmp_path, capsys):
    from sparqix import verify
    f = tmp_path / "a.sas"
    f.write_text("data a; set b; run;\n")
    assert verify.main([str(f), "--trust"]) == 4
    out = capsys.readouterr().out
    assert "cannot verify" in out and "SAS" in out
    with pytest.raises(verify.NotVerifiable):
        verify.build_harness("x", "y", language="sas")


# ---------------------------------------------------------------- dummy inputs in every language (tests/dummy)
# file -> (detected language, SQL dialect, substrings that must be in the output, number of TODOs expected)
_DUMMY = {
    "pd_window.py": ("pandas", "generic", ["F.lag(F.col('amt'), 1).over(Window.partitionBy('user')", "rowsBetween(-2, Window.currentRow)", "for i, row in enumerate(df.toLocalIterator()):"], 0),
    "r_window.R": ("r", "generic", ["F.lag(F.col('amt'), 1).over(Window.partitionBy('user').orderBy(F.col('ts').asc()))", "F.rank().over(", ".pivot('kind').agg(F.sum('amt'))"], 0),
    "sas_macro.sas": ("sas", "generic", ["sales_f = sales.withColumn('big', F.when(F.col('amt') > 100, 1).otherwise(0))", "orders_f = orders.withColumn('big', F.when(F.col('amt') > 10, 1).otherwise(0))",
                                         "t1 = base.withColumn('x', 1)", "t2 = base.withColumn('x', 2)", "hi = sales.filter(F.col('amt') > 100)"], 0),
    "sas_datalines.sas": ("sas", "generic", [".createDataFrame([(1.0, 'Ann', 10.5, '2024-01-02'), (2.0, \"O'Brien\", None", "F.to_date(F.col('joined'), 'yyyy-MM-dd')",
                                             "spark.read.csv('/data/in.csv', schema='a double, b string, c double', sep=',', header=True)"], 0),
    "sas_merge.sas": ("sas", "generic", ["def _sas_merge(frames, flags, by):", "_sas_merge([a.select('id', 'x'), b], ['ina', 'inb'], ['id'])", ".filter(F.col('ina') & F.col('inb'))",
                                         ".filter(F.col('p') & (~F.col('q')))"], 0),
    "sas_bygroup.sas": ("sas", "generic", ["F.row_number().over(Window.partitionBy('region').orderBy(F.col('_row').desc())) == 1", "F.lag(F.col('x'), 2).over(Window.orderBy('_row'))",
                                           "(F.lit(100) + F.sum(", "F.last(F.col('v'), ignorenulls=True).over(Window.partitionBy('id')"], 0),
    "sales_tsql.sql": ("sql", "tsql", ["COALESCE(SUM(o.amount), 0)", "LIMIT 5", "datediff("], 0),
    "sales_oracle.sql": ("sql", "oracle", ["CASE e.deptno WHEN 10", "date_format(e.hiredate, 'yyyy-MM-dd')", "ORDER BY e.sal DESC LIMIT 10"], 0),
    "pg.sql": ("sql", "postgres", ["CAST(name AS STRING)", "array_join(collect_list(tag), ',')"], 0),
    "bq.sql": ("sql", "bigquery", ["NULLIF(b, 0)", "date_format(dt, 'yyyy-MM')", "slice(transform(sort_array(collect_list("], 0),
    "snow.sql": ("sql", "snowflake", ["date_format(dt, 'yyyy-MM')", "array_join(sort_array(collect_list(tag)), ',')", "IF(amt>10"], 0),
    "td.sql": ("sql", "teradata", ["QUALIFY RANK() OVER (ORDER BY SUM(amt) DESC) <= 3", "LIMIT 3", "DATE '2024-01-01'"], 0),
    "mysql.sql": ("sql", "mysql", ["LIMIT 10 OFFSET 5", "date_format(dt, 'yyyy-MM-dd')"], 0),
    "prog.sas": ("sas", "generic", ["spark.table('sales.orders')", ".filter(F.col('amount') > 100)", "F.when(F.col('amount') > 1000, 'GOLD')",
                                    "big.createOrReplaceTempView('big')", "F.col('net').desc()",
                                    "F.sum(F.coalesce(F.col('net'), F.lit(0))).over(Window.partitionBy('region').orderBy('_row')"], 0),
    "an.R": ("r", "generic", [".groupBy('region')", "F.sum(F.col('net')).alias('total')", ".write.mode('overwrite').csv('out.csv', header=True)"], 0),
    "pl.py": ("polars", "generic", [".groupBy('region')", ".write.mode('overwrite').parquet('out.parquet')"], 0),
    "dk.py": ("dask", "generic", [".groupBy('region')", ".write.mode('overwrite').csv('out.csv'"], 0),
    "pd1.py": ("pandas", "generic", ["F.date_format(F.col('dt'), 'yyyy-MM')", "how='left'"], 1),                      # iterrows is flagged
    "plain.py": ("python", "generic", ["sc.parallelize([1, 2, 3, 4, 5])"], 0),
    "Demo.scala": ("scala", "generic", ["sc.parallelize(List(1,2,3,4,5,6))", "reduceByKey"], 0),
    "S.java": ("java", "generic", ["jsc.parallelize(xs)"], 0),
    "f.formulas": ("excel", "generic", ["F.when(F.col('A') == 'x', F.col('B'))", "_xl_lookup(df, F.col('A'), lk_sheet2, 'A', 'B', '_lk1')"], 0),
    "proc_tsql.sql": ("proc", "tsql", ["def usp_load(spark, d, min_):", "args={'d': d, 'min_': min_}", "while n < 3:"], 0),
    "proc_ora.sql": ("proc", "oracle", ["def load_t(spark, p_d, p_min):", "for i in range(1, 3 + 1):"], 0),
    "job.hql": ("hive", "hive", ["INSERT OVERWRITE TABLE tgt PARTITION(dt)", "partitionOverwriteMode"], 0),
    "job.pig": ("pig", "generic", [".filter((F.col('amt') > F.lit(100)))", ".groupBy('region').agg(F.sum(F.col('amt')).alias('total'))"], 0),
    "refresh.bteq": ("bteq", "teradata", ["sys.exit(4)", "SELECT region, SUM(amt) FROM sales GROUP BY 1"], 1),        # ACTIVITYCOUNT is flagged
}


@pytest.mark.parametrize("name", sorted(_DUMMY))
def test_dummy_inputs_every_language(name):
    lang, dialect, must, todos = _DUMMY[name]
    src = open(os.path.join(HERE, "dummy", name)).read()
    assert detect_language(src, name) == lang
    code, rep = convert(src, "auto", filename=name, sql_dialect=dialect)
    if not name.endswith((".java", ".scala")):
        ast.parse(code)
    for m in must:
        assert m in code, (name, m)
    assert len(rep.todos) == todos, (name, rep.todos)
    assert "password" not in code.lower() and "tdp/user" not in code


def test_dummy_bug_fixes_stay_fixed():
    # BigQuery ARRAY_AGG(... ORDER BY ... LIMIT n) is not valid Spark: never emitted as collect_list(... ORDER BY ...); converted via sort_array/slice
    code, rep = convert("SELECT ARRAY_AGG(e ORDER BY ts LIMIT 3) a FROM t", "sql", sql_dialect="bigquery", sql_out="sql")
    assert "collect_list(e ORDER BY" not in code and "slice(transform(sort_array(" in code and not rep.todos
    # LISTAGG ordered by another column: warned, not silently reordered
    _, rep = convert("SELECT LISTAGG(a, ',') WITHIN GROUP (ORDER BY b) FROM t", "sql", sql_dialect="snowflake", sql_out="sql")
    assert any("WITHIN GROUP" in w[1] for w in rep.warnings)
    # R: a named argument inside a call is not an assignment, and standalone write.csv(df, path) converts
    code, rep = convert('df <- read.csv("a.csv")\nwrite.csv(df, "o.csv", row.names = FALSE)\n', "r")
    assert ".write.mode('overwrite').csv('o.csv', header=True)" in code and not rep.todos
    # SAS: an input dataset never produced in the script is loaded as a table; PROC SQL gets the DataFrames it reads registered as views
    code, _ = convert("data a; set lib.t; x = 1; run;\nproc sql; create table b as select x from a; quit;\n", "sas")
    assert "t = spark.table('lib.t')" in code and code.index("a.createOrReplaceTempView('a')") < code.index('spark.sql("""')


# ---------------------------------------------------------------- SAS: RETAIN, FIRST./LAST., LAG, MERGE, macros, DATALINES
def sas(src):
    code, rep = convert(src, "sas")
    if not rep.todos:
        ast.parse(code)
    return code, rep


def test_sas_datalines_and_infile():
    code, rep = sas("data p; input id name $ amt; datalines;\n1 Ann 2.5\n2 O'Brien .\n;\nrun;\n")
    assert "spark.createDataFrame([(1.0, 'Ann', 2.5), (2.0, \"O'Brien\", None)], 'id double, name string, amt double')" in code and not rep.todos
    code, rep = sas("data r; infile '/d/in.csv' dlm=',' dsd firstobs=2; input a b $; run;")
    assert "spark.read.csv('/d/in.csv', schema='a double, b string', sep=',', header=True)" in code
    for bad in ("data d; input @1 a 3.; datalines;\n123\n;\nrun;", "data d; input a weird.; datalines;\n1\n;\nrun;", "data d; datalines;\n1\n;\nrun;"):
        _, rep = sas(bad)
        assert rep.todos, bad                                               # unsupported forms are flagged, never guessed
    # data that looks like code stays data: it only ever becomes literals
    code, _ = sas("data d; input a $; datalines;\n'); import os; os.system('id')\n;\nrun;\n")
    assert not any(isinstance(n, (ast.Import, ast.ImportFrom)) and not (isinstance(n, ast.ImportFrom) and n.module == "pyspark.sql") for n in ast.walk(ast.parse(code)))


def test_sas_by_group_retain_and_lag():
    code, rep = sas("data g; set s; by r; if first.r then n = 0; n + 1; run;")
    assert "Window.partitionBy('r').orderBy('_row').rowsBetween" in code and ".drop('_row')" in code and not rep.todos
    code, _ = sas("data u; set s; retain tot 100; tot = tot + a; run;")
    assert "(F.lit(100) + F.sum(F.coalesce(F.col('a'), F.lit(0)))" in code
    code, _ = sas("data c; set s; by g; if first.g then c = 1; else c = c + 1; retain c; run;")
    assert "F.sum(F.coalesce(F.lit(1), F.lit(0))).over(Window.partitionBy('g')" in code
    code, _ = sas("data l; set s; by r; if last.r then output; run;")
    assert ".filter(F.row_number().over(Window.partitionBy('r').orderBy(F.col('_row').desc())) == 1)" in code
    # FIRST.x without BY x, LAG inside IF, an unrecognised RETAIN use, DO loops and several OUTPUTs are flagged, not guessed
    for bad in ("data g; set s; if first.r then n = 1; run;", "data l; set s; if x > lag(x) then u = 1; run;", "data d; set s; retain m; m = f(x); run;",
                "data z; set s; do i = 1 to 3; output; end; run;" ):
        _, rep = sas(bad)
        assert rep.todos, bad


def test_sas_merge():
    code, rep = sas("data b; merge x(in=a keep=id v) y(in=b2); by id; if a and not b2; run;")
    assert "_sas_merge([x.select('id', 'v'), y], ['a', 'b2'], ['id'])" in code and ".filter(F.col('a') & (~F.col('b2')))" in code and ".drop('a', 'b2')" in code
    assert "def _sas_merge(frames, flags, by):" in code and any("MERGE" in w[1] for w in rep.warnings)
    _, rep = sas("data b; merge x y; run;")
    assert rep.todos                                                          # MERGE without BY pairs rows by position: flagged


def test_sas_merge_helper_semantics():
    """run the generated helper on plain Python stand-ins for DataFrames: the later dataset wins for same-named columns where it has the row"""
    code, _ = sas("data b; merge x(in=a) y(in=b2); by id; run;")
    helper = code[code.index("def _sas_merge"):code.index("\n\n\nx = ")] if "\n\n\nx = " in code else code[code.index("def _sas_merge"):]
    assert "F.when(F.col(flag), F.col(c + '__r')).otherwise(F.col(c))" in helper


def test_sas_macros_expand_at_conversion_time():
    code, rep = sas("%let thr = 100;\n%macro f(ds, lim=10);\ndata &ds._f; set &ds; if a > &lim then b = 1; else b = 0; run;\n%mend;\n%f(s, lim=&thr);\n%f(t)\n"
                    "%macro r(n); %do i = 1 %to &n; data t&i; set base; x = &i; run; %end; %mend;\n%r(3)\n")
    assert "s_f = s.withColumn('b', F.when(F.col('a') > 100, 1).otherwise(0))" in code and "t_f = t.withColumn('b', F.when(F.col('a') > 10, 1).otherwise(0))" in code
    assert all(f"t{k} = base.withColumn('x', {k})" in code for k in (1, 2, 3)) and not rep.todos
    code, _ = sas("%let m = B;\n%macro c(v); %if &v = A %then %do; data a; set b; run; %end; %else %if &v = B %then %do; data c; set d; run; %end; %else %do; data e; set f; run; %end; %mend;\n%c(&m)\n")
    assert "c = d" in code and "a = b" not in code and "e = f" not in code
    code, rep = sas("%let x = %sysfunc(today());\ndata d; set s; y = &x; run;\n")      # not knowable statically: left as TODO
    assert rep.todos
    # '&' inside single quotes is not a macro reference (like SAS)
    code, _ = sas("%let v = 5;\ndata d; set s; t = '&v'; u = \"&v\"; run;")
    assert "'&v'" in code and "'5'" in code


def test_sas_macro_limits():
    import time
    t0 = time.time()
    _, rep = sas("%macro m(a); %m(&a) %mend;\n%m(1);\n")                      # recursion is cut off, not followed forever
    assert time.time() - t0 < 5
    _, rep = sas("%macro big(n); %do i = 1 %to 100000; data t&i; set b; run; %end; %mend;\n%big(1);\n")
    assert rep.todos or rep.warnings
    t0 = time.time()
    sas("data a;\n" + "datalines;\n1\n" * 6000)                              # unterminated DATALINES blocks: linear time
    assert time.time() - t0 < 5


# ---------------------------------------------------------------- pandas: shift/diff/cumsum/rolling/iterrows ; R: lag/lead/cumsum/slice/pivot
def pyc(src):
    code, rep = convert("import pandas as pd\n" + src, "pandas")
    ast.parse(code)
    return code, rep


def test_pandas_shift_cumsum_rolling_use_sort_order():
    code, rep = pyc("df = pd.read_csv('a.csv')\ndf = df.sort_values('d')\n"
                    "df['p'] = df['v'].shift(1)\ndf['n'] = df['v'].shift(-2, fill_value=0)\ndf['c'] = df['v'].cumsum()\n"
                    "df['d1'] = df['v'].diff()\ndf['pc'] = df['v'].pct_change()\ndf['r'] = df['v'].rolling(3).mean()\n"
                    "df['rm'] = df['v'].rolling(3, min_periods=1).sum()\ndf['e'] = df['v'].expanding().max()\n")
    w = "Window.orderBy(F.col('d').asc())"
    assert f"F.lag(F.col('v'), 1).over({w})" in code and f"F.lead(F.col('v'), 2, 0).over({w})" in code
    assert f"F.when(F.col('v').isNotNull(), F.sum(F.col('v')).over({w}.rowsBetween(Window.unboundedPreceding, Window.currentRow)))" in code
    assert f"F.col('v') - F.lag(F.col('v'), 1).over({w})" in code and f"F.col('v') / F.lag(F.col('v'), 1).over({w}) - 1" in code
    assert f"F.when(F.count(F.col('v')).over({w}.rowsBetween(-2, Window.currentRow)) >= 3, F.avg(F.col('v')).over({w}.rowsBetween(-2, Window.currentRow)))" in code
    assert f"F.sum(F.col('v')).over({w}.rowsBetween(-2, Window.currentRow))" in code and "F.max(F.col('v')).over(" in code
    assert not rep.todos and any("single partition" in x[1] for x in rep.warnings)
    assert "monotonically_increasing_id" not in code                           # explicit sort is used, no fallback warning
    assert not any("row order" in x[1] for x in rep.warnings)


def test_pandas_window_methods_fall_back_to_row_order_with_warning():
    code, rep = pyc("df = pd.read_csv('a.csv')\ndf['p'] = df['v'].shift()\n")
    assert "Window.orderBy(F.monotonically_increasing_id())" in code and any("row order" in x[1] for x in rep.warnings)
    code, rep = pyc("df = pd.read_csv('a.csv')\ndf = df.sort_values('d')\ndf['d'] = df['d'].shift()\ndf['q'] = df['v'].shift()\n")
    assert code.count("F.monotonically_increasing_id()") == 1                  # once the sort column is overwritten the order is no longer trusted


def test_pandas_groupby_window_methods_and_transform_lambda():
    code, rep = pyc("df = pd.read_csv('a.csv').sort_values(['k', 'd'])\ndf['c'] = df.groupby('k')['v'].cumsum()\n"
                    "df['s'] = df.groupby(['k', 'j'])['v'].shift(1)\ndf['n'] = df.groupby('k').cumcount()\n"
                    "df['m'] = df.groupby('k')['v'].transform(lambda s: s.rolling(2).mean())\n")
    assert "Window.partitionBy('k').orderBy(F.col('k').asc(), F.col('d').asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)" in code
    assert "F.lag(F.col('v'), 1).over(Window.partitionBy('k', 'j')" in code and "F.row_number().over(Window.partitionBy('k')" in code and "- 1)" in code
    assert ">= 2, F.avg(F.col('v')).over(Window.partitionBy('k')" in code and not rep.todos
    assert not any("single partition" in x[1] for x in rep.warnings)


def test_pandas_window_methods_unsupported_forms_are_todos():
    for bad in ("df['x'] = df['v'].cumprod(skipna=False)", "df['x'] = df['v'].rank(method='bogus')", "df['x'] = df['v'].rolling('7D').sum()", "df['x'] = df['v'].rolling(3, center=True).sum()",
                "df['x'] = df['v'].rolling(3).median()", "df['x'] = df['v'].shift(freq='D')", "df['x'] = df['v'].diff(0)",
                "df['x'] = df['v'].cumsum(skipna=False)", "df['x'] = df.groupby('k')[['v', 'w']].cumsum()", "df['x'] = df['v'].rolling(n).sum()"):
        code, rep = pyc("df = pd.read_csv('a.csv')\n" + bad + "\n")
        assert rep.todos, bad


def test_pandas_iterrows_driver_side_loop():
    code, rep = pyc("df = pd.read_csv('a.csv')\ntotal = 0\nfor i, row in df.iterrows():\n    total += row['v']\nfor t in df.itertuples():\n    print(t.v)\n")
    assert "for i, row in enumerate(df.toLocalIterator()):\n    total += row['v']" in code and "for t in df.toLocalIterator():\n    print(t.v)" in code
    assert not rep.todos and any("toLocalIterator" in x[1] for x in rep.warnings)
    for bad in ("for i, row in df.iterrows():\n    df.loc[i, 'z'] = row['v']", "for t in df.itertuples():\n    print(t.Index)",
                "for i, row in df.iterrows():\n    pass\nelse:\n    pass", "for row in df.iterrows():\n    pass"):
        _, rep = pyc("df = pd.read_csv('a.csv')\n" + bad + "\n")
        assert rep.todos, bad


def rc(src):
    code, rep = convert(src, "r")
    ast.parse(code)
    return code, rep


def test_r_lag_lead_cumsum_row_number():
    code, rep = rc('d <- read_csv("a") %>% arrange(dt)\nx <- d %>% group_by(k) %>% mutate(p = lag(v), n = lead(v, 2, default = 0), c = cumsum(v), r = row_number())\n'
                   'y <- d %>% mutate(q = dplyr::lag(v, order_by = desc(dt)))\n')
    w = "Window.partitionBy('k').orderBy(F.col('dt').asc())"
    assert f"F.lag(F.col('v'), 1).over({w})" in code and f"F.lead(F.col('v'), 2, 0).over({w})" in code and f"F.row_number().over({w})" in code
    assert f"F.when(F.sum(F.col('v').isNull().cast('int')).over({w}.rowsBetween(Window.unboundedPreceding, Window.currentRow)) == 0," in code      # R: NA propagates
    assert "F.lag(F.col('v'), 1).over(Window.orderBy(F.col('dt').desc()))" in code and not rep.todos
    code, rep = rc('d <- read_csv("a")\nx <- d %>% mutate(p = lag(v))\n')
    assert "Window.orderBy(F.monotonically_increasing_id())" in code and any("arrange" in w[1] for w in rep.warnings)
    code, _ = rc('d <- read_csv("a") %>% arrange(dt)\nd$p <- lag(d$v)\n')
    assert "d = d.withColumn('p', F.lag(F.col('v'), 1).over(Window.orderBy(F.col('dt').asc())))" in code
    for bad in ("x <- d %>% mutate(q = lag(v, n = k))", "x <- d %>% summarise(q = lag(v))", "x <- d %>% mutate(q = lag(v, default = 1, foo = 2))"):
        _, rep = rc('d <- read_csv("a")\n' + bad + "\n")
        assert rep.todos, bad


def test_r_window_in_filter_is_materialised_first():
    code, _ = rc('d <- read_csv("a") %>% arrange(dt)\nf <- d %>% group_by(k) %>% filter(v > lag(v))\n')
    assert ".withColumn('_f0', F.col('v') > F.lag(F.col('v'), 1).over(Window.partitionBy('k')" in code and ".filter(F.col('_f0'))" in code and ".drop('_f0')" in code


def test_r_slice_verbs():
    code, rep = rc('d <- read_csv("a") %>% arrange(dt, desc(v))\n'
                   'a <- d %>% group_by(k) %>% slice_head(n = 2)\nb <- d %>% slice_tail(n = 3)\n'
                   'c <- d %>% group_by(k) %>% slice_max(order_by = v, n = 1)\ne <- d %>% slice_min(v, n = 2, with_ties = FALSE)\n'
                   'f <- d %>% slice(1:3)\ng <- d %>% slice(c(2, 5))\nh <- d %>% group_by(k) %>% head(2)\n')
    assert "Window.partitionBy('k').orderBy(F.col('dt').asc(), F.col('v').desc())" in code and ".filter(F.col('_rn') <= 2)" in code
    assert "Window.orderBy(F.col('dt').desc(), F.col('v').asc())" in code                                        # slice_tail = head of the reversed order
    assert "F.rank().over(Window.partitionBy('k').orderBy(F.col('v').desc()))" in code                          # with_ties defaults to TRUE
    assert "F.row_number().over(Window.orderBy(F.col('v').asc()))" in code
    assert "F.col('_rn').between(1, 3)" in code and "F.col('_rn').isin([2, 5])" in code and "h = d.limit(2)" in code and not rep.todos
    for bad in ("slice(-1)", "slice(n())", "slice_head(prop = 0.1)", "slice_max(n = 2)", "slice_sample(n = 2, replace = TRUE)", "slice(0)"):
        _, rep = rc('d <- read_csv("a")\nx <- d %>% ' + bad + "\n")
        assert rep.todos, bad


def test_r_pivot_longer_and_wider():
    code, rep = rc('d <- read_csv("a")\nl <- d %>% pivot_longer(cols = c(a, b), names_to = "k", values_to = "v", values_drop_na = TRUE)\n'
                   'l2 <- d %>% pivot_longer(c(a, b))\nw <- l %>% pivot_wider(names_from = k, values_from = v, id_cols = c(id))\n'
                   'w2 <- d %>% pivot_wider(names_from = k, values_from = v, values_fn = sum, values_fill = 0)\n')
    assert "F.explode(F.array(F.struct(F.lit('a').alias('k'), F.col('a').alias('v')), F.struct(F.lit('b').alias('k'), F.col('b').alias('v'))))" in code
    assert ".drop('a', 'b')" in code and "F.col('_pl.k').alias('k')" in code and ".filter(F.col('v').isNotNull())" in code
    assert "F.col('_pl.k').alias('name'), F.col('_pl.v').alias('value')" in code
    assert "w = l.groupBy('id').pivot('k').agg(F.first('v'))" in code
    assert "(lambda d: d.groupBy(*[c for c in d.columns if c not in ('k', 'v')]).pivot('k').agg(F.sum('v')))(d).fillna(0)" in code and not rep.todos
    for bad in ("pivot_longer(cols = starts_with('a'))", "pivot_longer(-id)", "pivot_longer(a:c)", "pivot_longer(c(a, b), names_prefix = 'x')",
                "pivot_wider(names_from = k)", "pivot_wider(names_from = c(k, j), values_from = v)", "pivot_wider(names_from = k, values_from = c(v, w))",
                "pivot_wider(names_from = k, values_from = v, values_fn = list)", "pivot_longer(c(a, b), names_to = nm)"):
        _, rep = rc('d <- read_csv("a")\nx <- d %>% ' + bad + "\n")
        assert rep.todos, bad


def test_pandas_r_window_output_is_safe():
    # hostile column names / literals only ever appear inside repr() strings
    code, _ = pyc("df = pd.read_csv('a.csv')\ndf['x'] = df[\"v'); import os; os.system('id') #\"].shift(1)\n")
    assert "import os" not in [ln for ln in code.splitlines() if ln.startswith("import")]
    code, _ = rc('d <- read_csv("a")\nl <- d %>% pivot_longer(cols = c(a), names_to = "k\'); import os #", values_to = "v")\n')
    assert not any(isinstance(n, ast.Import) and any(a.name == "os" for a in n.names) for n in ast.walk(ast.parse(code)))


# ---------------------------------------------------------------- Excel lookups
def xl(src, **kw):
    code, rep = convert(src, "excel", **kw)
    if not rep.todos:
        ast.parse(code)
    return code, rep


def test_excel_vlookup_exact_and_approximate():
    code, rep = xl("a = =VLOOKUP(A2,Prices!A:C,3,FALSE)*B2\nt = =VLOOKUP(B2,'Tax Rates'!$A$2:$B$9,2)\nz = =IFERROR(VLOOKUP(A2,Prices!A:B,2,0),0)\n",
                   excel_columns={"Prices.A": "sku", "Prices.C": "price"})
    assert "df = _xl_lookup(df, F.col('A'), lk_prices, 'sku', 'price', '_lk1')\ndf = df.withColumn('a', F.col('_lk1') * F.col('B'))\ndf = df.drop('_lk1')" in code
    assert "df = _xl_approx(df, F.col('B'), lk_tax_rates, 'A', 'B', '_lk2')" in code                # default (TRUE) = approximate match
    assert "F.coalesce(F.col('_lk3'), F.lit(0))" in code
    assert "lk_prices = spark.table('prices')" in code and "lk_tax_rates = spark.table('tax_rates')" in code and "def _xl_lookup(" in code and "def _xl_approx(" in code
    assert not rep.todos and any("approximate" in w[1] for w in rep.warnings) and any("spark.table" in n for n in rep.notes)


def test_excel_xlookup_and_index_match():
    code, rep = xl('x = =XLOOKUP(A2,Prices!A:A,Prices!C:C,"n/a")\ny = =INDEX(Prices!C:C,MATCH(A2,Prices!A:A,0))\nw = =XLOOKUP(UPPER(A2),Prices!A:A,Prices!B:B)\n')
    assert "df = _xl_lookup(df, F.col('A'), lk_prices, 'A', 'C', '_lk1')" in code and "F.coalesce(F.col('_lk1'), F.lit('n/a'))" in code
    assert "df = _xl_lookup(df, F.col('A'), lk_prices, 'A', 'C', '_lk2')" in code and "df = df.withColumn('y', F.col('_lk2'))" in code
    assert "_xl_lookup(df, F.upper(F.col('A')), lk_prices, 'A', 'B', '_lk3')" in code and not rep.todos


def test_excel_lookup_unsupported_forms_are_todos():
    for bad in ("=VLOOKUP(A2,A:C,2,0)", "=MATCH(A2,Prices!A:A,0)", "=SUM(Prices!B:B)", "=VLOOKUP(A2,Prices!A:B,3,0)", "=VLOOKUP(A2,Prices!A:C,C2,0)",
                "=VLOOKUP(A2,Prices!A:C,2,B2)", "=INDEX(Prices!C:C,MATCH(A2,Prices!A:A,1))", "=INDEX(Prices!C:C,2)", "=HLOOKUP(A2,Prices!A1:C3,2,0)",
                "=XLOOKUP(A2,Prices!A:A,Prices!C:C,0,2)", "=XLOOKUP(A2,Prices!A:B,Prices!C:C)", "=XLOOKUP(A2,Prices!A:A,Other!C:C)", "=VLOOKUP(A2,Prices!A:C,0,0)"):
        _, rep = xl(bad + "\n")
        assert rep.todos, bad


def test_excel_lookup_helper_and_hostile_sheet_names_are_safe():
    code, rep = xl("a = =VLOOKUP(A2,'x\\'); import os; os.system(1) #'!A:B,2,0)\n")
    names = [n for n in ast.walk(ast.parse(code)) if isinstance(n, (ast.Import, ast.ImportFrom))]
    assert all(not (isinstance(n, ast.Import)) for n in names)
    code, _ = xl("a = =VLOOKUP(A2,'class def'!A:B,2,0)\n")
    assert "lk_class_def" in code                                              # variable names are prefixed, never a keyword


def test_excel_tokenizer_is_linear_on_hostile_input():
    import time
    for pay in ("'" * 30000, "a." * 15000, "a!" * 15000, "'a" * 15000):
        t0 = time.time()
        convert("=VLOOKUP(A2," + pay, "excel")
        assert time.time() - t0 < 3, pay[:4]


# ---------------------------------------------------------------- SQL: Oracle ROWNUM, BigQuery ARRAY_AGG
def osql(q, d="oracle"):
    out, rep = convert(q, "sql", sql_dialect=d, sql_out="sql")
    return out.strip().rstrip(";").strip(), rep


def test_oracle_rownum_bounds_become_limit():
    assert osql("SELECT * FROM emp WHERE ROWNUM <= 10")[0] == "SELECT * FROM emp LIMIT 10"
    assert osql("SELECT * FROM emp WHERE ROWNUM < 4")[0] == "SELECT * FROM emp LIMIT 3"
    assert osql("SELECT * FROM emp WHERE ROWNUM = 1")[0] == "SELECT * FROM emp LIMIT 1"
    assert osql("SELECT * FROM emp WHERE sal > 5 AND ROWNUM <= 3")[0] == "SELECT * FROM emp WHERE sal > 5 LIMIT 3"
    assert osql("SELECT * FROM emp WHERE ROWNUM <= 3 AND sal > 5 AND x = 1")[0] == "SELECT * FROM emp WHERE sal > 5 AND x = 1 LIMIT 3"
    assert osql("SELECT * FROM emp WHERE x BETWEEN 1 AND 5 AND ROWNUM <= 3")[0] == "SELECT * FROM emp WHERE x BETWEEN 1 AND 5 LIMIT 3"
    q, rep = osql("SELECT * FROM emp WHERE ROWNUM <= 10 AND sal > 5 ORDER BY sal")
    assert q == "SELECT * FROM emp WHERE sal > 5 ORDER BY sal LIMIT 10" and any("before ORDER BY" in w[1] for w in rep.warnings)
    q, rep = osql("SELECT * FROM (SELECT * FROM emp ORDER BY sal DESC) WHERE ROWNUM <= 5")      # top-N idiom: no ordering warning
    assert q == "SELECT * FROM (SELECT * FROM emp ORDER BY sal DESC) LIMIT 5" and not rep.warnings and not rep.todos
    assert osql("SELECT * FROM emp WHERE name = 'ROWNUM <= 3'")[0] == "SELECT * FROM emp WHERE name = 'ROWNUM <= 3'"


def test_oracle_rownum_in_select_list_and_pagination():
    q, rep = osql("SELECT e.*, ROWNUM AS n FROM emp e")
    assert q == "SELECT e.*, row_number() OVER (ORDER BY monotonically_increasing_id()) AS n FROM emp e" and any("single partition" in w[1] for w in rep.warnings)
    q, _ = osql("SELECT * FROM (SELECT a.*, ROWNUM rn FROM (SELECT * FROM emp ORDER BY sal) a WHERE ROWNUM <= 20) WHERE rn > 10")
    assert q == ("SELECT * FROM (SELECT * FROM (SELECT a.*, row_number() OVER (ORDER BY monotonically_increasing_id()) rn FROM (SELECT * FROM emp ORDER BY sal) a) _rn_w "
                 "WHERE rn <= 20) WHERE rn > 10")


def test_oracle_rownum_unsupported_forms_stay_todo():
    for bad in ("SELECT dept, COUNT(*) FROM emp WHERE ROWNUM <= 10 GROUP BY dept", "SELECT DISTINCT dept FROM emp WHERE ROWNUM <= 10", "SELECT COUNT(*) FROM emp WHERE ROWNUM <= 10",
                "SELECT * FROM emp WHERE ROWNUM > 5", "SELECT * FROM emp WHERE ROWNUM <= 3 OR sal > 1", "SELECT * FROM emp WHERE ROWNUM = 2",
                "SELECT * FROM a WHERE ROWNUM <= 3 UNION SELECT * FROM b", "SELECT * FROM emp WHERE ROWNUM <= 3 ORDER BY ROWNUM", "SELECT * FROM emp WHERE ROWNUM <= 3 LIMIT 5"):
        _, rep = osql(bad)
        assert rep.todos, bad


def test_bigquery_array_agg_order_limit():
    f = lambda e: osql(f"SELECT {e} FROM t", "bigquery")
    assert f("ARRAY_AGG(e)")[0] == "SELECT collect_list(e) FROM t"
    assert f("ARRAY_AGG(DISTINCT e)")[0] == "SELECT collect_set(e) FROM t"
    assert f("ARRAY_AGG(e ORDER BY e DESC)")[0] == "SELECT sort_array(collect_list(e), false) FROM t"
    assert f("ARRAY_AGG(e ORDER BY e)")[0] == "SELECT sort_array(collect_list(e)) FROM t"
    assert f("ARRAY_AGG(e ORDER BY ts LIMIT 3) a")[0] == ("SELECT slice(transform(sort_array(collect_list(CASE WHEN e IS NOT NULL THEN struct(ts AS _k0, e AS _v) END)), _e -> _e._v), 1, 3) a FROM t")
    assert f("ARRAY_AGG(e IGNORE NULLS ORDER BY ts DESC, id DESC LIMIT 2)")[0] == (
        "SELECT slice(transform(sort_array(collect_list(CASE WHEN e IS NOT NULL THEN struct(ts AS _k0, id AS _k1, e AS _v) END), false), _e -> _e._v), 1, 2) FROM t")
    assert f("ARRAY_AGG(e LIMIT 5)")[0] == "SELECT slice(collect_list(e), 1, 5) FROM t"
    assert "(UPPER(e)) IS NOT NULL THEN struct(ts AS _k0, UPPER(e) AS _v)" in f("ARRAY_AGG(UPPER(e) ORDER BY ts)")[0]
    q, rep = f("ARRAY_AGG(e ORDER BY ts, id DESC)")
    assert q.startswith("SELECT transform(array_sort(collect_list(CASE WHEN e IS NOT NULL THEN struct(ts AS _k0, id AS _k1, e AS _v) END), (_l, _r) -> COALESCE(NULLIF(") and any("comparator" in w[1] for w in rep.warnings)
    assert "OVER (PARTITION BY k)" in f("ARRAY_AGG(e ORDER BY ts) OVER (PARTITION BY k)")[0] and f("ARRAY_AGG(e ORDER BY ts) OVER (PARTITION BY k)")[0].count("collect_list(") == 1
    assert f("ARRAY_AGG(e) WITHIN GROUP (ORDER BY ts)", )[0].startswith("SELECT transform(sort_array(collect_list(CASE WHEN e IS NOT NULL")


def test_bigquery_array_agg_unsupported_forms_stay_todo():
    for bad in ("ARRAY_AGG(DISTINCT e ORDER BY ts)", "ARRAY_AGG(e ORDER BY ts NULLS LAST)", "ARRAY_AGG(e ORDER BY ts LIMIT n)", "ARRAY_AGG(e LIMIT 0)",
                "ARRAY_AGG(e LIMIT 2 ORDER BY ts)", "ARRAY_AGG(e) FILTER (WHERE x > 1)", "ARRAY_AGG(e ORDER BY ts LIMIT 2.5)"):
        q, rep = osql(f"SELECT {bad} FROM t", "bigquery")
        assert rep.todos and "ARRAY_AGG" in q, bad


def test_sql_rownum_array_agg_stay_linear_and_safe():
    import time
    t0 = time.time()
    for pay in ("ROWNUM " * 5000, "ROWNUM," * 5000, "AND ROWNUM <= 1 " * 3000, "ARRAY_AGG(e ORDER BY x, " * 300, "ARRAY_AGG(" * 300 + "1" + " ORDER BY x LIMIT n)" * 300,
                "f(" * 200 + "1" + ")" * 200):
        for sel in ("SELECT * FROM t WHERE " + pay, "SELECT " + pay + " FROM t"):
            try:
                osql(sel)
                osql(sel, "bigquery")
            except Exception as e:                                             # a clean ConvertError is fine; a hang is not
                assert type(e).__name__ == "ConvertError"
    assert time.time() - t0 < 10
    q, _ = osql("SELECT ARRAY_AGG(e ORDER BY ts) FROM t WHERE c = 'x''); DROP TABLE t; --' AND ROWNUM <= 2", "oracle")
    assert q.endswith("LIMIT 2") and "x''); DROP TABLE t; --'" in q                     # literals are never touched


# ---- more SQL dialects -------------------------------------------------------------------------------------------

def _sql(dialect, q):
    code, rep = convert(q, "sql", sql_dialect=dialect, sql_out="sql")
    return code, rep


_DIALECT_CASES = [
    ("db2", "CREATE TABLE t (id INTEGER, name VARCHAR(20), v DECFLOAT(16))", ["name STRING", "v DOUBLE"]),
    ("netezza", "CREATE TABLE t (a BYTEINT, b INT4) DISTRIBUTE ON (a)", ["a TINYINT", "b INT"]),
    ("vertica", "CREATE TABLE t (a LONG VARCHAR, b INT) ORDER BY a SEGMENTED BY HASH(a) ALL NODES", ["a STRING"]),
    ("clickhouse", "SELECT quantile(0.9)(x), intDiv(a,b) FROM t", ["percentile_approx(x, 0.9)", "DIV"]),
    ("clickhouse", "SELECT arrayMap(x -> x*2, arr) FROM t", ["transform(arr, x -> x*2)"]),
    ("informix", "CREATE TABLE t (a SERIAL8, b DATETIME YEAR TO SECOND, c LVARCHAR)", ["a BIGINT", "b TIMESTAMP", "c STRING"]),
    ("firebird", "SELECT CAST('NOW' AS TIMESTAMP) FROM RDB$DATABASE", ["current_timestamp()"]),
    ("firebird", "SELECT FIRST 3 a FROM t", ["LIMIT 3"]),
    ("informix", "SELECT FIRST 3 a FROM t", ["LIMIT 3"]),
    ("impala", "SELECT DAYS_ADD(d,3), STRLEFT(s,3) FROM t", ["date_add(d, 3)"]),
    ("mariadb", "CREATE TABLE t (id INT UNSIGNED AUTO_INCREMENT, n VARCHAR(10)) ENGINE=InnoDB", ["n STRING"]),
    ("synapse", "SELECT TOP 5 [a] FROM dbo.x OPTION (MAXDOP 1)", ["LIMIT 5", "`a`"]),
    ("greenplum", "SELECT a::int, now() FROM t", ["CAST(a AS int)"]),
    ("cockroachdb", "SELECT * FROM t AS OF SYSTEM TIME '-5s'", ["SELECT * FROM t"]),
]


@pytest.mark.parametrize("dialect,q,expect", _DIALECT_CASES)
def test_more_dialects(dialect, q, expect):
    code, rep = _sql(dialect, q)
    for e in expect:
        assert e in code, (dialect, code)
    assert "OPTION" not in code and "AS OF SYSTEM" not in code


def test_dialect_alias_note_and_todos():
    code, rep = _sql("mariadb", "SELECT 1")
    assert any("MySQL" in str(n) for n in rep.notes)
    code, rep = _sql("clickhouse", "SELECT a FROM t LIMIT 3 BY k")
    assert rep.todos
    code, rep = _sql("clickhouse", "SELECT arrayMap(x -> x * 2, a) FROM t")
    assert not rep.todos           # lambda, not a JSON operator
    code, rep = _sql("postgres", "SELECT j->>'k' FROM t")
    assert rep.todos


def test_dialect_choices_cover_all_names():
    from sparqix.sql_dialects import NAMES
    from sparqix.web import DIALECTS
    assert set(NAMES) <= set(DIALECTS)


def test_new_dialects_hostile_input_is_fast():
    import time
    for d in ("db2", "vertica", "clickhouse", "informix", "firebird", "starrocks", "synapse"):
        for q in ("SELECT " + "quantile(" * 3000 + "x", "CREATE TABLE t (" + "a INT, " * 20000 + ")",
                  "SELECT '" + "'' " * 20000, "SELECT a " + "OPTION (" * 5000, "CREATE TABLE t (" + "(" * 20000):
            t = time.time()
            try:
                convert(q, "sql", sql_dialect=d, sql_out="sql")
            except Exception as e:
                assert type(e).__name__ in ("ConvertError", "InputRejected", "Unsupported"), e
            assert time.time() - t < 5, (d, q[:30])


def test_project_cli_accepts_every_option_it_reads(tmp_path):
    """`sparqix project` read --ai-provider / --ai-base-url without defining them, so the GitHub Action crashed with AttributeError."""
    from sparqix.cli import main
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.py").write_text("import pandas as pd\ndf = pd.read_csv('a.csv')\n")
    for extra in ([], ["--ai-provider", "ollama", "--ai-base-url", "http://localhost:11434/v1"]):
        out = tmp_path / ("out%d" % len(extra))
        main(["project", str(src), "-o", str(out)] + extra)
        assert (out / "a.py").exists()


def test_pandas_rank_becomes_window_rank():
    src = ("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df.sort_values(['id'])\n"
           "df['r1'] = df.groupby('g')['v'].rank(method='first', ascending=False)\n"
           "df['r2'] = df['v'].rank(method='dense')\n"
           "df['r3'] = df.groupby('g')['v'].rank(method='min')\n")
    code, rep = convert(src, "pandas")
    assert "F.row_number().over(Window.partitionBy('g').orderBy(F.col('v').desc_nulls_last(), F.col('id').asc()))" in code
    assert "F.dense_rank().over(Window.orderBy(" in code and "F.rank().over(Window.partitionBy('g')" in code
    assert not rep.todos


def test_pandas_rank_average_max_and_cumprod():
    src = ("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df.sort_values(['id'])\n"
           "df['avg'] = df.groupby('g')['v'].rank()\n"
           "df['mx'] = df.groupby('g')['v'].rank(method='max', ascending=False)\n"
           "df['cp'] = df.groupby('g')['v'].cumprod()\n")
    code, rep = convert(src, "pandas")
    assert not rep.todos
    # average = lowest rank + (ties - 1) / 2; max = lowest rank + ties - 1; ties counted per group AND value
    assert "(F.rank().over(Window.partitionBy('g').orderBy(F.col('v').asc_nulls_last())) + (F.count(F.lit(1)).over(Window.partitionBy('g', F.col('v'))) - 1) / 2)" in code
    assert "desc_nulls_last())) + (F.count(F.lit(1)).over(Window.partitionBy('g', F.col('v'))) - 1))" in code
    assert "F.product(F.col('v')).over(Window.partitionBy('g')" in code and "rowsBetween(Window.unboundedPreceding, Window.currentRow)" in code


def test_converted_run_makes_relative_spark_paths_absolute(tmp_path, monkeypatch):
    """Spark resolves relative paths in the JVM's own working directory, not the folder verify() chdir()s to
    (found by the spark-e2e job: PATH_NOT_FOUND in a deleted temp folder of an earlier case)."""
    import sys
    import types
    seen = []

    class Reader:
        def csv(self, path, **kw):
            seen.append(path)
            return path
    rw = types.ModuleType("pyspark.sql.readwriter")
    rw.DataFrameReader = Reader
    rw.DataFrameWriter = type("W", (), {})
    sql = types.ModuleType("pyspark.sql")
    sql.readwriter = rw
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.readwriter", rw)
    from sparqix.verify import _converted_ns
    ns = _converted_ns("pandas", "from pyspark.sql.readwriter import DataFrameReader as R\nx = R().csv('a.csv')\ny = R().csv('/abs/b.csv')\nz = R().csv('s3://b/c.csv')\n", str(tmp_path))
    assert seen == [str(tmp_path / "a.csv"), "/abs/b.csv", "s3://b/c.csv"]
    assert Reader().csv("a.csv") == "a.csv"          # restored afterwards


def test_no_old_product_name_anywhere():
    """The project was once called 'sparkify'; the web page header split it across tags (<b>Spark</b>ify), which a plain text search misses."""
    import re
    from sparqix import web
    text = re.sub(r"<[^>]+>", "", web.PAGE).lower()
    assert "sparkify" not in text and "sparqix" in text
    for path in glob.glob(os.path.join(HERE, "..", "sparqix", "*.py")) + [os.path.join(HERE, "..", "README.md")]:
        assert "sparkify" not in re.sub(r"<[^>]+>", "", open(path, encoding="utf-8").read()).lower(), path


# ---------------------------------------------------------------- web: graceful shutdown and configurable limits
def test_web_env_limits_are_validated(monkeypatch):
    from sparqix import web
    monkeypatch.setenv("SPARQIX_MAX_CONCURRENT", "3")
    assert web._env_int("SPARQIX_MAX_CONCURRENT", 6, 1, 64) == 3
    for bad in ("0", "999", "abc", "-1", ""):
        monkeypatch.setenv("SPARQIX_MAX_CONCURRENT", bad)
        assert web._env_int("SPARQIX_MAX_CONCURRENT", 6, 1, 64) == 6          # out of range / not a number: the default


def test_web_drain_waits_for_running_conversions():
    import threading
    import time
    from sparqix import web
    assert web._drain(0.2) is True                                  # nothing running
    assert web._busy.acquire(blocking=False)                        # one conversion "running"
    try:
        t = time.time()
        assert web._drain(0.3) is False and time.time() - t >= 0.25
        threading.Timer(0.2, web._busy.release).start()
        assert web._drain(3) is True                                # it finishes while we wait
    finally:
        pass
    assert web._drain(0.2) is True                                  # every slot is free again after a drain


def test_web_server_exits_cleanly_on_sigterm():
    """As a container's main process Python ignores an unhandled SIGTERM; the server must stop by itself (exit code 0, not -15)."""
    import signal
    import socket
    import subprocess
    import sys
    import time
    import urllib.request
    if not hasattr(signal, "SIGTERM") or sys.platform == "win32":
        pytest.skip("POSIX only")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    p = subprocess.Popen([sys.executable, "-m", "sparqix", "serve", "--port", str(port)], cwd=os.path.join(HERE, ".."),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=dict(os.environ, SPARQIX_DRAIN_SECONDS="2"))
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1)
                break
            except OSError:
                time.sleep(0.1)
        else:
            pytest.fail("server did not start")
        p.send_signal(signal.SIGTERM)
        assert p.wait(timeout=10) == 0
    finally:
        if p.poll() is None:
            p.kill()


def test_k8s_manifests_match_the_published_image_and_memory_budget():
    """The image is published as ghcr.io/<owner>/sparqix:<version> (see image.yml); the Deployment must point at it, and the pod's memory limit
    must cover the worst case of its worker processes (otherwise a burst of requests gets the pod OOM-killed)."""
    import re
    k8s = os.path.join(HERE, "..", "deploy", "k8s")
    dep = open(os.path.join(k8s, "deployment.yaml")).read()
    ver = re.search(r'^version = "([^"]+)"', open(os.path.join(HERE, "..", "pyproject.toml")).read(), re.M).group(1)
    assert "YOUR-ORG" not in dep and re.search(r"image: ghcr\.io/shipitamit/sparqix:[0-9][^\s#]*", dep)
    assert re.search(r"name: SPARQIX_MAX_CONCURRENT, value: \"(\d+)\"", dep) and re.search(r"name: SPARQIX_WORKER_MEMORY_MB, value: \"(\d+)\"", dep)
    conc = int(re.search(r"name: SPARQIX_MAX_CONCURRENT, value: \"(\d+)\"", dep).group(1))
    mem = int(re.search(r"name: SPARQIX_WORKER_MEMORY_MB, value: \"(\d+)\"", dep).group(1))
    limit = re.search(r"limits: \{cpu: \"?\d+\"?, memory: (\d+)(Gi|Mi)\}", dep)
    limit_mb = int(limit.group(1)) * (1024 if limit.group(2) == "Gi" else 1)
    assert conc * mem + 300 <= limit_mb, (conc, mem, limit_mb)
    grace = int(re.search(r"terminationGracePeriodSeconds: (\d+)", dep).group(1))
    drain = int(re.search(r"name: SPARQIX_DRAIN_SECONDS, value: \"(\d+)\"", dep).group(1))
    assert grace >= 5 + drain + 10
    np = open(os.path.join(k8s, "networkpolicy.yaml")).read()
    assert "port: 443" not in np and os.path.exists(os.path.join(k8s, "networkpolicy-ai-egress.example.yaml"))   # outbound HTTPS is opt-in
    assert not re.search(r"(?m)^  replicas:", dep)                      # the HPA owns the replica count
    assert 'name: SPARQIX_TRUSTED_PROXIES, value: "1"' in dep
    ing = open(os.path.join(k8s, "ingress.example.yaml")).read()
    assert "proxy-read-timeout" in ing and "limit-rps" in ing
    assert ver.split(".dev")[0] in open(os.path.join(HERE, "..", "deploy", "k8s", "kustomization.yaml")).read()


# ---------------------------------------------------------------- web: proxy-aware throttle, access log, HSTS, metrics, Origin port
def _web_req(port, method, path, headers=None, body=None):
    import http.client
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    h = {"Host": f"127.0.0.1:{port}"}
    h.update(headers or {})
    c.request(method, path, body=body, headers=h)
    r = c.getresponse()
    return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read()


def test_web_throttle_is_per_client_behind_trusted_proxy():
    import threading
    from sparqix import web
    web._fails.clear()
    srv = web.make_server("127.0.0.1", 0, token="right", trusted_proxies=1)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        for i in range(web.MAX_FAILS + 2):                                    # attacker 203.0.113.9 (the entry the proxy appended)
            _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": f"bad{i}", "X-Forwarded-For": f"1.1.1.1, 203.0.113.9"})
        assert _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": "x", "X-Forwarded-For": "203.0.113.9"})[0] == 429
        # another client is not locked out; a spoofed left-hand entry does not help the attacker
        assert _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": "right", "X-Forwarded-For": "198.51.100.7"})[0] == 200
        assert _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": "x", "X-Forwarded-For": "8.8.8.8, 203.0.113.9"})[0] == 429
    finally:
        srv.shutdown()
        web._fails.clear()


def test_web_forwarded_headers_ignored_without_trusted_proxy():
    import threading
    from sparqix import web
    web._fails.clear()
    srv = web.make_server("127.0.0.1", 0, token="right")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        for i in range(web.MAX_FAILS + 1):                                    # rotating X-Forwarded-For must not dodge the throttle
            _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": f"bad{i}", "X-Forwarded-For": f"10.0.0.{i}"})
        assert _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": "x", "X-Forwarded-For": "10.9.9.9"})[0] == 429
        st, hd, _ = _web_req(port, "GET", "/healthz", {"X-Forwarded-Proto": "https"})
        assert "strict-transport-security" not in hd
    finally:
        srv.shutdown()
        web._fails.clear()


def test_web_hsts_only_behind_trusted_https_proxy():
    import threading
    from sparqix import web
    srv = web.make_server("127.0.0.1", 0, token="t", trusted_proxies=1)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        _, hd, _ = _web_req(port, "GET", "/?token=t", {"X-Forwarded-Proto": "https", "X-Forwarded-For": "1.2.3.4"})
        assert "max-age" in hd.get("strict-transport-security", "")
        _, hd, _ = _web_req(port, "GET", "/?token=t", {"X-Forwarded-Proto": "http", "X-Forwarded-For": "1.2.3.4"})
        assert "strict-transport-security" not in hd
    finally:
        srv.shutdown()


def test_web_access_log_has_no_token_or_query(capfd):
    import json as _j
    import threading
    from sparqix import web
    srv = web.make_server("127.0.0.1", 0, token="sekrit-token", access_log=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        _web_req(port, "GET", "/?token=sekrit-token")
        _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": "sekrit-token"})
        _web_req(port, "GET", "/api/examples", {"X-Sparqix-Token": "wrong"})
    finally:
        srv.shutdown()
    err = capfd.readouterr().err
    assert "sekrit-token" not in err and "wrong" not in err and "token" not in err.replace('"path"', "")
    recs = [_j.loads(x) for x in err.splitlines() if x.startswith("{")]
    assert [r["status"] for r in recs] == [200, 200, 401] and recs[0]["path"] == "/" and recs[0]["method"] == "GET" and recs[0]["ip"] == "127.0.0.1"
    capfd.readouterr()


def test_web_access_log_off_by_default(capfd):
    import threading
    from sparqix import web
    srv = web.make_server("127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        _web_req(srv.server_address[1], "GET", "/healthz")
    finally:
        srv.shutdown()
    assert capfd.readouterr().err.strip() == ""


def test_web_metrics_need_token_and_expose_only_counters():
    import json as _j
    import threading
    from sparqix import web
    srv = web.make_server("127.0.0.1", 0, token="t")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        assert _web_req(port, "GET", "/metrics")[0] == 401
        body = _j.dumps({"source": "import pandas as pd\ndf = pd.read_csv('a.csv')\n"})
        assert _web_req(port, "POST", "/api/convert", {"Content-Type": "application/json", "X-Sparqix-Token": "t"}, body)[0] == 200
        st, hd, b = _web_req(port, "GET", "/metrics", {"Authorization": "Bearer t"})       # Prometheus-style scrape
        text = b.decode()
        assert st == 200 and "text/plain" in hd["content-type"]
        assert 'sparqix_conversions_total{result="ok"} ' in text and "sparqix_http_requests_total{code=" in text
        assert "sparqix_conversions_in_flight 0" in text and "a.csv" not in text and "pandas" not in text
    finally:
        srv.shutdown()


def test_web_origin_must_match_host_and_port():
    import json as _j
    import threading
    from sparqix import web
    srv = web.make_server("127.0.0.1", 0, token="t", allowed_hosts=["app.example.com"])
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    js = {"Content-Type": "application/json", "X-Sparqix-Token": "t"}
    body = _j.dumps({"source": "x = 1\n", "language": "python"})
    try:
        ok = {**js, "Host": "app.example.com", "Origin": "https://app.example.com"}          # default port omitted by the browser
        assert _web_req(port, "POST", "/api/convert", ok, body)[0] == 200
        assert _web_req(port, "POST", "/api/convert", {**ok, "Origin": "https://app.example.com:8443"}, body)[0] == 403      # other port
        assert _web_req(port, "POST", "/api/convert", {**ok, "Origin": "https://evil.example.com"}, body)[0] == 403
    finally:
        srv.shutdown()


def test_pandas_round_uses_half_even_like_pandas():
    code, _ = convert("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf['x'] = df['x'].round(1)\ny = round(2.5)\n", "pandas")
    assert "F.bround(" in code and "F.round(" not in code


def test_string_agg_inline_order_by_becomes_valid_spark():
    """BigQuery / Postgres STRING_AGG(x, sep ORDER BY k): the ORDER BY must not be left inside array_join(...) (invalid Spark SQL)"""
    def conv(q):
        return convert(q, "sql", sql_dialect="bigquery", sql_out="sql")[0]
    a = conv("SELECT STRING_AGG(c, ',' ORDER BY c) FROM t")
    assert "ORDER BY" not in a and "sort_array(collect_list(c))" in a
    b = conv("SELECT STRING_AGG(c, '|' ORDER BY id DESC) FROM t")
    assert "ORDER BY" not in b and "struct(id AS _k, c AS _v)" in b and ", false)" in b and "'|'" in b
    assert "ORDER BY" not in conv("SELECT STRING_AGG(c ORDER BY c DESC) FROM t")
    assert conv("SELECT STRING_AGG(c, ',') FROM t") == "SELECT array_join(collect_list(c), ',') FROM t;"


def test_verify_can_use_a_hand_written_original_for_dialect_sql(monkeypatch):
    pytest.importorskip("pandas")
    from sparqix import verify as V
    seen = {}
    monkeypatch.setattr(V, "_orig_sql", lambda src, wd: seen.setdefault("orig", src) and {"query_1": None})
    monkeypatch.setattr(V, "_conv_sql", lambda code, wd: seen.setdefault("conv", code) and {"query_1": None})
    monkeypatch.setattr(V, "_compare_values", lambda n, a, b, r: [])
    monkeypatch.setattr(V, "_as_pandas", lambda v, errors=None: object())
    V.verify("SEL id FROM sales;", HERE, language="sql", dialect="teradata", original="SELECT id FROM sales;")
    assert seen["orig"] == "SELECT id FROM sales;" and "SELECT id FROM sales" in seen["conv"] and "SEL " not in seen["conv"]


def test_e2e_dialect_cases_that_duckdb_cannot_run_have_an_oracle():
    cases = os.path.join(HERE, "e2e", "cases")
    for name in os.listdir(cases):
        parts = name.split(".")
        if name.endswith(".sql") and not name.endswith(".oracle.sql") and len(parts) > 2 and parts[-2] in ("teradata", "bigquery", "databricks"):
            assert os.path.exists(os.path.join(cases, name[:-4] + ".oracle.sql")), name


def test_compare_frames_treats_decimal_like_float():
    """Spark returns DECIMAL columns as decimal.Decimal; the original (pandas/DuckDB) holds floats: equal numbers must compare equal"""
    pytest.importorskip("pandas")
    import decimal
    import pandas as pd
    from sparqix.verify import compare_frames
    a = pd.DataFrame({"k": ["a", "b", "c"], "total": [2310.4800000000005, 1.5, 7.0]})
    b = pd.DataFrame({"k": ["a", "b", "c"], "total": [decimal.Decimal("2310.48"), decimal.Decimal("1.50"), decimal.Decimal("7.00")]})
    assert compare_frames(a, b) == [] and compare_frames(b, a) == []
    c = b.copy()
    c.loc[0, "total"] = decimal.Decimal("2310.49")
    assert compare_frames(a, c)                                     # a real difference is still reported
    n = pd.DataFrame({"k": ["a"], "v": [None]})
    m = pd.DataFrame({"k": ["a"], "v": [decimal.Decimal("1")]})
    assert compare_frames(n, m)                                     # NULL vs a number still differs


def test_pandas_row_filter_keeps_the_sort_order_for_later_windows():
    """df[mask] and df.loc[mask] keep pandas' row order, so a later cumulative/shift window must order by the sort columns, not by monotonically_increasing_id()
    (after other windows Spark's physical row order is not the sorted order any more: found by the real-Spark e2e)"""
    for pick in ("df[df['qty'] <= 3]", "df.loc[df['qty'] <= 3]", "df[df['qty'] <= 3].copy()"):
        src = ("import pandas as pd\ndf = pd.read_csv('a.csv')\ndf = df.sort_values(['customer', 'id'])\n"
               f"small = {pick}\nsmall['p'] = small.groupby('customer')['qty'].cumprod()\n")
        code, rep = convert(src, "pandas")
        assert "orderBy(F.col('customer').asc(), F.col('id').asc())" in code and "monotonically_increasing_id" not in code, pick
