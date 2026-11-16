# sparqix

Convert legacy data code into **Apache Spark**: **pandas, Polars, Dask, R/dplyr, SAS, Jupyter notebooks, dbt models, Excel formulas, Scala/Java collections,
plain Python, 40 SQL dialects (including Teradata, DB2, Netezza, Vertica, ClickHouse, HANA), stored procedures (T-SQL, PL/SQL, Teradata SPL)** and **ETL tools (Informatica, Talend, Alteryx, Pentaho,
Pig, SSIS, DataStage, Hive, Teradata BTEQ/FastLoad/MultiLoad/TPT)**.
Zero dependencies (stdlib only). Output always parses; anything it cannot translate safely is left as a
`# TODO(sparqix)` comment with the original code, never silently converted wrongly.

## Install

```bash
pip install sparqix           # from PyPI (Python 3.9+)
sparqix --help

pip install .                 # from a checkout, or run in place: python -m sparqix ...
```

## At a glance

| You want to | Use |
|---|---|
| Convert one file (language auto-detected) | `sparqix FILE` |
| Convert a whole folder in dependency order | `sparqix project DIR -o OUT` ([Whole projects](#whole-projects)) |
| Target Databricks, Delta/Unity, Gluten/Velox, Comet, Fabric or Photon | `--target …`, `--delta`, `--catalog` |
| Check the result on sample data | `sparqix verify` ([Verify](#verify-run-and-compare)) |
| Convert schedulers/orchestration | `sparqix orchestrate` |
| Get model suggestions for the TODOs | `--ai`, with any provider ([Model-assisted suggestions](#model-assisted-suggestions---ai)) |
| Let a model convert, fix and migrate with guard rails | `sparqix agent fix\|migrate\|chat` ([Agentic mode](#agentic-mode)) |
| Use a browser UI | `sparqix serve` ([Web UI](#web-ui-sparqix-serve)) |
| Run in CI or on Kubernetes | [GitHub Action](#github-action), [`deploy/`](deploy/README.md) |

The AI features are optional and provider-agnostic (Anthropic, OpenAI, Gemini, Mistral, Groq, Together, OpenRouter, Ollama or any OpenAI-compatible server).
Nothing leaves your machine unless you turn them on.

```bash
python -m sparqix examples/sales.py                      # pandas -> PySpark DataFrame API
python -m sparqix examples/report_tsql.sql --dialect tsql    # SQL Server -> spark.sql(...)
python -m sparqix q.sql --dialect oracle --sql-out sql   # plain Spark SQL
python -m sparqix examples/wordcount.py                  # Python collections -> RDD
python -m sparqix examples/events_polars.py              # Polars -> PySpark
python -m sparqix examples/events_dask.py                # Dask -> PySpark
python -m sparqix examples/analysis.R                    # R / dplyr -> PySpark
python -m sparqix examples/sales.sas                     # SAS DATA step + PROCs -> PySpark
python -m sparqix examples/orders.dbt.sql --dialect tsql # dbt model (Jinja) -> spark.sql + temp view
python -m sparqix examples/formulas.formulas --excel-columns "B=price,C=qty"   # Excel formulas -> withColumn
python -m sparqix examples/Demo.scala                    # Scala collections -> RDD
python -m sparqix examples/Streams.java                  # Java streams -> JavaRDD
python -m sparqix examples/demo.ipynb -o demo_spark.ipynb  # notebook -> notebook (shared state across cells, %%sql cells)
python -m sparqix examples/sales_macro.tdsql --dialect teradata   # Teradata SQL + macros -> PySpark functions
python -m sparqix examples/orders.fastload               # Teradata FastLoad / MultiLoad / TPT -> PySpark load functions
python -m sparqix in.py -o out.py --strict               # exit 2 if TODOs remain (CI)
```
Library: `from sparqix import convert; code, report = convert(src, "auto")`

## What converts
| Input | Output | Covers |
|---|---|---|
| pandas | PySpark DataFrame | read/write csv/parquet/json, filter, `df[cols]`, `loc`, `assign`, `df['c']=`, groupby+agg (dict / named / `size`), merge/concat, sort, drop/dropna/fillna/rename/astype, `.str`/`.dt`, `np.where/select`, `value_counts`, `pivot_table`, `query`, `inplace=True`, scalar aggregates |
| SQL | Spark SQL | tsql / oracle / mysql / postgres / snowflake / bigquery / redshift / duckdb / hive / trino / presto / sqlite / teradata: quoting, TOP/FETCH/ROWNUM (`WHERE ROWNUM <= n` and select-list `ROWNUM`)/LIMIT a,b, ISNULL/NVL/DECODE/IIF, DATEADD/DATEDIFF/DATEPART, TO_CHAR/DATE_FORMAT formats, `::` casts, type names, STRING_AGG/GROUP_CONCAT/LISTAGG, CONVERT |
| Polars | PySpark DataFrame | scan/read, filter, select, with_columns, when/then/otherwise, group_by().agg, sort, join, str/dt namespaces, `.over()` windows, write_* |
| Dask | PySpark DataFrame | pandas rules + `.compute()/.persist()` removal, `repartition` |
| R (dplyr) | PySpark DataFrame | pipes (`%>%`, `\|>`), filter/mutate/select/rename/arrange/group_by/summarise/count/distinct, joins (incl. `by=c(a="b")`), case_when/ifelse, grouped mutate -> windows, `df$col <-` |
| Notebook | .ipynb | converts code cells in order, keeps markdown, strips magics, `%%sql` cells, per-cell TODO locations |
| SAS | PySpark DataFrame | DATA step (set/where/keep/drop/rename, assignments, IF/ELSE IF chains, delete), PROC SORT/MEANS/SUMMARY/FREQ/PRINT/IMPORT/EXPORT/SQL, SAS functions (upcase, substr, round, put/input common formats...) |
| dbt | Spark SQL / PySpark | ref(), source(), config(), `is_incremental()` blocks, literal `var()` defaults; unresolved Jinja flagged |
| Excel formulas | PySpark columns | IF/IFS/SWITCH, SUM/AVERAGE/..., SUMIF(S)/COUNTIF(S), text, date and math functions, `&`, lookups (`VLOOKUP`, `XLOOKUP`, `INDEX`+`MATCH`) |
| Scala | Scala RDD | List/Seq/Set/Array/range/Source.fromFile (also straight in a chain) -> RDD; map/filter/flatMap/collect{case}/groupBy.mapValues(size)/reduceByKey/mapValues/sorted+sortBy(.reverse)/find/forall/exists/headOption/drop/foldLeft/sum/... |
| Java | JavaRDD | `list.stream()` / `Files.lines(...)` chains: filter/map/flatMap/sorted(+Comparator)/skip/limit/reduce/min/max/any-all-noneMatch/findFirst/forEach, Collectors toList/toSet/joining/counting/summingX/toMap/groupingBy(+counting/summingX); method references |
| Python | PySpark RDD | list/dict comprehensions, map/filter/sorted/set, sum/len/min/max/any/all/Counter, accumulate / count / append loops, nested loops (flatMap), open()/csv.DictReader sources |

## Honest limits
Short version: [docs/limits.md](https://github.com/shipitamit/sparqix/blob/main/docs/limits.md). Details below.

* Not every construct can be translated mechanically. `apply(axis=1)` with a lambda that is not a plain column expression, positional indexing and index semantics are flagged as TODO.
* pandas: `shift`/`diff`/`pct_change`/`cumsum`/`cummax`/`cummin`/`rolling(n)`/`expanding()` (also per `groupby` and in `groupby().transform(lambda ...)`) become `Window` functions,
  ordered by the last `sort_values` (otherwise by current row order, with a warning). `iterrows`/`itertuples` loops that only *read* rows become a driver-side
  `toLocalIterator()` loop (slow, not distributed); loops that write back into the DataFrame stay TODO. `rank(method='min'|'dense'|'first', ascending=...)` (also per `groupby`) becomes `rank`/`dense_rank`/`row_number` over a window (nulls stay null, result is a double like pandas). Still TODO: `rank()` with the default `method='average'` or `'max'`, `cumprod`, `ewm`, time-based or centered rolling windows, `rolling().median()/count()`.
* SQL: Oracle `ROWNUM <= n` / `< n` / `= 1` (at any query level, also next to other WHERE conditions) becomes `LIMIT`; a `ROWNUM` in the select list becomes
  `row_number() OVER (ORDER BY monotonically_increasing_id())` (pagination with `ROWNUM rn ... WHERE ROWNUM <= n` keeps its meaning through a filter on the alias).
  Still TODO: ROWNUM next to GROUP BY / DISTINCT / aggregates / OR / UNION at the same level, `ROWNUM > n`, `ROWNUM = n` for n > 1. BigQuery `ARRAY_AGG` with `DISTINCT`, `IGNORE NULLS`,
  `ORDER BY` (any keys, ASC/DESC mixed) and `LIMIT n`, with `WITHIN GROUP` or `OVER (...)`, becomes `collect_list/collect_set` + `sort_array`/`array_sort` over a struct of the keys + `slice`;
  NULL elements are dropped. Still TODO: `NULLS FIRST/LAST`, a non-literal `LIMIT`, `FILTER (WHERE ...)`, `DISTINCT` ordered by another expression.
* Excel: `VLOOKUP` (exact and approximate), `XLOOKUP` (with `if_not_found`) and `INDEX(range, MATCH(key, range, 0))` become left joins through small `_xl_lookup`/`_xl_approx` helpers (first match wins, no match = NULL). The table must be a range on another sheet (`Sheet2!A:C`); each sheet is read with `spark.table(...)` and columns are the sheet letters unless mapped with `--excel-columns "Sheet2.A=sku"`. Excel text matching is case-insensitive, Spark's is not. Still TODO: `HLOOKUP`, `MATCH` alone, `INDEX` by row number, `XLOOKUP` match/search modes, approximate `MATCH`.
* R: `lag`/`lead`/`cumsum`/`cummax`/`cummin`/`row_number`, `slice`/`slice_head`/`slice_tail`/`slice_max`/`slice_min`/`slice_sample` (grouped too), `pivot_longer` and `pivot_wider` convert
  (order from `arrange()`, else current row order with a warning). Selection helpers inside `pivot_longer`/`pivot_wider` (`select()` supports `starts_with`, `ends_with`, `contains`, `everything`, `a:c` and `-x`), `prop=`, several `names_from`/`values_from` columns and `names_prefix` stay TODO.
* Semantics that differ are reported as WARNINGs (e.g. `drop_duplicates` keeps an arbitrary row in Spark,
  `to_csv` writes a directory, `apply` becomes a StringType UDF).
* Output was verified by golden tests and by checking it parses; it has **not** been executed against a live
  Spark cluster (PySpark was not installable where this was built). Run your own data through it before trusting it.
* SAS: RETAIN running sums/counters/carry-forward, FIRST./LAST., LAG/DIF, MERGE with IN= flags, macros (%let, %macro, %if, %do) and DATALINES/INFILE convert. Order-dependent logic uses a hidden `_row` column (`monotonically_increasing_id`), so it assumes the current row order and uses a single-partition window when there is no BY; MERGE is a full outer join (many-to-many and no-BY merges are flagged); macros are expanded at conversion time, so run-time-dependent macro logic stays TODO. Still TODO: LAG inside IF, DO/OUTPUT loops, input pointers (`@1`), other RETAIN assignments. Scala/Java: pattern based
  (collection variables defined from literals/files). The Java output is type-checked with `javac` against stubs of the Spark 3.x `JavaRDD` signatures (`tests/jvm_stubs`), not against real Spark jars; the Scala output has never been compiled (no Scala compiler was available), so read its warnings: RDD `sum()` is a Double, `count()` a Long, `zipWithIndex` a Long index, and `skip`/`drop`/`limit` depend on the RDD's element order. Not converted: `sortWith`, `partition`, `minBy/maxBy`, for-comprehensions, `IntStream.range` roots, `average()`, `partitioningBy`, stream variables kept across statements.
* Teradata (SQL, macros, SPL, FastLoad/MultiLoad/TPT) is best effort and has not been run against a real Teradata system. Things with no exact Spark
  equivalent are TODOs or warnings, not guesses. Newer conversions: `TOP n WITH TIES` (RANK() sub-query; also T-SQL/Redshift), `PERIOD(DATE|TIMESTAMP)` columns (split into `_begin`/`_end`; OVERLAPS/CONTAINS/BEGIN/END rewritten, end exclusive), `SAMPLE WHEN` (random filters per stratum), SPL cursors/`FOR … AS` loops/handlers (cursors are collected to the driver; `UNDO` does not roll back), and MultiLoad/TPT UPDATE/DELETE/upsert as Delta `MERGE` (target must be Delta/Iceberg; last row per key wins; set-based, not MultiLoad's sequential apply). Still TODO: the period algebra (`P_INTERSECT`, `P_NORMALIZE`, …), `SAMPLE WHEN` counts with `SELECT *`, `WITH TIES` inside sub-queries, SQLSTATE/named-condition handlers, `REPEAT`, conditional `APPLY … WHERE`.
* AI suggestions and the agent are unverified: patches are checked for syntax and safety, not correctness. The non-Anthropic providers were tested
  against a local stub server, not live APIs.
* Related tools exist: `pyspark.pandas` (run pandas API on Spark), SQLGlot (SQL transpiling). sparqix differs by
  emitting native DataFrame code and handling many source languages and ETL tools in one CLI.

## Tests
`python -m pytest -q` (234 pass; 6 are skipped: 5 orchestration examples covered by their own tests, and 1 that needs PySpark installed)

## BigQuery and Informatica

**More SQL dialects** (`--dialect NAME`): *own rules* – `db2`, `netezza`, `vertica`, `clickhouse`, `hana`, `informix`, `firebird`, `impala`
(e.g. DB2 `d + 3 DAYS`, `FETCH FIRST n ROWS`, Netezza `ISNULL`/`NOTNULL`, Vertica `DATEDIFF('day',..)`/projections, ClickHouse `quantile(0.9)(x)`,
`PREWHERE`, `toStartOfMonth`, Informix `SELECT FIRST n`/`UNITS`, Firebird `CAST('NOW' AS TIMESTAMP)`, Impala `DAYS_ADD`/`STRLEFT`);
*aliases* that reuse a family's rules plus their clean-ups – MySQL (`mariadb singlestore starrocks doris tidb`), PostgreSQL
(`greenplum cockroachdb yugabyte alloydb timescaledb`), T-SQL (`synapse sybase azuresql fabric_warehouse`), Trino (`athena starburst`).
Aliases emit a note saying which family was used. Column types in `CREATE TABLE` are mapped; storage/distribution clauses are dropped with a warning;
syntax with no safe equivalent (`LIMIT n BY`, `SKIP`) is a TODO. Not run against real engines.

**BigQuery** – SQL dialect (`--dialect bigquery`: SAFE_CAST, DATE_TRUNC, UNNEST, STRUCT, `@param`,
partition clauses, backtick table refs) and client code (`google.cloud.bigquery`, `pandas_gbq`),
converted to the spark-bigquery connector (`format('bigquery')`, writes need `temporaryGcsBucket`).

**Informatica** – `--from powercenter` (mapping XML export) and `--from idmc` (Cloud/IDMC mapping JSON),
both auto-detected. Each is read into a neutral graph (source, filter, expression, joiner, lookup,
router, aggregator, sequence, target …) and emitted as one PySpark function per mapping
(`m_name(spark, params=None, write=True)`). Unsupported transformations raise at runtime and are
listed as TODOs.

Caveats: the PowerCenter reader follows the documented XML element names but has only been tested
on a hand-authored example (`examples/m_orders_fact.xml`). The IDMC reader is best-effort because the
export schema is not publicly documented. Nothing has been run on a real Spark cluster. Send a real
export so the readers can be hardened.

## Databricks

`--target databricks` wraps any Python/SQL result as a Databricks notebook (source `.py` with
`# COMMAND ----------` cells, or `.sql` with `--sql-out sql`), reusing the notebook's `spark`.
`--from databricks` reads such notebooks: pandas and non-Spark `%sql` cells are converted cell by cell,
Spark-native cells, `dbutils` and `display` are kept as-is. Not tested on a real workspace.

## Verify (run and compare)

`sparqix verify script.py --trust --data ./sample [--vars out,merged] [--rtol 1e-6]` runs the original pandas
script and its converted PySpark version on the same sample files and diffs every resulting
DataFrame/number (ignoring row and column order, with float tolerance). Needs `pandas` and `pyspark`
installed. Exit code 0 = all match, 1 = differences, 3 = missing packages, 4 = this language cannot be verified, 5 = the original could not be run.
Series and column names produced by Spark may need `--vars` to line up.

Which languages can be verified depends on whether the **original** can be run next to the converted code:

| Source | The original is run by | Compared |
|---|---|---|
| pandas, Polars, Dask | the script itself (Polars/Dask must be installed) | every DataFrame / Series / number it defines |
| Jupyter notebook | its code cells, in order (`%magics` and `!shell` lines dropped) | same |
| SQL, any dialect (`--dialect` for the conversion) | DuckDB if installed, else SQLite, over the CSV files in `--data` (each file is a table named after it) | every SELECT, as `query_1`, `query_2`, ...; a different SELECT count is a difference |
| R | `Rscript` (R must be installed) | every data.frame and numeric scalar left in the workspace |
| SAS, Scala/Java, Excel, dbt, Informatica/Talend/SSIS/DataStage/Pig/Hive/BTEQ, stored procedures | nothing here can run the original | `verify` exits 4 and says why |

SQL caveat: only SQL that DuckDB/SQLite accepts can be verified (a Teradata `SEL`, Oracle `CONNECT BY` etc. is reported as "the original does not run", not as a mismatch).

## Whole projects

`sparqix project SRC -o OUT [--dialect D] [--sql-out sql] [--target databricks] [--strict]` converts a
folder: every recognised file (.py .sql .ipynb .R .sas .scala .java, PowerCenter .xml, IDMC .json) is
converted into the same relative path under OUT (a `_sas`/`_sql` suffix only if two sources would collide),
and `SPARQIX_REPORT.md` lists per-file status (OK / CHECK / NEEDS REVIEW / FAILED), TODOs, warnings,
local imports between Python files (their DataFrame types change, so check call sites) and skipped files.
One failing file never stops the run. `.git`, `venv`, `node_modules` etc. are ignored.

## Performance pass (`--optimize`)

Also works with `sparqix project`. Conservative and fully reported:
- simple Python UDFs (arithmetic, comparisons, if/else, upper/lower/strip, len, str/int/float, startswith, `in (...)`)
  become built-in column expressions; other UDFs are kept and flagged. Null handling follows Spark, not Python.
- `F.broadcast()` on a join's right side when it is clearly small (limit / aggregate / distinct result) or named in
  `--broadcast a,b`; other joins get a note suggesting it.
- `.cache()` on expensive results (join/aggregate/sort) used by two or more later statements.
- warnings for `collect()/toPandas()/count()` inside loops, `repartition(1)`, `crossJoin`, window without partitionBy.
Not applied to Scala/Java, notebooks, Informatica or plain-SQL output. Speed-ups are not measured here: benchmark on your data.

## pandas API on Spark (`--mode pandas-api`)

For pandas input, `--mode pandas-api` keeps your code almost verbatim and targets `pyspark.pandas`:
`import pandas as pd` becomes `import pyspark.pandas as ps`, `pd.` becomes `ps.` (also in type hints and
`from pandas import ...`). Fastest migration, but less control and often slower than the default PySpark
DataFrame output. Reported: `to_pickle`/`style`/`eval`/`read_pickle`/`chunksize` (not supported: TODO),
driver-side operations (`.values`, `to_numpy`, `iterrows`, `to_pandas`), `apply(axis=1)` (plain column-expression lambdas convert), `np.where`,
`iloc`, directory-style writes. Adds `compute.ops_on_diff_frames` when frames are merged/concatenated.
Works with `--target databricks`, `sparqix project` and `sparqix verify --mode pandas-api`; other
languages are converted to PySpark DataFrames as usual (with a note). Not run against real Spark here.

## Orchestration (`sparqix orchestrate`)

`sparqix orchestrate INPUT [--from airflow|crontab|pc-workflow] [--to databricks|airflow] [--code-root PATH] [--mapping-module M] [-o OUTDIR]`
reads an **Airflow DAG** (classic operators: Bash, SparkSubmit, SQL operators, Empty, Python; `>>`, `set_upstream`,
`chain`, retries, `default_args`, schedule), a **crontab**, or an **Informatica PowerCenter workflow** XML
(sessions, links, start/command tasks) and writes a **Databricks Jobs API** definition (`*.job.json`, plus
extracted `.sql` files and `run_<mapping>.py` entry scripts) or an **Airflow DAG** that runs the converted scripts
on Databricks. Cron is converted to Quartz (day-of-week renumbered); schedules are created PAUSED; `{{ ds }}`
becomes `{{job.start_time.iso_date}}`.
Not converted (reported as TODO/warnings): PythonOperator callables, TaskFlow `@task`, chained shell commands,
templated SQL, link conditions, Informatica command/email/decision tasks and schedulers. Cluster and warehouse ids
are `FILL_ME` placeholders on purpose. Point the generated tasks at scripts converted with `sparqix project`
(same relative paths). Job JSON follows the documented Jobs API 2.1 fields but was not submitted to a workspace.

## Delta Lake and Unity Catalog (`--delta`, `--catalog`, `--schema`)

`--delta`: `.write...parquet/orc(path)` and `saveAsTable` become Delta; reads of a literal path written as Delta use
`format('delta').load`; `STORED AS PARQUET/ORC/AVRO` DDL becomes `USING DELTA`; csv/json outputs are kept
(interchange formats). Writes with extra options and non-literal paths are flagged, not guessed.
`--catalog C [--schema S]`: one/two-part table names become `C.S.table` / `C.schema.table` in `spark.table`,
`saveAsTable`, `insertInto`, `DeltaTable.forName` and inside SQL (also `--sql-out sql`). Temp views, CTEs, 3-part
names, `EXTRACT(.. FROM ..)` and table functions are left alone; f-string SQL is flagged.
Always on for SQL input: `INSERT ... ON CONFLICT (keys) DO UPDATE/NOTHING` becomes `MERGE INTO` (Spark SQL has no ON
CONFLICT); MySQL `ON DUPLICATE KEY UPDATE` is reported as a TODO (the key columns are unknown).
Also works in `sparqix project`. Not applied to Scala/Java/notebook/pandas-API output. Not run on a real workspace.

## Stored procedures and SSIS

**Stored procedures / functions** (`--from proc`, auto-detected from `CREATE [OR REPLACE] PROCEDURE|FUNCTION`):
T-SQL and Oracle PL/SQL become one PySpark function per routine. Parameters/variables are Python variables passed to SQL as
named parameters (`spark.sql(q, args={...})`, Spark 3.4+; values only, not table/column names). Supported: DECLARE/SET/`:=`,
`SELECT @v = ...` / `SELECT ... INTO v`, IF/ELSIF/ELSE, WHILE, LOOP, FOR i IN a..b, FOR rec IN (query) (driver-side, warned),
TRY/CATCH and EXCEPTION blocks, EXEC/calls (OUT params return a dict), PRINT/DBMS_OUTPUT, RAISERROR/THROW/RAISE_APPLICATION_ERROR,
`#temp` tables as temp views, bare SELECT as the returned DataFrame, scalar/table functions. Commit/rollback/SET NOCOUNT are
dropped (Delta statements are atomic). Cursors, dynamic SQL, `@@ROWCOUNT`, LIKE/IN-subquery/CASE inside procedural conditions
become `_todo(...)` calls that raise at runtime, plus a TODO in the report. Transaction semantics across several statements are lost.

**SSIS** (`.dtsx`): control flow only, through `sparqix orchestrate` (Execute SQL, Execute Process, containers, precedence
constraints; non-success constraints are warned). Data Flow Tasks are listed with their components and left as TODOs: rebuild them.
Built from the documented package structure; not tested on real exports.

Not covered: Talend and Alteryx.

## Streaming (`--from stream`, auto-detected)

Converts to Spark Structured Streaming:
- **Kafka consumer loops** (kafka-python `for msg in consumer`, confluent-kafka `poll`): JSON parsing, field access, derived values,
  `if` filters, running dict counters/sums (-> streaming `groupBy` aggregation, outputMode update), `producer.send(topic, json.dumps({...}))`
  (-> Kafka sink with `to_json(struct(...))`) or `print` (-> console). Schema is inferred from the fields the code touches
  (numeric if used in arithmetic/numeric comparison, else string): review it. Per-record side effects (DB writes, HTTP calls)
  cannot be column logic: they are listed as TODOs and the generated job raises `NotImplementedError` until you write the foreachBatch function.
- **Directory polling loops** (`while True: listdir/glob -> pandas read -> process -> to_csv/to_parquet -> sleep`): file stream source +
  the pandas body converted with the normal pandas converter + file sink; sleep/seen-sets/moves are dropped (the checkpoint tracks files).
  A schema is not inferred: schema inference is switched on for testing only.
- **Legacy DStream** (StreamingContext): socket/text-file/Kafka sources, flatMap(split)/map/filter/reduceByKey(+)/countByValue,
  pprint/saveAsTextFiles/foreachRDD. Window operations, `updateStateByKey`, joins and unions are TODOs (need event time / state APIs).
Offsets, group ids, ordering and exactly-once behaviour differ from the originals (checkpoint-based). Needs the spark-sql-kafka package for Kafka.
Not run against a real cluster or broker.

## Talend and Alteryx

`sparqix job.item` (Talend job XML) and `sparqix flow.yxmd` (Alteryx workflow) are detected automatically (or `--from talend|alteryx`).
- **Talend:** file/DB input and output, tFilterRow, tMap (lookup joins, output filters, simple Java expressions), tAggregateRow, tSortRow,
  tUniqRow, tUnite, tLogRow. Other Java expressions are kept in `F.expr(...)` with a TODO; reject links, triggers, tJava*, tRunJob are reported.
- **Alteryx:** file input/output, Filter (True/False outputs), Formula, Select (drop/rename), Sort, Summarize, Unique, Union, Join (J/L/R outputs),
  Sample. Alteryx expressions are rewritten to Spark SQL (IF/THEN/ELSE → CASE); unknown functions and tools are TODOs.
Expressions are parsed (not pattern-matched) by `expr_xlate.py` into Column code:
- **Talend Java:** `?:`, `&& || !`, null checks, `+` as concat or addition (decided from the column types in the job metadata, with a warning when
  unknown), casts, String methods (`trim substring equals contains replace indexOf …`), `StringHandling.*`, `Relational.*`, `Math.*`,
  `Integer/Long/Double.parse*`, `TalendDate.formatDate/parseDate/addDate/diffDate/getPartOfDate`, tMap variables (`Var.x`), tFilterRow
  functions and advanced (Java) conditions.
- **Alteryx:** IF/ELSEIF/ELSE, IIF, IN, AND/OR/NOT, string, math and date functions (`DateTimeFormat` strftime patterns become Spark patterns,
  `Substring`/`FindString` 0-based → 1-based), `Regex_Match/Replace`, `Round` with a factor.
- Anything else (unknown routines, field access, non-literal arguments where Spark needs a literal) is a TODO that keeps the original text.
The output is checked for syntax and exact shape in tests, not executed on Spark.

Both readers were written from the documented file structure and tested on hand-written samples, not on real exports: expect to
adjust them against your own jobs (send a scrubbed export and they can be hardened).

## Pentaho, Pig and SSIS data flows

- **Pentaho Kettle (`.ktr`)**: CSV/text and table input, Select Values, Filter Rows (nested conditions, true/false outputs), Group By, Sort Rows,
  Unique, Merge Join, Calculator (arithmetic, copy, constant), Constant, Value Mapper, Append, text/table output.
- **Apache Pig (`.pig`)**: LOAD (PigStorage + schema), FILTER, FOREACH ... GENERATE (also after GROUP), GROUP, JOIN (inner/outer), ORDER,
  DISTINCT, LIMIT, UNION, STORE, DUMP. UDFs, FLATTEN, COGROUP, SPLIT, nested FOREACH and macros are TODOs.
- **SSIS data flows (`.dtsx`)**: each Data Flow Task becomes a function: OLE DB / ADO.NET / flat-file sources, Derived Column and Conditional
  Split (SSIS expression language incl. `?:`, `(DT_xxx)` casts, string/date functions), Data Conversion, Sort, Aggregate, Union All, Merge
  Join, Lookup, destinations. Task order still comes from `sparqix orchestrate FILE.dtsx`.
All three were written from the documented file formats and tested on hand-written samples, not on real exports.

## Teradata

Everything Teradata is handled with `--dialect teradata` or by auto-detection (`REPLACE PROCEDURE`, `BEGIN LOADING`, `.BEGIN MLOAD`, `DEFINE JOB`, `REPLACE MACRO`).

| Teradata | Becomes |
|---|---|
| SQL: SEL/INS/UPD/DEL, TOP n, QUALIFY, SAMPLE, `(FORMAT 'x')` / `CAST(.. FORMAT ..)`, CSUM/MSUM/MAVG/MDIFF/RANK(x DESC), INDEX/OREPLACE/STRTOK/TRYCAST, integer dates (YYYMMDD), `DATE` | Spark SQL (including TOP WITH TIES, PERIOD columns and SAMPLE WHEN); things with no exact equivalent get a warning/TODO instead of a guess (SAMPLE n/fraction is random) |
| DDL: MULTISET/SET/VOLATILE/GLOBAL TEMPORARY, PRIMARY INDEX, FALLBACK, CASESPECIFIC | `CREATE TABLE ... USING parquet` / temp views; physical clauses dropped with a note |
| COLLECT STATISTICS, HELP, SHOW, SET QUERY_BAND, BT/ET, LOCKING | dropped (kept as a comment) |
| Macros: `REPLACE MACRO m (x INT) AS (...;)`, `EXEC m(1)` | a Python function using `spark.sql(sql, args={...})` (bound parameters, Spark 3.4+); `EXEC` with non-literal arguments is a TODO |
| SPL: `REPLACE PROCEDURE`, DECLARE/SET/IF/ELSEIF/WHILE/LEAVE | a Python function (same engine as the Oracle/T-SQL procedures); cursors, `FOR ... AS`, CONTINUE/EXIT/UNDO handlers for SQLEXCEPTION/NOT FOUND convert; `REPEAT` and SQLSTATE handlers are TODOs |
| FastLoad / MultiLoad / TPT scripts (`--from tdload`) | a function per load: reads the file (delimited or fixed width) with a schema and appends to the table; MultiLoad/TPT UPDATE/DELETE/upsert labels become a Delta `MERGE` (a TODO when the keys cannot be mapped); TPT export operators write a delimited file |

Logon details (`LOGON`, `.LOGON`, `TdpId`, `UserName`, `UserPassword`) are never copied into the output. Everything is best effort: it has not been run
against a real Teradata system, so test the generated code on your data and read the TODO list.

## Hive/Impala, Teradata BTEQ, DataStage and job orchestration

- **Hive / Impala scripts (`.hql`)**: each statement runs through `spark.sql()` after the SQL converter. `SET hive.*` / `mapred.*` tuning is
  dropped (noted), user variables (`SET x=..`, `${hiveconf:x}`, `${var:x}`) are substituted at runtime from a `P` dict (fails loudly if unset),
  `COMPUTE STATS` -> `ANALYZE TABLE`, `INVALIDATE METADATA` / `REFRESH` -> `REFRESH TABLE`, `RECOVER PARTITIONS` -> `MSCK REPAIR`, Impala hints
  removed, dynamic-partition overwrite configured. `ADD JAR`, Kudu `UPSERT` and `TRANSFORM ... USING` are TODOs.
- **Teradata BTEQ (`.bteq`)**: `.LOGON/.SET/.LOGOFF` ignored, `.EXPORT` + SELECT written as CSV, `.QUIT n` -> `sys.exit(n)`, `.IF ACTIVITYCOUNT`
  approximated (TODO), `.IMPORT`/`.GOTO`/`.OS` are TODOs, SEL/INS/DEL/COLLECT STATS/MULTISET/PRIMARY INDEX normalised before the Teradata SQL dialect.
- **DataStage (`.dsx`)**: sequential file / data set, database connectors (JDBC with FILL_ME), Transformer (derivations, constraints, stage variables;
  DataStage BASIC expressions via `expr_xlate`), Filter, Sort, Remove Duplicates, Join, Funnel, Copy, Peek. *Best effort*: the .dsx format is not
  publicly specified in detail, so the reader was written from the structure I know and a hand-written sample.
- **Orchestration**: `sparqix orchestrate` now also reads Pentaho jobs (`.kjb`: transformations, SQL, shell, START schedule, hops), Talend parent jobs
  (`tRunJob` subjobs and SUBJOB_OK / RUN_IF / ERROR triggers) and Alteryx workflows (one task: schedules live in Alteryx Server, not in the file).
  Child jobs are tasks that run the script `sparqix project` produces for them (same base name, `.py`). Failure / RUN_IF hops become plain
  dependencies with a warning.

## Web UI (`sparqix serve`)

`python -m sparqix serve [--port 8765] [--host 127.0.0.1] [--open]` starts a local website (standard library only).

- **Convert tab** – paste code or drop/choose a file. Output has line numbers and syntax highlighting; TODO lines are shaded, with
  ▲/▼ buttons to step through them, and clicking a TODO in the report jumps to its line. Options: language, SQL dialect/output,
  Databricks target, pandas API mode, optimize, Delta + catalog/schema, broadcast hints, Excel column names, dbt model name, and a
  *strict* banner (UI only: warns that CI would fail). Options are remembered in the browser (never your code or API key);
  **Copy link** encodes options + input in the URL hash for sharing.
- **AI suggestions** – opt-in. Pick a provider and enter your API key (kept in memory only, never stored), optionally insert suggestions as live
  code, cap the number of blocks, or use *Dry run* to see exactly which prompts would be sent without sending anything.
- **Project tab** – upload several files, a folder or a `.zip` (or drop them). You get a status table (OK / CHECK / NEEDS REVIEW /
  FAILED), click-to-preview of each converted file, a **Download zip** of all outputs and the report as Markdown. Uploads are limited
  (file count, size), and zip entries with absolute paths, `..` or symlinks are rejected.
- **Orchestrate tab** – Airflow DAGs, crontabs, PowerCenter workflows and SSIS packages → Databricks job JSON / Airflow DAG, with
  **Download all (.zip)**.

JSON API: `POST /api/convert`, `/api/project`, `/api/orchestrate`; `GET /api/examples`, `/api/example`. `/?example=sales.py` deep-links
an example. The server only parses text and never runs it; it binds to localhost and has no authentication, so do not expose it to a
network as is.

## GitHub Action

This repository is also a composite GitHub Action (`action.yml`). On a pull request it converts the files changed in the PR, posts (and
updates) one comment with the per-file status, TODOs and warnings, uploads the converted files as an artifact and can fail the job.

```yaml
# .github/workflows/sparqix.yml  (full example: docs/github-workflow.yml)
on: pull_request
permissions: {contents: read, pull-requests: write}
jobs:
  convert:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: YOUR-ORG/sparqix@v1
        with: {path: etl, dialect: tsql, target: databricks, delta: "true", catalog: main, schema: etl, fail-on: failed}
```
Inputs: `path`, `output`, `changed-only` (default true; only on PRs), `dialect`, `sql-out`, `target`, `mode`, `optimize`, `delta`, `catalog`,
`schema`, `broadcast`, `comment`, `upload-artifact`, `fail-on` (`none` | `todo` | `failed`), `python-version`, `github-token`.
Outputs: `converted`, `needs-review`, `failed`, `todos`, `warnings`, `report-path`. Inputs reach scripts through environment variables
(no script injection). The comment step only warns on forked PRs (read-only token). Helper commands used by the action:
`sparqix project … --only FILE`, `sparqix ci-changed`, `sparqix ci-comment`. `.github/workflows/ci.yml` runs the tests on Python 3.9/3.11/3.13
and runs the action on `examples/`. Orchestration files (Airflow DAGs, crontabs, workflows, `.dtsx`) are skipped by `project`: use `sparqix orchestrate`.
Replace `YOUR-ORG/sparqix@v1` with the real repository path and a tag once published. Not run on GitHub: the scripts were exercised
locally against a simulated PR.

## Model-assisted suggestions (`--ai`)

`sparqix FILE --ai` (also `sparqix project … --ai`, and the action input `ai: "true"` with `ai-api-key`) asks a language model about each
`# TODO(sparqix)` block the rules could not convert. The original construct, the converter's reason and the preceding generated lines are sent
to the provider you choose. It is not tied to one vendor:

| `--ai-provider` | Speaks | Key variable | Notes |
|---|---|---|---|
| `anthropic` | Anthropic Messages | `ANTHROPIC_API_KEY` | default model `claude-sonnet-5-5` |
| `openai`, `gemini`, `mistral`, `groq`, `together`, `openrouter` | OpenAI-style chat completions | `OPENAI_API_KEY`, `GEMINI_API_KEY`, `MISTRAL_API_KEY`, `GROQ_API_KEY`, `TOGETHER_API_KEY`, `OPENROUTER_API_KEY` | pass `--ai-model` |
| `ollama` | same, local | none | `http://localhost:11434/v1` |
| `custom` | same | `SPARQIX_AI_API_KEY` (optional) | any OpenAI-compatible server (vLLM, llama.cpp, LM Studio, LiteLLM, a company gateway): needs `--ai-base-url` |

Provider: `--ai-provider` or `SPARQIX_AI_PROVIDER`; otherwise `custom` if `SPARQIX_AI_BASE_URL` is set, else the first vendor key found in the environment, else `anthropic`.
Model: `--ai-model` or `SPARQIX_AI_MODEL` (only Anthropic has a built-in default). Key: the provider's variable above, or `SPARQIX_AI_API_KEY` for any of them.
Base URL: `--ai-base-url` or `SPARQIX_AI_BASE_URL` (https; http only for localhost). The built-in URLs are conveniences: check them against your provider's docs.
The same settings drive the agent (`sparqix agent … --provider … --model … --base-url …`) and the web UI (provider drop-down). Agent mode needs a model that supports tool calling.
Suggestions are inserted below the TODO as **comments** marked `AI-SUGGESTION … unverified`. `--ai-apply` inserts them as live code instead, only if the
result still parses in context and passes a safety check (no file/network/exec/pickle patterns). The TODO count never drops because of AI output:
review every suggestion and run `sparqix verify`. `--ai-max N` caps blocks per file; `--ai-dry-run` prints the prompts and sends nothing. Opt-in only:
nothing is sent without `--ai`. Covers `TODO(sparqix)` comment blocks (pandas, Polars, Dask, R, SAS, Excel, Scala/Java, …), not TODOs that surface as runtime
errors (`_todo()`, stored procedures/streaming stubs). The pasted code is untrusted input to the model and its reply is untrusted output: it is never executed.
Tested against a fake client and a local fake endpoint only, not against the live API.

## Security

Converters only parse input and never execute it; generated code is quoted and checked with an AST safety net, XML with DTDs is rejected,
the web UI is loopback-only with host/origin checks, CSP, optional `--token` and per-request process isolation, AI prompts are redacted,
and CI comments are escaped. `sparqix verify` **executes** the script and therefore requires `--trust`. See [SECURITY.md](SECURITY.md)
for the threat model and the limits of these protections.

## Kubernetes / containers

A `Dockerfile` and hardened manifests (Deployment, Service, NetworkPolicy, PDB, HPA, optional Ingress and a batch Job) live in
[`deploy/`](deploy/README.md). Configuration is via environment: `SPARQIX_TOKEN` (required off-localhost), `SPARQIX_ALLOWED_HOSTS`, and optionally `SPARQIX_AI_PROVIDER` / `SPARQIX_AI_MODEL` / `SPARQIX_AI_BASE_URL` / `SPARQIX_AI_API_KEY` (any provider, see the AI section).

## Apache Gluten + Velox (`--target gluten`)

[Apache Gluten](https://gluten.apache.org/) is a Spark plugin that runs supported operators in a native engine (Velox). Your code does not change,
so any converted output can run on it; the target adds what is needed and tells you where the speed-up will stop:

    sparqix examples/sales.py --target gluten --gluten-offheap 8g

- Adds `spark.plugins`, off-heap memory and the columnar shuffle manager to the generated `SparkSession` (SQL / session-less output gets the same settings as a `spark-submit` header comment).
  The Gluten bundle jar for your Spark/Java version must be on the driver and executor classpaths **at launch**: it cannot be added from inside a running session.
- Warns about code that does not run natively: Python UDFs and pandas UDFs, `.rdd` and RDD calls, `toPandas()`, `collect()`, `F.expr(...)` strings, and JSON/Avro/text/JDBC reads and writes (Parquet/ORC/Delta are best supported).
  Native coverage depends on the Gluten version: the Spark UI shows the real fallbacks. Treat the warnings as a hint, not a guarantee.
- `sparqix verify script.py --trust --gluten` runs the Spark side with those settings and compares it with pandas.
- Works with `sparqix project ... --target gluten` and the web UI target menu. Scala/Java and notebook output is left unchanged with a warning.
- Not tested against a real Gluten build (no Spark here); the settings follow the Gluten documentation.

**Version-agnostic.** Nothing assumes a Spark, Scala or Java release. Override any setting with `--gluten-conf key=value` (repeatable) and name your jar with
`--gluten-jar /path/to/bundle.jar`; `sparqix gluten-conf` prints `spark-defaults.conf` lines. [`deploy/gluten/`](deploy/gluten/README.md) has a Dockerfile that builds on
*your* Spark image with *your* matching Gluten jar (build args, optional SHA-256 check). Option values are restricted to a safe character set, so they cannot inject code.

## Native engines: Velox presets, ClickHouse, Comet, Fabric, Photon

All of these run below the DataFrame/SQL API, so the converted code is unchanged; a target adds the settings and warns where code will not run natively
(Python/pandas UDFs, RDD calls, `toPandas()`, `F.expr` strings, JSON/Avro/JDBC I/O). Native coverage depends on the engine version: the Spark UI shows the real fallbacks.

| `--target` | Engine | What it adds |
|---|---|---|
| `gluten` | Apache Gluten, Velox backend (default) or ClickHouse | plugin, off-heap memory, columnar shuffle, jar header; optional `--gluten-backend velox\|clickhouse` |
| `comet` | Apache DataFusion Comet | `spark.plugins=...CometPlugin`, `CometShuffleManager`, native shuffle, off-heap memory, `$COMET_JAR` header |
| `fabric` | Microsoft Fabric native execution engine | a `%%configure` cell (`spark.native.enabled`, columnar shuffle) or the environment toggle |
| `photon` | Databricks Photon | a Databricks notebook plus the cluster setting (`runtime_engine: PHOTON`); nothing to set in code |

    sparqix job.py --target gluten --gluten-preset production --gluten-preset scan-heavy
    sparqix job.py --target gluten --gluten-backend clickhouse --gluten-libpath /opt/ch/libch.so
    sparqix job.py --target comet --gluten-offheap 8g
    sparqix gluten-conf --engine comet --jar /opt/comet.jar        # just the settings
    sparqix gluten-conf --list-presets

**Velox presets** (`--gluten-preset`, repeatable): `spill-safe` (spill aggregations, joins and sorts under memory pressure, lz4 shuffle), `scan-heavy` (bigger Velox cache),
`agg-heavy` (abandon unproductive partial aggregation sooner), `adaptive` (Spark AQE), `production` (spill-safe + adaptive). They are starting points taken from the
Gluten Velox configuration reference and common examples, not benchmarked values: measure and tune. Precedence: engine defaults < presets < `--gluten-conf`.
Velox-only presets are dropped for the ClickHouse backend and for Comet. The ClickHouse backend needs a `libch.so` that you build (no prebuilt bundle is assumed).

Setting names change between releases (for example Comet's fallback-logging key): override anything with `--gluten-conf key=value`. Not tested against real engines here.

## Agentic mode

A language model drives sparqix's own tools to convert, fix and migrate code. It works with any provider that supports tool calling (see `--ai-provider` above; `sparqix agent … --provider openai --model <id>`), and needs that provider's API key; nothing is sent otherwise.

    sparqix agent fix  job.py -o out/            # convert, resolve TODOs, check, write after you approve
    sparqix agent migrate legacy/ -o out/        # survey, convert in dependency order, fix, write MIGRATION.md
    sparqix agent chat job.py                    # talk about one file; the agent edits a draft, you decide
    sparqix agent fix job.py --dry-run           # show tools, limits and prompt; call nothing

The web UI has an **Assistant** panel under the converted code (same API key field as the AI suggestions): it explains TODOs and edits a draft; "Use revised code" replaces your output only when you click it.

What the agent can do is decided by the tools, not by the prompt, so hostile text inside a file it reads cannot make it do more:
reads stay under the project folder (no symlinks, `..`, secret-looking files such as `.env` or `*.pem`; masked before they are sent);
edits go to in-memory drafts and are refused if they do not parse or add `eval`/`exec`/`open`/`os`/network/dunder constructs or imports outside an allow-list;
writes go only to the output folder (`.py`/`.sql`/`.md`, never over a source or a file it did not create) and only after approval (default: asked each time; `--yes` approves writes into the output folder, nothing else);
running code is off unless you pass `--sandbox-image IMG` (a no-network, read-only, capability-less `docker run`; `--sandbox-data DIR` mounts samples read-only);
budgets limit steps (`--max-steps`), tokens (`--max-tokens`), bytes read and written. The web assistant has no file access and no code execution at all.

**Verify loop (pandas, Polars, Dask, notebooks, SQL, R).** With `--sandbox-image IMG --sandbox-data DIR` the agent also gets `verify_draft`: inside the no-network container it runs your
ORIGINAL script and the draft on copies of the sample data and diffs every DataFrame/number (row and column order ignored, float tolerance). On a mismatch the
model reads the differences, patches the draft and verifies again, up to `--verify-rounds` (default 5) per draft, then stops and reports what still differs.
It cannot cheat the check: the original text and the data are fixed by sparqix (read-only), only the draft can change, and the migration report ends with a
status table that sparqix writes from the last real run ("matched", "differs", "not verified", or "edited since"), whatever the model says. Build a suitable image with
[`deploy/sandbox/Dockerfile`](deploy/sandbox/Dockerfile) (pandas + PySpark + Java; `docker build -t sparqix-sandbox deploy/sandbox`), then:

    sparqix agent fix sales.py --sandbox-image sparqix-sandbox --sandbox-data ./sample -o out/

Languages whose original cannot be run here (SAS, Scala/Java, Excel, dbt, the ETL tools, stored procedures) are reviewed by reading: `verify_draft` says so instead of guessing. SQL drafts must be Spark SQL text (`sql_out=sql`) and R needs an image built with `WITH_R=1`. Matching on sample data is evidence, not proof: use data that exercises
nulls, duplicates and edge cases.

Limits: the agent's patches are checked for syntax and safety; the verify loop adds a behavioural check on your sample data for the languages above: use `sparqix verify` (or the sandbox) on sample data. Model output is not deterministic.
Not run against the live API or a real Docker daemon here: the loop, tools and limits are tested with a scripted stand-in for the model, and the verify harness with a local stand-in for `docker run`.

## License

MIT: see [LICENSE](LICENSE).
