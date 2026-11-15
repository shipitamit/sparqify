# What sparqix does not do (yet)

sparqix is a **migration aid, not a guarantee**. Review every converted file before it runs in production.

## How to read the output
- `# TODO(sparqix)` marks code it could not translate safely. The original code is kept next to it. Nothing is silently guessed.
- `WARNING` lines in the report mean the code converted but behaves differently in Spark (for example row order, `to_csv` writing a folder, or case-sensitive text matching).
- The converted code always parses. That does not mean it gives the same results; use `sparqix verify` on sample data for that.

## Testing status
| Area | Status |
|---|---|
| Golden tests and parse checks | Run on every change (234 tests) |
| Real Spark | CI job `spark-e2e` runs the cases in `tests/e2e/cases`. Coverage is small; treat it as a start, not proof |
| Java output | Type-checked with `javac` against hand-written Spark API stubs, not real Spark jars |
| Scala output | Never compiled |
| SQL dialects | Checked on hand-written examples, not against real database engines |
| Teradata, SAS, ETL tools | Best effort; the original tools were not available to compare against. The Pentaho, Pig, Informatica PowerCenter and SSIS conversions run on real Spark against hand-computed results (`tests/test_e2e_etl.py`); DataStage, Alteryx, Talend and stored procedures do not yet |

## Verify loop (`sparqix verify`)
- Can run the original for pandas, Polars, Dask, notebooks, SQL (DuckDB or SQLite) and R.
- **Cannot** run the original for SAS (proprietary, no free engine), Scala/Java, Excel, dbt, the ETL tools, or stored procedures. These are reviewed by reading, and the tool says so.
- SQL verification only works for SQL that DuckDB or SQLite accepts.
- A match on sample data is evidence, not proof. Use data that covers your edge cases.

## Main gaps by language
- **pandas:** `apply(axis=1)`, positional indexing, index semantics, `ewm`, time-based or centered rolling windows.
- **SQL:** `ROWNUM` next to `GROUP BY`/`OR`/`UNION`, `ROWNUM > n`, `NULLS FIRST/LAST` in some aggregates, ClickHouse `LIMIT n BY`, Informix/Firebird `SKIP`.
- **SQL dialects:** 19 of the 40 are aliases that reuse another dialect's rules (MariaDB uses MySQL, Greenplum uses PostgreSQL, and so on). Dialect-specific features may be missed; a note in the report says which family was used.
- **R:** selection helpers such as `starts_with`, `-col` and `a:c`; several `names_from` columns in `pivot_wider`.
- **SAS:** `LAG` inside `IF`, `DO`/`OUTPUT` loops, input pointers, run-time-dependent macros.
- **Excel:** `HLOOKUP`, `MATCH` alone, `INDEX` by row number, `XLOOKUP` match modes.
- **Scala/Java:** `sortWith`, `partition`, `minBy/maxBy`, for-comprehensions, `partitioningBy`, stream variables kept across statements.
- **Teradata:** period algebra (`P_INTERSECT`, `P_NORMALIZE`), SQLSTATE handlers, `REPEAT`, conditional `APPLY`. SPL cursors are collected to the driver and `UNDO` does not roll back.
- **ETL tools:** custom code (Python callables, UDFs, Pig macros) becomes a TODO.

## Semantics that change
- Order-dependent logic (SAS `RETAIN`, window functions without a sort) assumes the current row order and may use a single partition. This is slow on large data.
- `iterrows` loops that only read rows become a driver-side loop. That works but is not distributed.
- `drop_duplicates` keeps an arbitrary row in Spark.

## AI features
AI suggestions and the agent are optional and unverified. Patches are checked for syntax and safety, not correctness. Model output is not deterministic.
