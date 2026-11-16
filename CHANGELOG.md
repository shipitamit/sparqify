# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/).
sparqix is a **migration aid, not a guarantee**: read [docs/limits.md](docs/limits.md) before relying on a conversion.

## [0.1.0] - Unreleased (first beta)

### Converters
- Python/pandas, Polars, Dask, R (dplyr/tidyr), SAS (including macros), Jupyter notebooks, dbt models, Excel formulas, Scala, Java streams and plain Python to PySpark.
- SQL with 40 dialects (Teradata, Oracle, T-SQL, BigQuery, Snowflake, Databricks, PostgreSQL, MySQL, Hive, DB2, Netezza, Vertica, ClickHouse, SAP HANA, Informix, Firebird, Impala, Trino and more) to Spark SQL. Dialects that share a family reuse its rules; the report says which family was used.
- Stored procedures (T-SQL, Oracle PL/SQL, Teradata SPL) to Python functions that call `spark.sql`.
- ETL tools: Informatica PowerCenter and IDMC, DataStage, SSIS, Pentaho, Talend, Alteryx, Pig, Hive scripts, BTEQ and the Teradata load utilities.
- Orchestration: Airflow DAGs, cron and Talend/Pentaho jobs to Databricks job definitions or Airflow.
- Anything that cannot be translated safely becomes a `# TODO(sparqix)` with the original kept beside it; nothing is guessed. A syntax and safety check runs on every generated file.
- Optional AI suggestions for TODOs and an interactive assistant (any of several providers); both are off unless asked for, and their output is checked before it is applied.

### Verification
- `sparqix verify` runs the original and the converted code on sample data and compares the results (pandas, Polars, Dask, notebooks, SQL via DuckDB, R).
- CI runs real-Spark end-to-end checks: pandas, SQL (including Teradata, BigQuery, Databricks and PostgreSQL syntax), R, Polars and Dask cases, and every ETL converter and stored-procedure dialect against hand-computed results.

### Tools
- `sparqix project` converts a whole folder with a report, `sparqix ci-comment` / the GitHub Action comment on pull requests, `sparqix agent` for supervised project conversion, `sparqix gluten-conf` for Gluten/Velox settings.
- Web UI (`sparqix serve`): convert, project and orchestrate tabs, safe by default (loopback, Host and Origin checks, CSP, size and time limits, each request in an isolated process).
- Web UI access: one shared token, per-user tokens (`sparqix users add`), or single sign-on through an OIDC proxy; per-user rate limits and a daily AI cap; access log with the signed-in user; token-protected `/metrics`.

### Deployment
- A small image with no third-party packages, scanned for HIGH and CRITICAL vulnerabilities on every build and published signed, as `v<version>` and `<version>`.
- Kubernetes manifests: restricted pod security, read-only filesystem, NetworkPolicy (DNS only by default), PodDisruptionBudget, HorizontalPodAutoscaler, graceful shutdown, probes, a batch Job example and an SSO ingress example.

### Known limits
See [docs/limits.md](docs/limits.md): Scala output has never been compiled, SQL dialects are checked on hand-written examples rather than real engines, `UPDATE`/`DELETE`/`MERGE` in stored procedures are syntax-checked but not run, and a number of constructs (for example `ewm`, time-based rolling windows, SAS `LAG` inside `IF`) stay TODOs.
