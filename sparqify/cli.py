"""Command line interface:  sparqify FILE [--from auto|pandas|sql|python] [-o OUT]"""
from __future__ import annotations

import argparse
import sys

from .core import convert


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "ci-changed":
        from .ci import changed_main
        return changed_main(argv[1:])
    if argv and argv[0] == "ci-comment":
        from .ci import main as cmain
        return cmain(argv[1:])
    if argv and argv[0] == "serve":
        from .web import main as wmain
        return wmain(argv[1:])
    if argv and argv[0] == "users":
        from .webauth import users_main
        return users_main(argv[1:])
    if argv and argv[0] == "orchestrate":
        from .orchestrate import main as omain
        return omain(argv[1:])
    if argv and argv[0] == "project":
        from .project import main as pmain
        return pmain(argv[1:])
    if argv and argv[0] == "agent":
        from .agent import main as amain
        return amain(argv[1:])
    if argv and argv[0] == "gluten-conf":
        from .gluten_conv import main as gmain
        return gmain(argv[1:])
    if argv and argv[0] == "verify":
        from .verify import main as vmain
        return vmain(argv[1:])
    p = argparse.ArgumentParser(prog="sparqify",
                                description="Convert pandas / plain Python / SQL into Apache Spark code.")
    p.add_argument("file", nargs="?", help="input file (default: stdin)")
    p.add_argument("--from", dest="lang", default="auto", choices=["auto", "pandas", "polars", "dask", "r", "sql", "python", "notebook", "dbt", "sas", "excel", "scala", "java", "powercenter", "idmc", "talend", "alteryx", "pentaho", "pig", "ssis", "hive", "bteq", "datastage", "databricks", "proc", "stream", "tdload"])
    p.add_argument("--dialect", default="generic",
                   choices=["generic", "tsql", "oracle", "mysql", "postgres", "snowflake", "bigquery", "redshift", "duckdb", "hive", "trino", "presto", "sqlite", "teradata", "spark", "databricks", "alloydb", "athena", "aurora_mysql", "aurora_postgres", "azuresql", "clickhouse", "cockroachdb", "db2", "doris", "fabric_warehouse", "firebird", "greenplum", "hana", "impala", "informix", "mariadb", "memsql", "mssql", "netezza", "singlestore", "sqlserver", "starburst", "starrocks", "sybase", "synapse", "tidb", "timescaledb", "vertica", "yugabyte"],
                   help="source SQL dialect")
    p.add_argument("--target", default="spark", choices=["spark", "databricks", "gluten", "comet", "fabric", "photon"],
                   help="databricks: notebook; gluten (Velox/ClickHouse), comet (DataFusion), fabric (native execution engine), photon: settings for a native engine + fallback warnings")
    p.add_argument("--gluten-offheap", default="4g", help="off-heap size for --target gluten (e.g. 8g)")
    p.add_argument("--gluten-backend", default="", choices=["", "velox", "clickhouse"], help="gluten backend (default: your build's default, Velox)")
    p.add_argument("--gluten-libpath", default="", help="clickhouse backend: path of libch.so on the executors")
    p.add_argument("--gluten-preset", action="append", default=[], metavar="NAME",
                   help="Velox starting points, repeatable: spill-safe, scan-heavy, agg-heavy, adaptive, production (see `sparqify gluten-conf --list-presets`)")
    p.add_argument("--gluten-conf", action="append", default=[], metavar="KEY=VALUE",
                   help="extra or overriding Spark setting for --target gluten, repeatable (e.g. another shuffle manager for your Gluten/Spark version)")
    p.add_argument("--gluten-jar", default="", help="path of the Gluten bundle jar to show in the launch header (default: $GLUTEN_JAR; use the jar built for your Spark/Scala/OS)")
    p.add_argument("--mode", default="dataframe", choices=["dataframe", "pandas-api"],
                   help="pandas input: PySpark DataFrame code (default) or pandas API on Spark (pyspark.pandas)")
    p.add_argument("--delta", action="store_true", help="write Delta (parquet/orc/saveAsTable), DDL USING DELTA")
    p.add_argument("--catalog", default=None, help="Unity Catalog: qualify tables as CATALOG.SCHEMA.table")
    p.add_argument("--schema", default="default", help="schema for unqualified table names (with --catalog)")
    p.add_argument("--ai", action="store_true", help="ask a language model to suggest code for TODO blocks (sends snippets to the provider you choose; see --ai-provider)")
    p.add_argument("--ai-apply", action="store_true", help="with --ai: insert suggestions as live code (if they parse) instead of comments")
    p.add_argument("--ai-model", default=None, help="model id (default: env SPARQIFY_AI_MODEL; anthropic defaults to claude-sonnet-5-5, other providers need one)")
    p.add_argument("--ai-provider", default=None, help="anthropic, openai, gemini, mistral, groq, together, openrouter, ollama or custom (any OpenAI-compatible server); default: env SPARQIFY_AI_PROVIDER, else guessed from the API key variables set")
    p.add_argument("--ai-base-url", default=None, help="API base URL (e.g. http://localhost:11434/v1); env SPARQIFY_AI_BASE_URL; https required except for localhost")
    p.add_argument("--ai-max", type=int, default=20, help="maximum number of TODO blocks to send")
    p.add_argument("--ai-dry-run", action="store_true", help="print the prompts that would be sent; send nothing")
    p.add_argument("--optimize", action="store_true", help="performance pass: UDF->built-ins, broadcast joins, cache()")
    p.add_argument("--broadcast", default="", help="with --optimize: comma-separated DataFrame names to broadcast in joins")
    p.add_argument("--sql-out", default="pyspark", choices=["pyspark", "sql"],
                   help="emit spark.sql(...) calls (default) or plain Spark SQL")
    p.add_argument("--model-name", default="", help="dbt: name of the temp view for the model (default: file name)")
    p.add_argument("--excel-columns", default="", help='Excel: map letters to columns, e.g. "A=price,B=qty"')
    p.add_argument("-o", "--output", help="output file (default: stdout)")
    p.add_argument("--strict", action="store_true", help="exit with status 2 if any TODO remains")
    p.add_argument("-q", "--quiet", action="store_true", help="do not print the conversion report")
    args = p.parse_args(argv)

    if args.file:
        with open(args.file, encoding="utf-8") as fh:
            src = fh.read()
    else:
        src = sys.stdin.read()

    try:
        code, report = _run(src, args)
    except Exception as e:
        from .ai import AIError
        from .core import ConvertError, InputRejected, UnsafeOutput
        if isinstance(e, (AIError, ConvertError, InputRejected, UnsafeOutput, SyntaxError)):
            print(f"sparqify: {e.__class__.__name__}: {e}" if not isinstance(e, AIError) else f"sparqify: {e}", file=sys.stderr)
            return 2
        raise
    return _finish(code, report, args)


def _run(src, args):
    return convert(src, args.lang, sql_dialect=args.dialect, sql_out=args.sql_out, target=args.target, gluten_offheap=args.gluten_offheap, gluten_conf=args.gluten_conf, gluten_jar=args.gluten_jar, gluten_backend=args.gluten_backend, gluten_libpath=args.gluten_libpath, gluten_preset=args.gluten_preset, mode=args.mode, ai=args.ai, ai_apply=args.ai_apply, ai_model=args.ai_model, ai_provider=args.ai_provider, ai_base_url=args.ai_base_url, ai_max=args.ai_max, ai_dry_run=args.ai_dry_run, delta=args.delta, catalog=args.catalog, schema=args.schema, optimize=args.optimize, broadcast=[b for b in args.broadcast.split(',') if b],
                           filename=args.file or "", model=args.model_name,
                           excel_columns=dict(kv.split("=", 1) for kv in args.excel_columns.split(",") if "=" in kv))


def _finish(code, report, args):
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(code)
    else:
        sys.stdout.write(code)
    if not args.quiet:
        text = report.summary()
        if text:
            print("\n--- sparqify report ---\n" + text, file=sys.stderr)
        print(f"--- {len(report.todos)} TODO(s), {len(report.warnings)} warning(s) ---", file=sys.stderr)
    return 2 if (args.strict and report.todos) else 0


if __name__ == "__main__":
    raise SystemExit(main())
