"""More source SQL dialects for sql_conv.

Two kinds:
  * ALIASES: engines that speak a dialect sparqix already has (MariaDB = MySQL, Synapse = T-SQL, Greenplum = Postgres, Athena = Trino, ...). They run through
    that dialect's rules plus a few clean-ups of their own (storage / distribution clauses, hints).
  * OWN: engines with their own functions and syntax (DB2, Netezza, Vertica, ClickHouse, SAP HANA, Informix, Firebird, Impala).
    A text pre-pass rewrites the syntax Spark cannot parse, then `function()` maps their functions.

Everything is token/regex based and never touches string literals, quoted identifiers or comments (`_outside`). Syntax with no safe equivalent becomes a TODO.
Not run against the real engines or a real Spark here: the rules come from each engine's documented syntax and are covered by conversion tests only.
"""
from __future__ import annotations

import re

ALIASES = {
    # MySQL family
    "mariadb": "mysql", "singlestore": "mysql", "memsql": "mysql", "starrocks": "mysql", "doris": "mysql", "tidb": "mysql", "aurora_mysql": "mysql",
    # PostgreSQL family
    "greenplum": "postgres", "cockroachdb": "postgres", "yugabyte": "postgres", "alloydb": "postgres", "timescaledb": "postgres", "aurora_postgres": "postgres",
    # T-SQL family
    "synapse": "tsql", "sybase": "tsql", "azuresql": "tsql", "fabric_warehouse": "tsql", "mssql": "tsql", "sqlserver": "tsql",
    # Presto / Trino family
    "athena": "trino", "starburst": "trino",
}
OWN = ("db2", "netezza", "vertica", "clickhouse", "hana", "informix", "firebird", "impala")
NAMES = tuple(sorted(set(ALIASES) | set(OWN)))
_ALIAS_NOTE = {
    "mariadb": "MySQL", "singlestore": "MySQL", "memsql": "MySQL", "starrocks": "MySQL", "doris": "MySQL", "tidb": "MySQL", "aurora_mysql": "MySQL",
    "greenplum": "PostgreSQL", "cockroachdb": "PostgreSQL", "yugabyte": "PostgreSQL", "alloydb": "PostgreSQL", "timescaledb": "PostgreSQL", "aurora_postgres": "PostgreSQL",
    "synapse": "T-SQL", "sybase": "T-SQL", "azuresql": "T-SQL", "fabric_warehouse": "T-SQL", "mssql": "T-SQL", "sqlserver": "T-SQL",
    "athena": "Trino", "starburst": "Trino",
}

_LIT = re.compile(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|`[^`]*`|--[^\n]*|/\*.*?\*/)", re.S)


def base_dialect(name: str) -> str:
    return ALIASES.get(name, name)


def _outside(text, fn):
    """apply fn to the code parts of `text` only (not to strings, quoted identifiers or comments)"""
    return "".join(p if i % 2 else fn(p) for i, p in enumerate(_LIT.split(text)))


def _match_close(text, i):
    """index of the ')' closing the '(' at text[i] (literals skipped), or -1"""
    depth, k, n = 0, i, len(text)
    while k < n:
        m = _LIT.match(text, k)
        if m and m.start() == k and text[k] in "'\"`-/":
            k = m.end()
            continue
        if text[k] == "(":
            depth += 1
        elif text[k] == ")":
            depth -= 1
            if depth == 0:
                return k
        k += 1
    return -1


# ------------------------------------------------------------------------------------------------------------------ pre-pass rules
# (kind, regex, replacement-or-message, create_only)   kinds: sub (rewrite), drop (remove, warn with message), todo (flag, keep text)
_COMMON = [
    ("sub", r"(?i)\bDOUBLE\s+PRECISION\b", "DOUBLE", False),
]
_MYSQL_DDL = [
    ("drop", r"(?i)\s+ENGINE\s*=\s*\w+", "storage ENGINE dropped: add USING delta/parquet", True),
    ("drop", r"(?i)\s+(?:DEFAULT\s+)?(?:CHARSET|CHARACTER\s+SET)\s*=?\s*\w+", None, True),
    ("drop", r"(?i)\s+COLLATE\s*=?\s*\w+", None, True),
    ("drop", r"(?i)\s+AUTO_INCREMENT\s*=\s*\d+", None, True),
    ("drop", r"(?i)\s+ROW_FORMAT\s*=\s*\w+", None, True),
    ("drop", r"(?i)\s+AUTO_INCREMENT\b", "AUTO_INCREMENT dropped: Spark has no auto-increment column (use monotonically_increasing_id() or a Delta identity column)", True),
    ("sub", r"(?i)\s+UNSIGNED\b", "", True),
]
_PRE = {
    "mariadb": _MYSQL_DDL, "tidb": _MYSQL_DDL, "aurora_mysql": _MYSQL_DDL,
    "singlestore": _MYSQL_DDL + [("drop", r"(?i)\s*,?\s*(?:SHARD|SORT)\s+KEY\s*\((?:[^()]|\([^()]*\))*\)", "SHARD KEY / SORT KEY dropped", True)],
    "memsql": _MYSQL_DDL + [("drop", r"(?i)\s*,?\s*(?:SHARD|SORT)\s+KEY\s*\((?:[^()]|\([^()]*\))*\)", "SHARD KEY / SORT KEY dropped", True)],
    "starrocks": _MYSQL_DDL + [
        ("drop", r"(?i)\s+(?:DUPLICATE|AGGREGATE|UNIQUE|PRIMARY)\s+KEY\s*\((?:[^()]|\([^()]*\))*\)", "table key model dropped", True),
        ("drop", r"(?i)\s+DISTRIBUTED\s+BY\s+HASH\s*\((?:[^()]|\([^()]*\))*\)(?:\s+BUCKETS\s+\d+)?", "DISTRIBUTED BY dropped", True),
        ("drop", r"(?#raw)(?i)\s+PROPERTIES\s*\((?:[^()]|\([^()]*\))*\)", "PROPERTIES dropped", True)],
    "greenplum": [
        ("drop", r"(?i)\s+DISTRIBUTED\s+(?:BY\s*\((?:[^()])*\)|RANDOMLY|REPLICATED)", "DISTRIBUTED BY dropped", True),
        ("drop", r"(?i)\s+WITH\s*\(\s*(?:appendonly|appendoptimized|orientation|compresstype|compresslevel)[^()]*\)", "storage options dropped", True),
        ("todo", r"(?i)^\s*CREATE\s+(?:READABLE\s+|WRITABLE\s+)?EXTERNAL\s+(?:WEB\s+)?TABLE\b", "Greenplum external tables (gpfdist) have no equivalent: read the files with spark.read", False)],
    "cockroachdb": [
        ("drop", r"(?#raw)(?i)\s+AS\s+OF\s+SYSTEM\s+TIME\s+(?:'[^']*'|[^\s;)]+)", "AS OF SYSTEM TIME dropped (no time travel on this read)", False),
        ("todo", r"(?i)^\s*UPSERT\s+INTO\b", "UPSERT INTO has no Spark equivalent: use MERGE INTO on a Delta table", False)],
    "synapse": [
        ("drop", r"(?i)\s+WITH\s*\(\s*(?:DISTRIBUTION|CLUSTERED|HEAP|PARTITION)\b(?:[^()]|\([^()]*\))*\)", "DISTRIBUTION / index / partition options dropped", True),
        ("drop", r"(?#raw)(?i)\s+OPTION\s*\((?:[^()]|\([^()]*\))*\)\s*$", "OPTION (...) query hints dropped", False)],
    "azuresql": [("drop", r"(?i)\s+OPTION\s*\((?:[^()]|\([^()]*\))*\)\s*$", "OPTION (...) query hints dropped", False)],
    "fabric_warehouse": [("drop", r"(?i)\s+OPTION\s*\((?:[^()]|\([^()]*\))*\)\s*$", "OPTION (...) query hints dropped", False)],
    "sybase": [
        ("sub", r"(?i)\bHOLDLOCK\b", "", False),
        ("drop", r"(?i)\s+(?:AT\s+ISOLATION\s+(?:READ\s+UNCOMMITTED|READ\s+COMMITTED|SERIALIZABLE|[0-3]))", "isolation clause dropped", False)],
    "athena": [("todo", r"(?i)^\s*(?:MSCK\s+REPAIR\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+PARTITION)\b", "partition discovery: Spark equivalent is MSCK REPAIR TABLE / spark.catalog.recoverPartitions", False)],
    "db2": [
        ("drop", r"(?i)\s+FROM\s+(?:SYSIBM\.)?(?:SYSDUMMY1|DUAL)\b", None, False),
        ("sub", r"(?i)\bCURRENT\s+DATE\b", "current_date()", False),
        ("sub", r"(?i)\bCURRENT\s+TIMESTAMP\b", "current_timestamp()", False),
        ("sub", r"(?i)\bCURRENT\s+TIME\b", "date_format(current_timestamp(), 'HH:mm:ss')", False),
        ("sub", r"(?i)\bCURRENT\s+SCHEMA\b", "current_database()", False),
        ("sub", r"(?i)\bFETCH\s+FIRST\s+ROW\s+ONLY\b", "LIMIT 1", False),
        ("drop", r"(?i)\s+WITH\s+(?:UR|CS|RS|RR)\b(?!\s*\()", "isolation clause (WITH UR/CS/RS/RR) dropped", False),
        ("drop", r"(?i)\s+FOR\s+(?:READ|FETCH)\s+ONLY\b", None, False),
        ("drop", r"(?i)\s+OPTIMIZE\s+FOR\s+\d+\s+ROWS?\b", None, False),
        ("sub", r"(?i)\bGRAPHIC\b|\bVARGRAPHIC\b|\bDBCLOB\b", "VARCHAR", False),
        ("sub", r"(?i)\bDECFLOAT\b(?:\s*\(\s*\d+\s*\))?", "DOUBLE", False),
        ("drop", r"(?i)\s+ORGANIZE\s+BY\s+(?:ROW|COLUMN)\b", "ORGANIZE BY dropped", True),
        ("drop", r"(?i)\s+DISTRIBUTE\s+BY\s+(?:HASH\s*\((?:[^()])*\)|RANDOM|REPLICATION)", "DISTRIBUTE BY dropped", True),
        ("drop", r"(?i)\s+(?:COMPRESS\s+(?:YES|NO)|DATA\s+CAPTURE\s+\w+|NOT\s+LOGGED(?:\s+INITIALLY)?)\b", None, True),
        ("drop", r"(?i)(?<=\))\s+IN\s+\w+(?:\s+INDEX\s+IN\s+\w+)?(?=\s*$)", "tablespace clause dropped", True),
        ("drop", r"(?i)\s+GENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY(?:\s*\((?:[^()])*\))?", "identity column dropped (Delta identity columns need Databricks)", True),
        ("todo", r"(?i)\b(?:XMLQUERY|XMLTABLE|XMLELEMENT|XMLAGG)\s*\(", "DB2 XML functions have no Spark equivalent", False)],
    "netezza": [
        ("sub", r"(?i)\b([A-Za-z_][\w.]*)\s+ISNULL\b(?!\s*\()", r"\1 IS NULL", False),
        ("sub", r"(?i)\b([A-Za-z_][\w.]*)\s+NOTNULL\b(?!\s*\()", r"\1 IS NOT NULL", False),
        ("drop", r"(?i)\s+DISTRIBUTE\s+ON\s+(?:\((?:[^()])*\)|RANDOM|HASH\s*\((?:[^()])*\))", "DISTRIBUTE ON dropped", True),
        ("drop", r"(?i)\s+ORGANIZE\s+ON\s*\((?:[^()])*\)", "ORGANIZE ON dropped", True),
        ("sub", r"(?i)^\s*GENERATE\s+(?:EXPRESS\s+)?STATISTICS(?:\s+ON\s+([\w.]+))?", r"ANALYZE TABLE \1 COMPUTE STATISTICS", False),
        ("todo", r"(?i)^\s*CREATE\s+EXTERNAL\s+TABLE\b|\bREMOTE\s+SOURCE\b", "Netezza external tables have no equivalent: read the file with spark.read", False)],
    "vertica": [
        ("sub", r"(?i)\bLONG\s+VARCHAR\b", "STRING", False),
        ("sub", r"(?i)\bLONG\s+VARBINARY\b", "BINARY", False),
        ("drop", r"(?i)\s+(?:UN)?SEGMENTED\s+(?:BY\s+(?:[^;]*?)\s+)?(?:ALL\s+NODES|NODES\s+[\w, ]+)(?:\s+KSAFE(?:\s+\d+)?)?", "SEGMENTED BY dropped", True),
        ("drop", r"(?i)\s+ENCODING\s+\w+|\s+ACCESSRANK\s+\d+", None, True),
        ("drop", r"(?i)(?<=\))\s+ORDER\s+BY\s+[\w., ]+(?=\s*$)", "projection ORDER BY dropped", True),
        ("drop", r"(?i)\s+INCLUDE\s+(?:SCHEMA\s+)?PRIVILEGES", None, True),
        ("todo", r"(?i)^\s*CREATE\s+(?:PROJECTION|FLEX\s+TABLE)\b|^\s*COPY\s+\S+", "Vertica projections / COPY / flex tables have no equivalent: use spark.read and Delta", False),
        ("todo", r"(?i)\bTIMESERIES\b", "Vertica TIMESERIES (gap filling) has no direct equivalent: build the time grid with sequence() and left-join", False),
        ("todo", r"(?i)\bLIMIT\s+\d+\s+OVER\s*\(", "LIMIT n OVER (PARTITION BY ... ORDER BY ...) -> row_number() OVER (...) <= n in a sub-query", False),
        ("todo", r"(?i)\bUSING\s+PARAMETERS\b", "Vertica USING PARAMETERS has no equivalent", False)],
    "clickhouse": [
        ("sub", r"(?i)\b(?:Nullable|LowCardinality)\s*\(((?:[^()]|\([^()]*\))*)\)", r"\1", False),
        ("drop", r"(?i)\s+FORMAT\s+\w+\s*$", "FORMAT clause dropped", False),
        ("drop", r"(?i)\s+SETTINGS\s+[\w.]+\s*=[^;]*$", "trailing SETTINGS dropped", False),
        ("drop", r"(?i)(?<=\w)\s+FINAL\b(?=\s*(?:WHERE|PREWHERE|GROUP|ORDER|LIMIT|JOIN|SAMPLE|\)|;|$))", "FINAL (merge-on-read deduplication) has no equivalent: deduplicate explicitly for ReplacingMergeTree tables", False),
        ("sub", r"(?i)\bGLOBAL\s+(IN|(?:ANY\s+|ALL\s+)?(?:LEFT\s+|RIGHT\s+|INNER\s+|FULL\s+)?JOIN)\b", r"\1", False),
        ("todo", r"(?i)\bLIMIT\s+\d+(?:\s*,\s*\d+)?\s+BY\b", "LIMIT n BY col -> row_number() OVER (PARTITION BY col ...) <= n in a sub-query", False),
        ("todo", r"(?i)\b(?:LEFT\s+)?ARRAY\s+JOIN\b", "ARRAY JOIN -> LATERAL VIEW explode(...)", False),
        ("todo", r"(?i)\bWITH\s+TOTALS\b|\bSAMPLE\s+[\d.]+\b", "WITH TOTALS / SAMPLE have no direct equivalent", False),
        ("drop", r"(?i)\s+ENGINE\s*=\s*.*?(?=\s+AS\s+SELECT\b|\s*$)", "ENGINE / ORDER BY / PARTITION BY / TTL / SETTINGS dropped: add USING delta/parquet", True)],
    "hana": [
        ("drop", r"(?i)\s+FROM\s+DUMMY\b", None, False),
        ("sub", r"(?i)\bCURRENT_UTCTIMESTAMP\b", "current_timestamp()", False),
        ("sub", r"(?i)\bCURRENT_UTCDATE\b", "current_date()", False),
        ("drop", r"(?i)\s+WITH\s+PARAMETERS\s*\((?:[^()]|\([^()]*\))*\)", "WITH PARAMETERS dropped", False),
        ("sub", r"(?i)\bCREATE\s+(?:COLUMN|ROW)\s+TABLE\b", "CREATE TABLE", True),
        ("sub", r"(?i)\bSECONDDATE\b", "TIMESTAMP", False),
        ("sub", r"(?i)\bDAYDATE\b", "DATE", False),
        ("sub", r"(?i)\b(?:SHORTTEXT|ALPHANUM)\b", "VARCHAR", False),
        ("sub", r"(?i)\bNCLOB\b", "STRING", False),
        ("sub", r"(?i)\bSMALLDECIMAL\b", "DOUBLE", False),
        ("todo", r"(?i)\"_SYS_BIC\"\.", "calculation views (_SYS_BIC) have no Spark equivalent: export the view's data or re-implement it", False),
        ("todo", r"(?i)^\s*UPSERT\b|\bSERIES_GENERATE_\w+", "UPSERT / SERIES_GENERATE have no direct equivalent", False)],
    "informix": [
        ("sub", r"(?i)\bDATETIME\s+YEAR\s+TO\s+DAY\b", "DATE", False),
        ("sub", r"(?i)\bDATETIME\s+YEAR\s+TO\s+(?:SECOND|FRACTION(?:\(\d\))?|MINUTE|HOUR)\b", "TIMESTAMP", False),
        ("sub", r"(?i)\bDATETIME\s*\(\s*([\d\-: .]+)\s*\)\s+YEAR\s+TO\s+\w+(?:\(\d\))?", r"TIMESTAMP '\1'", False),
        ("sub", r"(?i)\bCURRENT\s+YEAR\s+TO\s+(?:SECOND|FRACTION(?:\(\d\))?|DAY|MINUTE|HOUR)\b", "current_timestamp()", False),
        ("sub", r"(?i)\bTODAY\b(?!\s*\()", "current_date()", False),
        ("drop", r"(?i)\s+FROM\s+systables\s+WHERE\s+tabid\s*=\s*1\b", None, False),
        ("sub", r"(?i)\bLVARCHAR\b", "VARCHAR", False),
        ("sub", r"(?i)\bSMALLFLOAT\b", "FLOAT", False),
        ("sub", r"(?i)\bSERIAL8\b", "BIGINT", False),
        ("drop", r"(?i)\s+(?:EXTENT|NEXT)\s+SIZE\s+\d+", None, True),
        ("drop", r"(?i)\s+LOCK\s+MODE\s+(?:ROW|PAGE|TABLE)\b", None, True),
        ("drop", r"(?i)(?<=\))\s+IN\s+\w+(?=\s*$)", "dbspace clause dropped", True),
        ("todo", r"(?i)\bEXTEND\s*\(|\bINTERVAL\s*\(\s*[\d\-: ]+\)\s+\w+\s+TO\b", "Informix EXTEND / INTERVAL literals have no automatic conversion", False)],
    "firebird": [
        ("drop", r"(?i)\s+FROM\s+RDB\$DATABASE\b", None, False),
        ("sub", r"(?#raw)(?i)\bCAST\s*\(\s*'NOW'\s+AS\s+TIMESTAMP\s*\)", "current_timestamp()", False),
        ("sub", r"(?#raw)(?i)\bCAST\s*\(\s*'TODAY'\s+AS\s+DATE\s*\)", "current_date()", False),
        ("sub", r"(?i)\bBLOB\s+SUB_TYPE\s+(?:TEXT|1)\b(?:\s+SEGMENT\s+SIZE\s+\d+)?", "STRING", False),
        ("sub", r"(?i)\bBLOB\s+SUB_TYPE\s+(?:BINARY|0)\b(?:\s+SEGMENT\s+SIZE\s+\d+)?", "BINARY", False),
        ("sub", r"(?i)\s+ROWS\s+(\d+)\s*$", r" LIMIT \1", False),
        ("todo", r"(?i)\bROWS\s+\d+\s+TO\s+\d+\b|^\s*UPDATE\s+OR\s+INSERT\b|^\s*EXECUTE\s+BLOCK\b|\bGEN_ID\s*\(", "Firebird ROWS m TO n / UPDATE OR INSERT / EXECUTE BLOCK / generators have no automatic conversion", False)],
    "impala": [
        ("sub", r"(?i)^\s*COMPUTE\s+(?:INCREMENTAL\s+)?STATS\s+([\w.]+)", r"ANALYZE TABLE \1 COMPUTE STATISTICS", False),
        ("sub", r"(?i)^\s*REFRESH\s+([\w.]+)", r"REFRESH TABLE \1", False),
        ("sub", r"(?i)^\s*INVALIDATE\s+METADATA(?:\s+[\w.]+)?", "-- INVALIDATE METADATA is not needed in Spark", False),
        ("sub", r"(?i)\bJOIN\s+\[(?:SHUFFLE|BROADCAST|NOSHUFFLE)\]", "JOIN", False),
        ("sub", r"(?i)\bSTRAIGHT_JOIN\b", "", False),
        ("todo", r"(?i)^\s*UPSERT\s+INTO\b", "UPSERT (Kudu) has no Spark equivalent: use MERGE INTO on a Delta table", False)],
}

# interval arithmetic written as labelled durations:  DB2 `d + 3 DAYS`, Informix `d + 7 UNITS DAY`
_DUR = re.compile(r"(?i)(?P<lhs>current_date\(\)|current_timestamp\(\)|[A-Za-z_][\w.]*(?:\([^()]*\))?|'[^']*')\s*(?P<sg>[+-])\s*(?P<n>\d+|\([^()]*\)|[A-Za-z_]\w*)\s+"
                  r"(?P<u>(?:YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)S?)\b")
_DUR_UNITS = re.compile(r"(?i)(?P<lhs>current_date\(\)|current_timestamp\(\)|[A-Za-z_][\w.]*(?:\([^()]*\))?)\s*(?P<sg>[+-])\s*(?P<n>\d+|\([^()]*\))\s+UNITS\s+"
                        r"(?P<u>YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)\b")


def _durations(text, rx):
    def sub(m):
        u = m.group("u").upper().rstrip("S") if not m.group("u").upper().endswith("SS") else m.group("u").upper()
        n = m.group("n")
        n = n if n.startswith("(") else f"({n})"
        return f"({m.group('lhs')} {m.group('sg')} {n} * INTERVAL 1 {u})"
    return rx.sub(sub, text)


def _first_top(text, ctx, name):
    """Informix / Firebird `SELECT [SKIP m] FIRST n` -> `SELECT TOP n` (moved to LIMIT by sql_conv); SKIP has no placement -> TODO"""
    def one(code):
        def sub(m):
            skip = m.group("skip") or m.group("skip2")
            if skip:
                ctx.todo(f"{name} SKIP {skip}: add OFFSET {skip} after the LIMIT by hand (Spark 3.4+)")
            return f"{m.group(1)}TOP {m.group('n')}"
        return re.sub(r"(?i)(\bSELECT\s+(?:DISTINCT\s+|UNIQUE\s+)?)(?:SKIP\s+(?P<skip>\d+)\s+)?FIRST\s+(?P<n>\d+)(?:\s+SKIP\s+(?P<skip2>\d+))?", sub, code)
    return _outside(text, one)


def _parametric_aggs(text, ctx):
    """ClickHouse quantile(0.5)(x) -> percentile_approx(x, 0.5); quantileExact -> percentile"""
    rx = re.compile(r"(?i)\b(quantile(?:Exact|TDigest|Timing)?)\s*\(\s*([\d.]+)\s*\)\s*\(")
    out, pos = [], 0
    while True:
        m = rx.search(text, pos)
        if not m:
            break
        close = _match_close(text, m.end() - 1)
        if close < 0:
            break
        inner = text[m.end():close]
        fn = "percentile" if m.group(1).lower() == "quantileexact" else "percentile_approx"
        out.append(text[pos:m.start()] + f"{fn}({inner}, {m.group(2)})")
        pos = close + 1
    return "".join(out) + text[pos:]


def prepass(text: str, ctx) -> str:
    """dialect-specific text rewrites run before tokenising one statement"""
    name = ctx.name
    rules = _COMMON + _PRE.get(name, [])
    is_create = re.match(r"(?is)\s*(?:--[^\n]*\n\s*)*CREATE\b", text) is not None
    for kind, rx, arg, create_only in rules:
        if create_only and not is_create:
            continue
        raw = rx.startswith("(?#raw)")
        compiled = re.compile(rx, re.S)
        if kind == "todo":
            if compiled.search(text):
                ctx.todo(arg)
            continue
        hit = []

        def one(code, compiled=compiled, kind=kind, arg=arg):
            if kind == "sub":
                new = compiled.sub(arg, code)
            else:
                new = compiled.sub("", code)
            if new != code:
                hit.append(1)
            return new
        text = one(text) if raw else _outside(text, one)
        if hit and kind == "drop" and arg:
            ctx.warn(arg)
    if name in ("informix", "firebird"):
        text = _first_top(text, ctx, name)
    if name == "informix":
        text = _outside(text, lambda c: _durations(c, _DUR_UNITS))
    if name == "db2":
        text = _outside(text, lambda c: _durations(c, _DUR))
    if name == "clickhouse":
        text = _outside(text, lambda c: _parametric_aggs(c, ctx))
        text = _clickhouse_prewhere(text, ctx)
    if name == "informix":
        text = _outside(text, lambda c: re.sub(r"(?i)\bCURRENT\b(?=\s*(?:[,);<>=+\-]|$|AND\b|OR\b|ORDER\b|GROUP\b))", "current_timestamp()", c))
    return text


def _clickhouse_prewhere(text, ctx):
    if not re.search(r"(?i)\bPREWHERE\b", text):
        return text
    if re.search(r"(?i)\bWHERE\b", re.sub(r"(?i)\bPREWHERE\b", "", text)):
        ctx.warn("PREWHERE next to WHERE: PREWHERE is only an optimisation hint in ClickHouse; it was turned into a leading condition of WHERE")
        text = re.sub(r"(?is)\bPREWHERE\s+(.*?)(?=\s+WHERE\b)", lambda m: "WHERE " + m.group(1) + " AND ", text, count=1)
        text = re.sub(r"(?i)\bAND\s+\s*WHERE\b", "AND", text, count=1)
        return text
    return re.sub(r"(?i)\bPREWHERE\b", "WHERE", text)


# ------------------------------------------------------------------------------------------------------------------ functions
def _fn(name, *a):
    return f"{name}({', '.join(a)})"


def _lit(a):
    return a[1:-1] if len(a) >= 2 and a[0] == "'" and a[-1] == "'" else None


def _split_regex(sep):
    return "'" + re.sub(r"([.|+*?()\[\]{}^$\\])", r"\\\\\1", sep) + "'"


def _json_key(rw, path):
    k = _lit(path)
    if k is None:
        return None
    if k.startswith("$"):
        return "'" + k + "'"
    return "'$." + k + "'"


def _db2(rw, up, a):
    c = rw.c
    if up == "VALUE":
        return _fn("COALESCE", *a)
    if c.ddl:
        return None
    if up == "DAYS" and len(a) == 1:
        return f"(datediff({a[0]}, DATE '0001-01-01') + 1)"
    simple = {"INTEGER": "INT", "INT": "INT", "BIGINT": "BIGINT", "SMALLINT": "SMALLINT", "DOUBLE": "DOUBLE", "FLOAT": "DOUBLE", "REAL": "FLOAT"}
    if up in simple and len(a) == 1 and not re.fullmatch(r"\d+", a[0]):
        return f"CAST({a[0]} AS {simple[up]})"
    if up in ("DECIMAL", "DEC") and len(a) in (2, 3) and not re.fullmatch(r"\d+", a[0]):
        return f"CAST({a[0]} AS DECIMAL({a[1]}, {a[2] if len(a) == 3 else 0}))"
    if up == "VARCHAR" and len(a) in (1, 2) and not re.fullmatch(r"\d+", a[0]):
        return f"CAST({a[0]} AS STRING)"
    if up == "CHAR" and len(a) in (1, 2) and not re.fullmatch(r"\d+", a[0]):
        c.warn("CHAR(x) -> CAST(x AS STRING): for dates/timestamps DB2 uses its own ISO/USA/EUR text format, Spark gives yyyy-MM-dd")
        return f"CAST({a[0]} AS STRING)"
    if up == "DATE" and len(a) == 1:
        return _fn("to_date", a[0])
    if up == "TIMESTAMP" and len(a) == 1:
        return _fn("to_timestamp", a[0])
    if up == "TIMESTAMP" and len(a) == 2:
        c.todo("TIMESTAMP(date, time) has no direct conversion: use to_timestamp(concat(date, ' ', time))")
        return None
    if up in ("TIMESTAMP_FORMAT", "VARCHAR_FORMAT") and len(a) >= 2 and a[1].startswith("'"):
        from .sql_conv import fmt_convert
        return _fn("to_timestamp" if up == "TIMESTAMP_FORMAT" else "date_format", a[0], fmt_convert(a[1], "oracle"))
    if up == "POSSTR" and len(a) == 2:
        return _fn("instr", a[0], a[1])
    if up == "STRIP" and len(a) == 1:
        return _fn("trim", a[0])
    if up == "MIDNIGHT_SECONDS" and len(a) == 1:
        return f"(hour({a[0]}) * 3600 + minute({a[0]}) * 60 + second({a[0]}))"
    if up == "DAYOFWEEK_ISO" and len(a) == 1:
        return f"(weekday({a[0]}) + 1)"
    if up in ("WEEK_ISO", "WEEK") and len(a) == 1:
        return _fn("weekofyear", a[0])
    if up in ("TRUNCATE", "DIGITS", "TIMESTAMPDIFF"):
        c.todo(f"DB2 {up}() has no automatic conversion")
    return None


def _netezza(rw, up, a):
    c = rw.c
    if up == "STRPOS" and len(a) == 2:
        return _fn("instr", a[0], a[1])
    if up in ("HASH", "HASH4", "HASH8") and a:
        c.warn(f"{up}() -> hash(): the values differ from Netezza's, so do not mix them (joins, partitions)")
        return _fn("hash", a[0])
    if up == "BTRIM" and len(a) == 1:
        return _fn("trim", a[0])
    if up in ("AGE", "TIMEOFDAY"):
        c.todo(f"Netezza {up}() has no automatic conversion")
    return None


def _vertica(rw, up, a):
    c = rw.c
    if up == "TIMESTAMPADD" and len(a) == 3:
        return rw.dateadd(a[0].strip("'"), a[1], a[2])
    if up == "TIMESTAMPDIFF" and len(a) == 3:
        return rw.datediff(a[0].strip("'"), a[1], a[2])
    if up == "APPROXIMATE_COUNT_DISTINCT" and len(a) == 1:
        return _fn("approx_count_distinct", a[0])
    if up == "MEDIAN" and len(a) == 1:
        return f"percentile({a[0]}, 0.5)"
    if up == "REGEXP_SUBSTR" and len(a) == 2:
        return _fn("regexp_extract", a[0], a[1], "0")
    if up == "REGEXP_SUBSTR" or up == "REGEXP_COUNT":
        c.todo(f"Vertica {up}() with position/occurrence arguments has no automatic conversion")
    if up == "STRING_TO_ARRAY" and len(a) in (1, 2):
        sep = _lit(a[1]) if len(a) == 2 else ","
        return _fn("split", a[0], _split_regex(sep)) if sep is not None else None
    if up == "HASH" and a:
        c.warn("HASH() -> hash(): the values differ from Vertica's")
        return _fn("hash", a[0])
    if up in ("CLOCK_TIMESTAMP", "STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP") and not a:
        return "current_timestamp()"
    return None


_CH_TRUNC = {"TOSTARTOFMONTH": ("trunc", "MONTH"), "TOSTARTOFQUARTER": ("trunc", "QUARTER"), "TOSTARTOFYEAR": ("trunc", "YEAR"), "TOMONDAY": ("trunc", "WEEK"),
             "TOSTARTOFDAY": ("date_trunc", "DAY"), "TOSTARTOFHOUR": ("date_trunc", "HOUR"), "TOSTARTOFMINUTE": ("date_trunc", "MINUTE"),
             "TOSTARTOFSECOND": ("date_trunc", "SECOND"), "TOSTARTOFWEEK": ("date_trunc", "WEEK")}
_CH_CAST = {"TOSTRING": "STRING", "TOINT8": "TINYINT", "TOINT16": "SMALLINT", "TOINT32": "INT", "TOINT64": "BIGINT", "TOUINT8": "SMALLINT", "TOUINT16": "INT",
            "TOUINT32": "BIGINT", "TOUINT64": "DECIMAL(20,0)", "TOFLOAT32": "FLOAT", "TOFLOAT64": "DOUBLE", "TOBOOL": "BOOLEAN"}
_CH_PART = {"TOYEAR": "year", "TOMONTH": "month", "TODAYOFMONTH": "day", "TOHOUR": "hour", "TOMINUTE": "minute", "TOSECOND": "second", "TOQUARTER": "quarter",
            "TODAYOFYEAR": "dayofyear", "TOWEEK": "weekofyear", "TOISOWEEK": "weekofyear"}
_CH_ADD = {"ADDDAYS": ("date_add", 1), "SUBTRACTDAYS": ("date_sub", 1), "ADDMONTHS": ("add_months", 1), "SUBTRACTMONTHS": ("add_months", -1),
           "ADDYEARS": ("add_months", 12), "SUBTRACTYEARS": ("add_months", -12)}
_CH_IF = {"SUMIF": "sum", "AVGIF": "avg", "MINIF": "min", "MAXIF": "max"}


def _clickhouse(rw, up, a):
    c = rw.c
    if up in _CH_TRUNC and len(a) == 1:
        f, unit = _CH_TRUNC[up]
        if up == "TOSTARTOFWEEK":
            c.warn("toStartOfWeek: ClickHouse's default week starts on Sunday, Spark's date_trunc('WEEK') on Monday")
        return f"trunc({a[0]}, '{unit}')" if f == "trunc" else f"date_trunc('{unit}', {a[0]})"
    if up in _CH_CAST and len(a) == 1:
        return f"CAST({a[0]} AS {_CH_CAST[up]})"
    if up == "TODATE" and len(a) == 1:
        return _fn("to_date", a[0])
    if up in ("TODATETIME", "TODATETIME64", "PARSEDATETIMEBESTEFFORT", "PARSEDATETIME64BESTEFFORT") and a:
        return _fn("to_timestamp", a[0])
    if up in ("TODECIMAL32", "TODECIMAL64", "TODECIMAL128") and len(a) == 2:
        return f"CAST({a[0]} AS DECIMAL(38, {a[1]}))"
    if up == "TOUNIXTIMESTAMP" and len(a) == 1:
        return _fn("unix_timestamp", a[0])
    if up == "FROMUNIXTIMESTAMP" and len(a) == 1:
        return _fn("from_unixtime", a[0])
    if up in _CH_PART and len(a) == 1:
        return _fn(_CH_PART[up], a[0])
    if up == "TODAYOFWEEK" and len(a) == 1:
        return f"(weekday({a[0]}) + 1)"
    if up in ("DATEDIFF", "DATE_DIFF") and len(a) == 3:
        return rw.datediff(a[0].strip("'"), a[1], a[2])
    if up in _CH_ADD and len(a) == 2:
        f, k = _CH_ADD[up]
        if f == "add_months":
            return f"add_months({a[0]}, {k} * ({a[1]}))" if abs(k) != 1 else f"add_months({a[0]}, {'-' if k < 0 else ''}({a[1]}))"
        return _fn(f, a[0], a[1])
    if up == "FORMATDATETIME" and len(a) >= 2 and a[1].startswith("'"):
        from .sql_conv import fmt_convert
        return _fn("date_format", a[0], fmt_convert(a[1], "mysql"))
    if up == "TODAY" and not a:
        return "current_date()"
    if up == "YESTERDAY" and not a:
        return "date_sub(current_date(), 1)"
    if up in _CH_IF and len(a) == 2:
        return f"{_CH_IF[up]}(CASE WHEN {a[1]} THEN {a[0]} END)"
    if up in ("UNIQ", "UNIQCOMBINED", "UNIQHLL12", "UNIQTHETA") and a:
        return _fn("approx_count_distinct", a[0]) if len(a) == 1 else None
    if up == "UNIQEXACT" and len(a) == 1:
        return f"count(DISTINCT {a[0]})"
    if up == "ANY" and len(a) == 1:
        return _fn("first", a[0])
    if up == "ANYLAST" and len(a) == 1:
        return _fn("last", a[0])
    if up == "ARGMAX" and len(a) == 2:
        return _fn("max_by", *a)
    if up == "ARGMIN" and len(a) == 2:
        return _fn("min_by", *a)
    if up == "GROUPARRAY" and len(a) == 1:
        return _fn("collect_list", a[0])
    if up == "GROUPUNIQARRAY" and len(a) == 1:
        return _fn("collect_set", a[0])
    if up == "ARRAYJOIN" and len(a) == 1:
        c.warn("arrayJoin(x) -> explode(x): only valid in the select list, one generator per query")
        return _fn("explode", a[0])
    if up == "ARRAYMAP" and len(a) == 2:
        return _fn("transform", a[1], a[0])
    if up == "ARRAYFILTER" and len(a) == 2:
        return _fn("filter", a[1], a[0])
    if up == "HAS" and len(a) == 2:
        return _fn("array_contains", *a)
    if up == "ARRAYSORT" and len(a) == 1:
        return _fn("sort_array", a[0])
    if up == "ARRAYDISTINCT" and len(a) == 1:
        return _fn("array_distinct", a[0])
    if up == "ARRAYSUM" and len(a) == 1:
        return f"aggregate({a[0]}, 0, (acc, x) -> acc + x)"
    if up == "ARRAYSTRINGCONCAT" and len(a) in (1, 2):
        return _fn("array_join", *a) if len(a) == 2 else _fn("array_join", a[0], "''")
    if up in ("SPLITBYCHAR", "SPLITBYSTRING") and len(a) == 2:
        sep = _lit(a[0])
        return _fn("split", a[1], _split_regex(sep)) if sep is not None else None
    if up == "LENGTHUTF8" and len(a) == 1:
        return _fn("length", a[0])
    if up == "POSITION" and len(a) == 2:
        return _fn("instr", a[0], a[1])
    if up == "MULTIIF" and len(a) >= 3 and len(a) % 2 == 1:
        body = " ".join(f"WHEN {a[i]} THEN {a[i + 1]}" for i in range(0, len(a) - 1, 2))
        return f"CASE {body} ELSE {a[-1]} END"
    if up == "INTDIV" and len(a) == 2:
        return f"({a[0]} DIV {a[1]})"
    if up in ("GENERATEUUIDV4",) and not a:
        return "uuid()"
    if up in ("CITYHASH64", "SIPHASH64", "XXHASH64", "XXHASH32", "HALFMD5") and len(a) == 1:
        c.warn(f"{up}() -> xxhash64(): the values differ from ClickHouse's")
        return _fn("xxhash64", a[0])
    if up in ("JSONEXTRACTSTRING", "JSONEXTRACTRAW", "JSONEXTRACTINT", "JSONEXTRACTFLOAT") and len(a) == 2:
        key = _json_key(rw, a[1])
        if key is None:
            return None
        g = _fn("get_json_object", a[0], key)
        return {"JSONEXTRACTINT": f"CAST({g} AS BIGINT)", "JSONEXTRACTFLOAT": f"CAST({g} AS DOUBLE)"}.get(up, g)
    if up == "QUANTILE" or up.startswith("QUANTILE"):
        c.todo("parametric aggregate quantile(level)(x) is only converted in the form quantile(0.5)(x)")
    return None


def _hana(rw, up, a):
    c = rw.c
    if up == "ADD_DAYS" and len(a) == 2:
        return _fn("date_add", *a)
    if up == "ADD_YEARS" and len(a) == 2:
        return f"add_months({a[0]}, 12 * ({a[1]}))"
    if up == "ADD_SECONDS" and len(a) == 2:
        return f"({a[0]} + ({a[1]}) * INTERVAL 1 SECOND)"
    if up == "DAYS_BETWEEN" and len(a) == 2:
        return _fn("datediff", a[1], a[0])
    if up == "SECONDS_BETWEEN" and len(a) == 2:
        return f"(unix_timestamp({a[1]}) - unix_timestamp({a[0]}))"
    if up == "TO_NVARCHAR" and len(a) == 1:
        return f"CAST({a[0]} AS STRING)"
    if up == "TO_NVARCHAR" and len(a) == 2 and a[1].startswith("'"):
        from .sql_conv import fmt_convert
        return _fn("date_format", a[0], fmt_convert(a[1], "oracle"))
    if up in ("TO_INT", "TO_INTEGER") and len(a) == 1:
        return f"CAST({a[0]} AS INT)"
    if up == "TO_BIGINT" and len(a) == 1:
        return f"CAST({a[0]} AS BIGINT)"
    if up == "TO_DOUBLE" and len(a) == 1:
        return f"CAST({a[0]} AS DOUBLE)"
    if up == "TO_DECIMAL" and len(a) in (1, 3):
        return f"CAST({a[0]} AS DECIMAL({a[1]}, {a[2]}))" if len(a) == 3 else f"CAST({a[0]} AS DECIMAL(38, 10))"
    if up == "TO_DATS" and len(a) == 1:
        return _fn("date_format", a[0], "'yyyyMMdd'")
    if up == "SYSUUID" and not a:
        return "uuid()"
    if up == "LOCATE" and len(a) == 2:
        return _fn("locate", a[1], a[0])
    if up in ("SUBSTR_BEFORE", "SUBSTR_AFTER"):
        c.todo(f"HANA {up}() has no direct conversion: use substring_index / split")
    return None


def _informix(rw, up, a):
    if up == "MDY" and len(a) == 3:
        return _fn("make_date", a[2], a[0], a[1])
    if up == "WEEKDAY" and len(a) == 1:
        return f"(dayofweek({a[0]}) - 1)"
    if up == "DATE" and len(a) == 1 and not rw.c.ddl:
        return _fn("to_date", a[0])
    return None


def _firebird(rw, up, a):
    c = rw.c
    if up == "LIST" and len(a) in (1, 2):
        return _fn("array_join", _fn("collect_list", a[0]), a[1] if len(a) == 2 else "','")
    if up == "GEN_ID":
        c.todo("GEN_ID (generator) has no Spark equivalent: use monotonically_increasing_id() or an explicit sequence")
    return None


_IMPALA_UNITS = {"DAYS": "DAY", "WEEKS": "WEEK", "HOURS": "HOUR", "MINUTES": "MINUTE", "SECONDS": "SECOND", "MILLISECONDS": "MILLISECOND", "MICROSECONDS": "MICROSECOND"}


def _impala(rw, up, a):
    m = re.fullmatch(r"(\w+?)_(ADD|SUB)", up)
    if m and len(a) == 2 and m.group(1) in ("DAYS", "WEEKS", "MONTHS", "YEARS", "HOURS", "MINUTES", "SECONDS", "MILLISECONDS", "MICROSECONDS"):
        unit, add = m.group(1), m.group(2) == "ADD"
        n = a[1] if add else f"-({a[1]})"
        if unit == "DAYS":
            return _fn("date_add", a[0], a[1]) if add else _fn("date_sub", a[0], a[1])
        if unit == "WEEKS":
            return _fn("date_add", a[0], f"7 * ({a[1]})") if add else _fn("date_sub", a[0], f"7 * ({a[1]})")
        if unit == "MONTHS":
            return _fn("add_months", a[0], a[1] if add else f"-({a[1]})")
        if unit == "YEARS":
            return _fn("add_months", a[0], f"12 * ({a[1]})" if add else f"-12 * ({a[1]})")
        return f"({a[0]} {'+' if add else '-'} ({a[1]}) * INTERVAL 1 {_IMPALA_UNITS[unit]})"
    if up == "INT_MONTHS_BETWEEN" and len(a) == 2:
        return f"CAST(months_between({a[0]}, {a[1]}) AS INT)"
    if up == "FROM_TIMESTAMP" and len(a) == 2:
        return _fn("date_format", *a)
    if up == "STRLEFT" and len(a) == 2:
        return _fn("left", *a)
    if up == "STRRIGHT" and len(a) == 2:
        return _fn("right", *a)
    if up == "ISNULL" and len(a) == 2:
        return _fn("COALESCE", *a)
    if up == "NDV" and len(a) == 1:
        return _fn("approx_count_distinct", a[0])
    if up == "APPX_MEDIAN" and len(a) == 1:
        return _fn("percentile_approx", a[0], "0.5")
    return None


def _trino(rw, up, a):
    """Trino / Presto / Athena functions beyond the generic ones"""
    from .sql_conv import fmt_convert
    if up == "DATE_FORMAT" and len(a) == 2 and a[1].startswith("'"):
        return _fn("date_format", a[0], fmt_convert(a[1], "mysql"))
    if up == "DATE_PARSE" and len(a) == 2 and a[1].startswith("'"):
        return _fn("to_timestamp", a[0], fmt_convert(a[1], "mysql"))
    if up == "FORMAT_DATETIME" and len(a) == 2:
        return _fn("date_format", *a)
    if up == "PARSE_DATETIME" and len(a) == 2:
        return _fn("to_timestamp", *a)
    if up == "TO_UNIXTIME" and len(a) == 1:
        return _fn("unix_timestamp", a[0])
    if up == "FROM_ISO8601_DATE" and len(a) == 1:
        return _fn("to_date", a[0])
    if up == "FROM_ISO8601_TIMESTAMP" and len(a) == 1:
        return _fn("to_timestamp", a[0])
    if up in ("JSON_EXTRACT_SCALAR", "JSON_EXTRACT") and len(a) == 2 and a[1].startswith("'"):
        return _fn("get_json_object", *a)
    if up == "ARBITRARY" and len(a) == 1:
        return _fn("first", a[0])
    if up == "MAP_AGG" and len(a) == 2:
        return f"map_from_entries(collect_list(struct({a[0]}, {a[1]})))"
    if up == "CONTAINS" and len(a) == 2:
        return _fn("array_contains", *a)
    if up == "LEVENSHTEIN_DISTANCE" and len(a) == 2:
        return _fn("levenshtein", *a)
    if up in ("DAY_OF_WEEK", "DOW") and len(a) == 1:
        return f"(weekday({a[0]}) + 1)"
    if up == "TRY" and len(a) == 1:
        rw.c.todo("TRY(expr) has no direct equivalent: use try_cast / try_divide / try_element_at, or when() guards")
    return None


_FUNCS = {"db2": _db2, "netezza": _netezza, "vertica": _vertica, "clickhouse": _clickhouse, "hana": _hana, "informix": _informix, "firebird": _firebird,
          "impala": _impala, "trino": _trino, "presto": _trino}


def function(rw, up, a):
    """dialect-specific function mapping -> Spark SQL text, or None"""
    f = _FUNCS.get(rw.c.d)
    return f(rw, up, a) if f else None


CH_TYPES = {"INT8": "TINYINT", "INT16": "SMALLINT", "INT32": "INT", "INT64": "BIGINT", "UINT8": "SMALLINT", "UINT16": "INT", "UINT32": "BIGINT",
            "UINT64": "DECIMAL(20,0)", "FLOAT32": "FLOAT", "FLOAT64": "DOUBLE", "STRING": "STRING", "FIXEDSTRING": "STRING", "DATETIME": "TIMESTAMP",
            "DATETIME64": "TIMESTAMP", "DATE32": "DATE", "UUID": "STRING", "BOOL": "BOOLEAN"}


def alias_note(name):
    base = _ALIAS_NOTE.get(name)
    return f"dialect {name}: converted with the {base} rules (plus {name}-specific clean-ups); functions only {name} has are passed through unchanged" if base else None


_CONSTRAINT = re.compile(r"(?i)^\s*(?:CONSTRAINT|PRIMARY|FOREIGN|UNIQUE|CHECK|KEY|INDEX|FULLTEXT|SPATIAL|PERIOD|LIKE)\b")


def ddl_types(text, ctx, types):
    """CREATE TABLE (col type, ...): map each column's type to its Spark name (the converter used to leave column types untouched)"""
    m = re.match(r"(?is)(\s*(?:--[^\n]*\n\s*)*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:LOCAL\s+|GLOBAL\s+)?(?:TEMP|TEMPORARY)\s+|EXTERNAL\s+|MULTISET\s+|SET\s+|VOLATILE\s+)*TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^\s(]+\s*)\(", text)
    if not m:
        return text
    open_i = m.end() - 1
    close = _match_close(text, open_i)
    if close < 0:
        return text
    inner = text[open_i + 1:close]
    items, depth, cur, q = [], 0, [], None
    for ch in inner:
        if q:
            cur.append(ch)
            if ch == q:
                q = None
            continue
        if ch in "'\"`":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    items.append("".join(cur))
    out, changed = [], False
    for it in items:
        mm = None if _CONSTRAINT.match(it) else re.match(r"(?s)^(\s*(?:`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[\w$#]+)\s+)([A-Za-z_]\w*)(\s*\((?:[^()]|\([^()]*\))*\))?(.*)$", it)
        if mm:
            t = types.get(mm.group(2).upper())
            if t is not None and t != mm.group(2).upper():
                args = mm.group(3) or ""
                if t == "DECIMAL":
                    t = "DECIMAL" + (args.replace(" ", "") if args else "")
                it = f"{mm.group(1)}{t}{mm.group(4)}"
                changed = True
            elif mm.group(2).upper() in ("VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2") and not mm.group(3):
                it = f"{mm.group(1)}STRING{mm.group(4)}"
                changed = True
        out.append(it)
    if not changed:
        return text
    return text[:open_i + 1] + ",".join(out) + text[close:]
