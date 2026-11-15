"""SQL dialect -> Spark SQL (and optionally wrapped in PySpark spark.sql calls).

Token based, so strings, quoted identifiers and comments are never touched.
Supported source dialects: tsql, oracle, mysql, postgres, snowflake, bigquery, redshift, duckdb, hive, trino, presto, sqlite, teradata, spark, databricks,
generic, and (sql_dialects.py) mariadb, singlestore, starrocks, doris, tidb, greenplum, cockroachdb, yugabyte, alloydb, synapse, sybase, azuresql, athena,
db2, netezza, vertica, clickhouse, hana, informix, firebird, impala.
"""
from __future__ import annotations

import re

from .core import Report, cm

TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<comment>--[^\n]*|/\*.*?\*/)
  | (?P<str>'(?:[^']|'')*')
  | (?P<dq>"(?:[^"]|"")*")
  | (?P<bt>`[^`]*`)
  | (?P<br>\[[^\]]*\])
  | (?P<num>\d+(?:\.\d+)?)
  | (?P<word>[A-Za-z_#@$][A-Za-z_0-9$#]*)
  | (?P<op>::|<=|>=|<>|!=|\|\||->>|->|.)
""", re.X | re.S)

TYPES = {
    "VARCHAR": "STRING", "NVARCHAR": "STRING", "CHAR": "STRING", "NCHAR": "STRING", "TEXT": "STRING",
    "NTEXT": "STRING", "CLOB": "STRING", "VARCHAR2": "STRING", "NVARCHAR2": "STRING",
    "NUMBER": "DECIMAL", "NUMERIC": "DECIMAL", "DATETIME": "TIMESTAMP", "DATETIME2": "TIMESTAMP",
    "SMALLDATETIME": "TIMESTAMP", "TIMESTAMPTZ": "TIMESTAMP", "TIMESTAMP_NTZ": "TIMESTAMP",
    "TIMESTAMP_LTZ": "TIMESTAMP", "INT64": "BIGINT", "FLOAT64": "DOUBLE", "BOOL": "BOOLEAN",
    "BIT": "BOOLEAN", "MONEY": "DECIMAL(19,4)", "INT4": "INT", "INT8": "BIGINT", "FLOAT8": "DOUBLE",
    "FLOAT4": "FLOAT", "BYTEA": "BINARY", "VARBINARY": "BINARY", "BYTES": "BINARY", "SERIAL": "INT",
    "BIGSERIAL": "BIGINT", "REAL": "FLOAT", "LONG": "BIGINT", "BYTEINT": "TINYINT", "CLOB": "STRING", "NCLOB": "STRING",
    "INT2": "SMALLINT", "DATETIMEOFFSET": "TIMESTAMP", "UNIQUEIDENTIFIER": "STRING", "XML": "STRING", "JSON": "STRING", "JSONB": "STRING", "UUID": "STRING",
    "SMALLMONEY": "DECIMAL(10,4)", "IMAGE": "BINARY", "TINYTEXT": "STRING", "MEDIUMTEXT": "STRING", "LONGTEXT": "STRING", "MEDIUMINT": "INT", "RAW": "BINARY",
    "BLOB": "BINARY", "LONGBLOB": "BINARY", "MEDIUMBLOB": "BINARY", "TINYBLOB": "BINARY", "STRING": "STRING",
}
ORA_PG_FMT = [("YYYY", "yyyy"), ("YY", "yy"), ("MONTH", "MMMM"), ("MON", "MMM"), ("MM", "MM"),
              ("DDD", "DDD"), ("DD", "dd"), ("DAY", "EEEE"), ("DY", "EEE"), ("HH24", "HH"),
              ("HH12", "hh"), ("HH", "hh"), ("MI", "mm"), ("SS", "ss"), ("FF3", "SSS"),
              ("FF6", "SSSSSS"), ("AM", "a"), ("PM", "a")]
MYSQL_FMT = {"%Y": "yyyy", "%y": "yy", "%m": "MM", "%d": "dd", "%e": "d", "%H": "HH", "%h": "hh",
             "%I": "hh", "%i": "mm", "%s": "ss", "%S": "ss", "%M": "MMMM", "%b": "MMM", "%a": "EEE",
             "%W": "EEEE", "%p": "a", "%j": "DDD", "%f": "SSSSSS"}
STRFTIME_FMT = dict(MYSQL_FMT, **{"%M": "mm", "%B": "MMMM", "%A": "EEEE", "%T": "HH:mm:ss", "%F": "yyyy-MM-dd"})
PARTS = {"YEAR", "YY", "YYYY", "QUARTER", "MONTH", "MM", "M", "WEEK", "WK", "DAY", "DD", "D", "HOUR", "HH",
         "MINUTE", "MI", "N", "SECOND", "SS", "S"}


_EXTRA_FUNCS = {"db2", "netezza", "vertica", "clickhouse", "hana", "informix", "firebird", "impala", "trino", "presto"}


def tokenize(sql: str):
    toks = []
    for m in TOKEN.finditer(sql):
        toks.append((m.lastgroup, m.group()))
    return toks


class Ctx:
    def __init__(self, dialect, report, bq_views=False):
        from .sql_dialects import base_dialect
        self.name = dialect               # as asked for (mariadb, db2, ...)
        self.d = base_dialect(dialect)    # the rule set used (mariadb -> mysql); engines with their own rules keep their name
        self.ddl = False                  # the statement being converted is CREATE / ALTER
        self.r = report
        self.line = 1
        self.bq_views = bq_views          # BigQuery: map project.dataset.table -> temp view dataset_table
        self.bq_tables = []               # raw ids of referenced BigQuery tables

    def warn(self, msg):
        self.r.warn(self.line, msg)

    def todo(self, msg):
        self.r.todo(self.line, msg)


def fmt_convert(fmt_lit: str, style: str) -> str:
    """fmt_lit includes the quotes."""
    body = fmt_lit[1:-1]
    if style == "oracle":
        out, i = "", 0
        while i < len(body):
            for a, b in ORA_PG_FMT:
                if body[i:i + len(a)].upper() == a and body[i:i + len(a)] == body[i:i + len(a)].upper():
                    out += b
                    i += len(a)
                    break
            else:
                out += body[i]
                i += 1
        return "'" + out + "'"
    table = MYSQL_FMT if style == "mysql" else STRFTIME_FMT
    return "'" + re.sub(r"%[A-Za-z]", lambda m: table.get(m.group(), m.group()), body) + "'"


def split_args(toks):
    """split a token list (inside parens) on top-level commas -> list of token lists"""
    out, cur, depth = [], [], 0
    for t in toks:
        if t[1] in "([" and t[0] == "op":
            depth += 1
        elif t[1] in ")]" and t[0] == "op":
            depth -= 1
        if t == ("op", ",") and depth == 0:
            out.append(cur)
            cur = []
        else:
            cur.append(t)
    out.append(cur)
    return out if toks else []


def txt(toks) -> str:
    return "".join(t[1] for t in toks).strip()


class Rewriter:
    def __init__(self, ctx: Ctx):
        self.c = ctx
        self._memo: dict = {}

    # ---- driver ---------------------------------------------------------
    def render(self, toks) -> str:
        key = tuple(toks)
        hit = self._memo.get(key)
        if hit is None:
            hit = self._memo[key] = self._render(toks)
        return hit

    def _render(self, toks) -> str:
        c = self.c
        out = []
        i, n = 0, len(toks)
        while i < n:
            kind, val = toks[i]
            c.line += val.count("\n") if kind in ("ws", "comment") else 0
            if kind == "dq":
                if c.d == "mysql":
                    out.append("'" + val[1:-1].replace("'", "''") + "'")
                elif c.d in ("spark", "bigquery"):
                    out.append(val)
                else:
                    out.append("`" + val[1:-1].replace('""', '"') + "`")
            elif kind == "br" and c.d == "tsql":
                out.append("`" + val[1:-1] + "`")
            elif kind == "bt" and c.d == "bigquery" and "." in val:
                out.append(self.bq_table(val[1:-1]))
            elif kind == "word" and c.d == "bigquery" and val in ("r", "R") and i + 1 < n and toks[i + 1][0] == "str":
                out.append(toks[i + 1][1].replace("\\", "\\\\"))     # raw string r'..' -> escaped literal
                i += 2
                continue
            elif kind == "word" and c.d == "bigquery" and val.startswith("@") and len(val) > 1:
                out.append(":" + val[1:])
            elif kind == "word":
                up = val.upper()
                # function call?
                j = i + 1
                while j < n and toks[j][0] == "ws":
                    j += 1
                if j < n and toks[j] == ("op", "(") and up not in ("IN", "AS", "ON", "AND", "OR", "NOT", "FROM", "JOIN", "WHERE", "VALUES", "SELECT", "BY", "OVER", "USING"):
                    end = self.match(toks, j)
                    inner = toks[j + 1:end]
                    # postfix clauses
                    k = end + 1
                    rep, consumed = self.function(up, val, inner, toks, end)
                    if rep is not None:
                        out.append(rep)
                        i = consumed + 1
                        continue
                    out.append(val + "(" + self.render(inner) + ")")
                    i = end + 1
                    continue
                if up in ("SYSDATE", "SYSTIMESTAMP", "GETDATE", "SYSDATETIME", "LOCALTIMESTAMP") and c.d != "spark":
                    out.append("current_timestamp()")
                elif up == "ROWNUM":
                    c.todo("ROWNUM in this form (with GROUP BY/DISTINCT/aggregates/OR/set operations at the same level, or compared with >, >= or = n>1) has no automatic conversion: use row_number() OVER (...) or LIMIT")
                    out.append(val)
                else:
                    out.append(val)
            elif kind == "op" and val == "::":
                # postgres style cast: previous operand :: type
                typ, i2 = self.read_type(toks, i + 1)
                prev = self.pop_operand(out)
                out.append(f"CAST({prev} AS {typ})")
                i = i2
                continue
            elif kind == "op" and val in ("->>", "->"):
                nk = next((toks[q] for q in range(i + 1, n) if toks[q][0] != "ws"), ("", ""))
                if val == "->>" or nk[0] in ("str", "num"):           # `x -> x * 2` is a lambda (Trino, ClickHouse, Spark), not a JSON operator
                    c.todo("JSON operators (->, ->>): use get_json_object(col, '$.key') or from_json")
                out.append(val)
            elif kind == "op" and val == "+" and False:
                out.append(val)
            else:
                out.append(val)
            i += 1
        return "".join(out)

    def bq_table(self, ident):
        c = self.c
        parts = ident.split(".")
        if ident not in c.bq_tables:
            c.bq_tables.append(ident)
        if len(parts) >= 2:
            ds, tbl = parts[-2], parts[-1]
        else:
            return "`" + ident + "`"
        if c.bq_views:
            return f"`{ds}_{tbl}`".replace("-", "_")
        if len(parts) == 3:
            c.warn(f"project id `{parts[0]}` dropped from table reference - dataset becomes a Spark database/schema")
        return f"`{ds}`.`{tbl}`"

    def match(self, toks, start):
        depth = 0
        for k in range(start, len(toks)):
            if toks[k] == ("op", "("):
                depth += 1
            elif toks[k] == ("op", ")"):
                depth -= 1
                if depth == 0:
                    return k
        return len(toks) - 1

    def read_type(self, toks, i):
        while i < len(toks) and toks[i][0] == "ws":
            i += 1
        name = toks[i][1]
        i += 1
        args = ""
        if i < len(toks) and toks[i] == ("op", "("):
            e = self.match(toks, i)
            args = "(" + txt(toks[i + 1:e]) + ")"
            i = e + 1
        return self.map_type(name, args), i

    def map_type(self, name, args=""):
        up = name.upper()
        if self.c.d == "clickhouse":
            from .sql_dialects import CH_TYPES
            t = CH_TYPES.get(up)
            if t is not None:
                return t
        t = TYPES.get(up)
        if t is None:
            return name + args
        if t in ("STRING", "BINARY") or t.startswith("DECIMAL(") or t in ("TIMESTAMP", "BOOLEAN", "BIGINT", "DOUBLE", "INT", "FLOAT"):
            return t
        if t == "DECIMAL":
            return "DECIMAL" + args
        return t

    def pop_operand(self, out):
        # take the trailing operand (identifier / literal / parenthesised group)
        s = "".join(out)
        m = re.search(r"(\([^()]*\)|[\w.`'\"$@#\[\]]+(\([^()]*\))?)\s*$", s)
        if not m:
            return ""
        out.clear()
        out.append(s[:m.start()])
        return m.group(1)

    # ---- functions --------------------------------------------------------
    def function(self, up, orig, inner, toks, end):
        c = self.c
        d = c.d
        args = [self.render(a).strip() for a in split_args(inner)]
        raw = split_args(inner)
        nxt = end

        def fn(name, *a):
            return f"{name}({', '.join(a)})"

        if d in _EXTRA_FUNCS:
            from .sql_dialects import function as dialect_function
            r = dialect_function(self, up, args)
            if r is not None:
                return r, nxt
        # ---- generic / cross-dialect
        if up in ("ISNULL",) and d in ("tsql", "netezza", "vertica") and len(args) == 2:
            return fn("COALESCE", *args), nxt
        if up == "NVL":
            return fn("COALESCE", *args), nxt
        if up == "NVL2" and len(args) == 3:
            return f"IF({args[0]} IS NOT NULL, {args[1]}, {args[2]})", nxt
        if up == "IFF" or up == "IIF":
            return fn("IF", *args), nxt
        if up in ("LEN", "DATALENGTH") and len(args) == 1:
            return fn("LENGTH", *args), nxt
        if up in ("GETDATE", "SYSDATETIME", "NOW", "SYSDATE", "CURRENT_TIMESTAMP") and not args and d != "spark":
            return "current_timestamp()", nxt
        if up == "CURDATE" and not args:
            return "current_date()", nxt
        if up in ("RANDOM",) and not args:
            return "rand()", nxt
        if up == "CHARINDEX" and len(args) == 2:
            return fn("INSTR", args[1], args[0]), nxt
        if up == "CHARINDEX":
            c.todo("CHARINDEX with start position: use locate(substr, str, pos)")
            return fn("locate", args[0], args[1], args[2]), nxt
        if up == "DECODE" and d in ("oracle", "db2", "netezza", "vertica", "informix") and len(args) >= 3:
            e, rest = args[0], args[1:]
            body = " ".join(f"WHEN {rest[i]} THEN {rest[i + 1]}" for i in range(0, len(rest) - 1, 2))
            els = f" ELSE {rest[-1]}" if len(rest) % 2 else ""
            return f"CASE {e} {body}{els} END", nxt
        if up in ("STRING_AGG", "GROUP_CONCAT", "LISTAGG", "WM_CONCAT"):
            return self.string_agg(up, raw, args, toks, end)
        if up == "ARRAY_AGG":
            return self.array_agg(inner, toks, end)
        if up == "COUNTIF":
            return fn("count_if", *args), nxt
        if up == "SAFE_DIVIDE" and len(args) == 2:
            return f"({args[0]} / NULLIF({args[1]}, 0))", nxt
        if up == "TO_VARCHAR" and len(args) == 1:
            return f"CAST({args[0]} AS STRING)", nxt
        if up == "TO_VARCHAR" and len(args) == 2 and args[1].startswith("'"):
            c.warn("TO_VARCHAR(x, 'fmt') treated as a date/time format; for numbers use format_number()/printf instead")
            return fn("date_format", args[0], fmt_convert(args[1], "oracle")), nxt
        if up in ("CONVERT",) and d == "tsql" and len(args) >= 2:
            typ = self.map_type(args[0].split("(")[0], "(" + args[0].split("(", 1)[1] if "(" in args[0] else "")
            if len(args) == 3 and args[2].strip().isdigit():
                style = {"23": "yyyy-MM-dd", "120": "yyyy-MM-dd HH:mm:ss", "121": "yyyy-MM-dd HH:mm:ss.SSS",
                         "112": "yyyyMMdd", "126": "yyyy-MM-dd'T'HH:mm:ss"}.get(args[2].strip())
                if style and typ == "STRING":
                    return f"date_format({args[1]}, '{style.replace(chr(39), chr(39) * 2)}')", nxt
                c.todo(f"CONVERT style {args[2]} not converted - use date_format/to_date explicitly")
            return f"CAST({args[1]} AS {typ})", nxt
        if up == "TRY_CONVERT" and d == "tsql" and len(args) >= 2:
            return f"TRY_CAST({args[1]} AS {self.map_type(args[0].split('(')[0])})", nxt
        # casts
        if up in ("CAST", "TRY_CAST"):
            m = re.match(r"(?is)(.*)\s+AS\s+(\w+)\s*(\(.*\))?\s*$", self.render(inner).strip())
            if m:
                return f"{orig}({m.group(1)} AS {self.map_type(m.group(2), m.group(3) or '')})", nxt
            return None, nxt
        # ---- date/time
        if up == "DATEADD" and len(args) == 3:
            return self.dateadd(args[0], args[1], args[2]), nxt
        if up == "DATEDIFF" and d in ("tsql", "snowflake", "redshift", "vertica", "firebird") and len(args) == 3:
            return self.datediff(args[0].strip("'"), args[1], args[2]), nxt
        if up == "DATE_DIFF" and d == "bigquery" and len(args) == 3:
            return self.datediff(args[2], args[1], args[0]), nxt
        if up == "DATE_DIFF" and d in ("trino", "presto", "duckdb") and len(args) == 3:
            return self.datediff(args[0].strip("'"), args[1], args[2]), nxt
        if up == "DATE_ADD" and d in ("trino", "presto") and len(args) == 3:
            return self.dateadd(args[0].strip("'"), args[1], args[2]), nxt
        if d == "bigquery":
            r = self.bigquery_function(up, orig, args, raw, nxt)
            if r is not None:
                return r
        if up == "APPROX_DISTINCT":
            return fn("approx_count_distinct", *args), nxt
        if up == "CARDINALITY":
            return fn("size", *args), nxt
        if up in ("ZEROIFNULL",):
            return f"COALESCE({args[0]}, 0)", nxt
        if up == "NULLIFZERO":
            return f"NULLIF({args[0]}, 0)", nxt
        if up == "CHARACTERS" and d == "teradata":
            return fn("LENGTH", *args), nxt
        if up == "LIST" and d == "duckdb":
            return fn("collect_list", *args), nxt
        if up == "STRFTIME" and len(args) == 2 and d == "duckdb" and args[1].startswith("'"):
            return fn("date_format", args[0], fmt_convert(args[1], "strftime")), nxt
        if up == "STRFTIME" and len(args) == 2 and d == "sqlite" and args[0].startswith("'"):
            return fn("date_format", args[1], fmt_convert(args[0], "strftime")), nxt
        if up == "DATETIME" and d == "sqlite" and args == ["'now'"]:
            return "current_timestamp()", nxt
        if up == "DATE" and d == "sqlite" and args == ["'now'"]:
            return "current_date()", nxt
        if up == "DATEPART" and len(args) == 2:
            p = args[0].upper()
            m = {"YEAR": "year", "YY": "year", "YYYY": "year", "MONTH": "month", "MM": "month", "DAY": "day",
                 "DD": "day", "HOUR": "hour", "MINUTE": "minute", "SECOND": "second", "QUARTER": "quarter",
                 "WEEK": "weekofyear", "DAYOFYEAR": "dayofyear"}.get(p)
            if m:
                return fn(m, args[1]), nxt
            c.todo(f"DATEPART({args[0]}) not converted")
            return None, nxt
        if up in ("DATE_ADD", "DATE_SUB", "ADDDATE", "SUBDATE") and d == "mysql" and len(args) == 2:
            m = re.match(r"(?is)INTERVAL\s+(.+?)\s+(\w+)$", args[1])
            if m:
                sign = "+" if up in ("DATE_ADD", "ADDDATE") else "-"
                return f"({args[0]} {sign} ({m.group(1)}) * INTERVAL 1 {m.group(2).upper()})", nxt
        if up == "DATE_FORMAT" and d == "mysql" and len(args) == 2 and args[1].startswith("'"):
            return fn("date_format", args[0], fmt_convert(args[1], "mysql")), nxt
        if up == "STR_TO_DATE" and len(args) == 2 and args[1].startswith("'"):
            return fn("to_timestamp", args[0], fmt_convert(args[1], "mysql")), nxt
        if up == "FORMAT_DATE" and d == "bigquery" and len(args) == 2 and args[0].startswith("'"):
            return fn("date_format", args[1], fmt_convert(args[0], "strftime")), nxt
        if up == "TO_CHAR" and len(args) == 2 and args[1].startswith("'"):
            return fn("date_format", args[0], fmt_convert(args[1], "oracle")), nxt
        if up == "TO_CHAR" and len(args) == 1:
            return f"CAST({args[0]} AS STRING)", nxt
        if up == "TO_DATE" and d in ("oracle", "postgres", "redshift", "duckdb", "db2", "netezza", "vertica", "hana", "informix") and len(args) == 2 and args[1].startswith("'"):
            return fn("to_date", args[0], fmt_convert(args[1], "oracle")), nxt
        if up == "TO_TIMESTAMP" and d in ("oracle", "postgres", "redshift", "duckdb", "db2", "netezza", "vertica", "hana", "informix") and len(args) == 2 and args[1].startswith("'"):
            return fn("to_timestamp", args[0], fmt_convert(args[1], "oracle")), nxt
        if up == "TRUNC" and d == "oracle" and len(args) == 1:
            return fn("to_date", args[0]), nxt
        if up == "ADD_MONTHS":
            return None, nxt
        if up in ("GENERATE_SERIES", "UNNEST", "FLATTEN"):
            c.todo(f"{up}() -> use explode(sequence(...)) / explode(array)")
        return None, nxt

    BQ_PARTS = {"DAY": "DAY", "WEEK": "WEEK", "MONTH": "MONTH", "QUARTER": "QUARTER", "YEAR": "YEAR",
                "HOUR": "HOUR", "MINUTE": "MINUTE", "SECOND": "SECOND"}

    def bigquery_function(self, up, orig, args, raw, nxt):
        c = self.c
        fn = lambda name, *a: f"{name}({', '.join(a)})"
        if up == "SAFE_CAST":
            m = re.match(r"(?is)(.*)\s+AS\s+(\w+)\s*(\(.*\))?\s*$", self.render(sum(raw, [])).strip()) if False else None
            m = re.match(r"(?is)(.*)\s+AS\s+(\w+)\s*(\(.*\))?\s*$", ", ".join(args))
            if m:
                return f"TRY_CAST({m.group(1)} AS {self.map_type(m.group(2), m.group(3) or '')})", nxt
        if up in ("DATE_TRUNC", "TIMESTAMP_TRUNC", "DATETIME_TRUNC") and len(args) == 2:
            part = args[1].upper().split("(")[0]
            if up == "DATE_TRUNC" and part in ("MONTH", "QUARTER", "YEAR", "WEEK"):
                return fn("trunc", args[0], repr(part)), nxt
            if up == "DATE_TRUNC" and part == "DAY":
                return fn("to_date", args[0]), nxt
            return fn("date_trunc", repr(part), args[0]), nxt
        if up in ("PARSE_DATE", "PARSE_TIMESTAMP", "PARSE_DATETIME") and len(args) == 2 and args[0].startswith("'"):
            return fn("to_date" if up == "PARSE_DATE" else "to_timestamp", args[1], fmt_convert(args[0], "strftime")), nxt
        if up in ("FORMAT_TIMESTAMP", "FORMAT_DATETIME") and len(args) == 2 and args[0].startswith("'"):
            return fn("date_format", args[1], fmt_convert(args[0], "strftime")), nxt
        if up in ("DATE_ADD", "DATE_SUB", "TIMESTAMP_ADD", "TIMESTAMP_SUB", "DATETIME_ADD", "DATETIME_SUB") and len(args) == 2:
            m = re.match(r"(?is)INTERVAL\s+(.+?)\s+(\w+)$", args[1])
            if m:
                sign = "+" if up.endswith("_ADD") else "-"
                return f"({args[0]} {sign} ({m.group(1)}) * INTERVAL 1 {m.group(2).upper()})", nxt
        if up in ("TIMESTAMP_DIFF", "DATETIME_DIFF") and len(args) == 3:
            return self.datediff(args[2], args[1], args[0]), nxt
        if up == "LOGICAL_OR":
            return fn("bool_or", *args), nxt
        if up == "LOGICAL_AND":
            return fn("bool_and", *args), nxt
        if up == "ARRAY_LENGTH":
            return fn("size", *args), nxt
        if up == "ARRAY_TO_STRING" and len(args) >= 2:
            return fn("array_join", *args), nxt
        if up == "SPLIT" and len(args) == 2 and args[1].startswith("'"):
            lit = args[1][1:-1]
            return fn("split", args[0], "'" + re.sub(r"([.|+*?()\[\]{}^$\\])", r"\\\\\1", lit) + "'"), nxt
        if up == "REGEXP_CONTAINS" and len(args) == 2:
            return f"({args[0]} RLIKE {args[1]})", nxt
        if up == "REGEXP_EXTRACT" and len(args) == 2:
            grp = 1 if "(" in args[1] else 0
            return fn("regexp_extract", args[0], args[1], str(grp)), nxt
        if up == "GENERATE_UUID":
            return "uuid()", nxt
        if up == "FARM_FINGERPRINT":
            return fn("xxhash64", *args), nxt
        if up == "TO_JSON_STRING":
            return fn("to_json", *args), nxt
        if up == "FORMAT" and args and args[0].startswith("'"):
            return fn("format_string", *args), nxt
        if up in ("CURRENT_DATETIME",):
            return "current_timestamp()", nxt
        if up == "DATE" and len(args) == 3:
            return fn("make_date", *args), nxt
        if up == "DATE" and len(args) == 1:
            return fn("to_date", *args), nxt
        if up == "TIMESTAMP" and len(args) == 1:
            return fn("to_timestamp", *args), nxt
        if up == "STRUCT":
            parts, named = [], True
            for a in args:
                m = re.match(r"(?is)(.*)\s+AS\s+(\w+)$", a)
                if m:
                    parts += [repr(m.group(2)), m.group(1)]
                else:
                    named = False
            if named and parts:
                return fn("named_struct", *parts), nxt
            return fn("struct", *args), nxt
        if up in ("APPROX_QUANTILES", "GENERATE_DATE_ARRAY", "GENERATE_TIMESTAMP_ARRAY", "ML.PREDICT"):
            c.todo(f"{up}() has no direct Spark equivalent (use percentile_approx / sequence / Spark ML)")
        return None

    def dateadd(self, part, n, d):
        p = part.upper()
        if p in ("DAY", "DD", "D", "DAYS"):
            return f"date_add({d}, {n})"
        if p in ("MONTH", "MM", "M"):
            return f"add_months({d}, {n})"
        if p in ("YEAR", "YY", "YYYY"):
            return f"add_months({d}, 12 * ({n}))"
        if p in ("WEEK", "WK", "WW"):
            return f"date_add({d}, 7 * ({n}))"
        unit = {"HOUR": "HOUR", "HH": "HOUR", "MINUTE": "MINUTE", "MI": "MINUTE", "N": "MINUTE",
                "SECOND": "SECOND", "SS": "SECOND", "S": "SECOND", "QUARTER": "MONTH"}.get(p)
        if unit:
            mult = "3 * " if p == "QUARTER" else ""
            return f"({d} + {mult}({n}) * INTERVAL 1 {unit})"
        self.c.todo(f"DATEADD part {part!r} not converted")
        return f"DATEADD({part}, {n}, {d})"

    def datediff(self, part, a, b):
        p = part.upper()
        if p in ("DAY", "DD", "D", "DAYS"):
            return f"datediff({b}, {a})"
        if p in ("MONTH", "MM", "M"):
            self.c.warn("DATEDIFF(month) -> months_between truncated; SQL Server counts boundary crossings, results can differ")
            return f"CAST(months_between({b}, {a}) AS INT)"
        if p in ("YEAR", "YY", "YYYY"):
            return f"(year({b}) - year({a}))"
        div = {"HOUR": 3600, "HH": 3600, "MINUTE": 60, "MI": 60, "N": 60, "SECOND": 1, "SS": 1, "S": 1}.get(p)
        if div:
            expr = f"unix_timestamp({b}) - unix_timestamp({a})"
            return f"CAST(({expr}) / {div} AS BIGINT)" if div != 1 else f"({expr})"
        self.c.todo(f"DATEDIFF part {part!r} not converted")
        return f"DATEDIFF({part}, {a}, {b})"

    def array_agg(self, inner, toks, end):
        """ARRAY_AGG([DISTINCT] x [IGNORE|RESPECT NULLS] [ORDER BY k [ASC|DESC], ...] [LIMIT n]) [WITHIN GROUP (ORDER BY ..)] [OVER (..)]
        -> collect_list/collect_set, sort_array (or sort_array over structs of the sort keys), slice for LIMIT"""
        c = self.c
        nxt = end

        def fail(msg):
            c.todo(f"ARRAY_AGG: {msg}")
            return None, nxt

        # markers at parenthesis depth 0
        marks, depth, i = [], 0, 0
        words = [(k, t) for k, t in enumerate(inner)]
        while i < len(inner):
            t = inner[i]
            if t == ("op", "("):
                depth += 1
            elif t == ("op", ")"):
                depth -= 1
            elif depth == 0 and t[0] == "word":
                up = t[1].upper()
                nxt_word = next((x[1].upper() for x in inner[i + 1:] if x[0] not in ("ws", "comment")), "")
                if (up in ("IGNORE", "RESPECT") and nxt_word == "NULLS") or (up == "ORDER" and nxt_word == "BY") or up == "LIMIT":
                    marks.append((up, i))
            i += 1
        first = marks[0][1] if marks else len(inner)
        expr_toks = inner[:first]
        distinct = False
        sig = [k for k, t in enumerate(expr_toks) if t[0] not in ("ws", "comment")]
        if sig and expr_toks[sig[0]][0] == "word" and expr_toks[sig[0]][1].upper() == "DISTINCT":
            distinct = True
            expr_toks = expr_toks[sig[0] + 1:]
        if not txt(expr_toks):
            return fail("empty argument")
        order_toks, limit_toks, seen = None, None, []
        for n, (kind, pos) in enumerate(marks):
            stop = marks[n + 1][1] if n + 1 < len(marks) else len(inner)
            body = inner[pos:stop]
            if kind in ("IGNORE", "RESPECT"):
                seen.append("N")
            elif kind == "ORDER":
                seen.append("O")
                order_toks = body
            else:
                seen.append("L")
                limit_toks = body
        if "".join(seen) not in ("", "N", "O", "L", "NO", "NL", "OL", "NOL"):
            return fail("clauses must be written as [IGNORE NULLS] [ORDER BY ...] [LIMIT n]")
        # trailing WITHIN GROUP (ORDER BY ...) / OVER (...) / FILTER
        k, consumed, over = end + 1, end, ""            # over: "" | list of tokens | " OVER name"
        while k < len(toks) and toks[k][0] in ("ws", "comment"):
            k += 1
        if k < len(toks) and toks[k][0] == "word" and toks[k][1].upper() == "WITHIN":
            k2 = k + 1
            while k2 < len(toks) and (toks[k2][0] in ("ws", "comment") or toks[k2][1].upper() == "GROUP"):
                k2 += 1
            if not (k2 < len(toks) and toks[k2] == ("op", "(")) or order_toks is not None:
                return fail("unsupported WITHIN GROUP form")
            close = self.match(toks, k2)
            order_toks = toks[k2 + 1:close]
            consumed, k = close, close + 1
            while k < len(toks) and toks[k][0] in ("ws", "comment"):
                k += 1
        if k < len(toks) and toks[k][0] == "word" and toks[k][1].upper() == "FILTER":
            return fail("FILTER (WHERE ...) is not combined with the ordering rewrite - use CASE WHEN inside the aggregate")
        if k < len(toks) and toks[k][0] == "word" and toks[k][1].upper() == "OVER":
            k2 = k + 1
            while k2 < len(toks) and toks[k2][0] in ("ws", "comment"):
                k2 += 1
            if k2 < len(toks) and toks[k2] == ("op", "("):
                close = self.match(toks, k2)
                over = toks[k2 + 1:close]                 # rendered after validation
                consumed = close
            elif k2 < len(toks) and toks[k2][0] == "word":
                over = f" OVER {toks[k2][1]}"
                consumed = k2
            else:
                return fail("unsupported OVER clause")
        limit = None
        if limit_toks is not None:
            lt = [t for t in limit_toks[1:] if t[0] not in ("ws", "comment")]
            if len(lt) != 1 or lt[0][0] != "num" or not lt[0][1].isdigit() or int(lt[0][1]) < 1:
                return fail("LIMIT must be a positive integer literal")
            limit = int(lt[0][1])
        key_toks = []
        if order_toks is not None:
            body = list(order_toks)
            while body and (body[0][0] in ("ws", "comment") or body[0][1].upper() in ("ORDER", "BY")):
                body = body[1:]
            for part in split_args(body):
                ptoks = [t for t in part if t[0] not in ("ws", "comment")]
                if any(t[0] == "word" and t[1].upper() in ("NULLS", "FIRST", "LAST") for t in ptoks[-3:]):
                    return fail("NULLS FIRST/LAST in the ordering has no automatic conversion")
                desc = False
                if ptoks and ptoks[-1][0] == "word" and ptoks[-1][1].upper() in ("ASC", "DESC"):
                    desc = ptoks[-1][1].upper() == "DESC"
                    cut = max(i for i, t in enumerate(part) if t == ptoks[-1])
                    part = part[:cut]
                if not txt(part):
                    return fail("empty ORDER BY key")
                key_toks.append((part, desc))
        norm = lambda e: re.sub(r"\s+", "", e).lower()
        if distinct and key_toks and not (len(key_toks) == 1 and norm(txt(key_toks[0][0])) == norm(txt(expr_toks))):
            return fail("DISTINCT with ORDER BY on a different expression has no automatic conversion (BigQuery does not allow it either)")
        if isinstance(over, list):
            over = f" OVER ({self.render(over).strip()})"
        x = self.render(expr_toks).strip()                 # everything is validated: render once, no fallback re-render
        keys = [(self.render(p).strip(), d) for p, d in key_toks]
        agg = "collect_set" if distinct else "collect_list"
        simple = bool(re.fullmatch(r"[\w.`]+", x))
        if not keys:
            code = f"{agg}({x}){over}"
            if limit is not None or distinct:
                c.warn("ARRAY_AGG without ORDER BY: the order (and which elements LIMIT keeps) is not defined, as in BigQuery")
        elif len(keys) == 1 and norm(keys[0][0]) == norm(x):
            code = f"sort_array({agg}({x}){over}{', false' if keys[0][1] else ''})"
        else:
            dirs = {d for _, d in keys}
            ks = ", ".join(f"{e} AS _k{n}" for n, (e, _) in enumerate(keys))
            cond = x if simple else f"({x})"
            arr = f"{agg}(CASE WHEN {cond} IS NOT NULL THEN struct({ks}, {x} AS _v) END){over}"
            if len(dirs) == 1:
                srt = f"sort_array({arr}{', false' if True in dirs else ''})"
            else:
                def cmp(n, desc):
                    a, b = ("_r", "_l") if desc else ("_l", "_r")
                    f = f"_k{n}"
                    return (f"CASE WHEN {a}.{f} IS NULL AND {b}.{f} IS NULL THEN 0 WHEN {a}.{f} IS NULL THEN -1 WHEN {b}.{f} IS NULL THEN 1 "
                            f"WHEN {a}.{f} < {b}.{f} THEN -1 WHEN {a}.{f} > {b}.{f} THEN 1 ELSE 0 END")
                parts = ", ".join(f"NULLIF({cmp(n, d)}, 0)" for n, (_, d) in enumerate(keys))
                srt = f"array_sort({arr}, (_l, _r) -> COALESCE({parts}, 0))"
                c.warn("ARRAY_AGG with mixed ASC/DESC keys -> array_sort with a comparator (Spark 3.0+)")
            code = f"transform({srt}, _e -> _e._v)"
        if limit is not None:
            code = f"slice({code}, 1, {limit})"
        if not (distinct or keys or limit is not None):
            c.warn("ARRAY_AGG -> collect_list: NULL elements are dropped (BigQuery keeps them or fails on output)")
        return code, consumed

    def string_agg(self, up, raw, args, toks, end):
        c = self.c
        distinct = False
        first = raw[0]
        s = txt(first)
        if re.match(r"(?i)distinct\s", s):
            distinct = True
            s = s[8:].strip()
            args = [self.render([("word", s)]).strip()] + args[1:]
        sep = "','"
        if up == "GROUP_CONCAT":
            m = re.search(r"(?is)^(.*?)\s+SEPARATOR\s+('(?:[^']|'')*')\s*$", args[0])
            if m:
                args = [m.group(1), m.group(2)]
            if re.search(r"(?i)\bORDER\s+BY\b", args[0]):
                c.warn("GROUP_CONCAT ... ORDER BY dropped: collect_list order is not guaranteed")
                args = [re.split(r"(?i)\s+ORDER\s+BY\s+", args[0])[0]] + args[1:]
        if len(args) > 1:
            sep = args[1]
        col = args[0]
        agg = "collect_set" if distinct else "collect_list"
        # swallow WITHIN GROUP (...)
        k = end + 1
        while k < len(toks) and toks[k][0] == "ws":
            k += 1
        consumed = end
        if k + 1 < len(toks) and toks[k][1].upper() == "WITHIN":
            k2 = k + 1
            while toks[k2][0] == "ws" or toks[k2][1].upper() == "GROUP":
                k2 += 1
            if toks[k2] == ("op", "("):
                close = self.match(toks, k2)
                inner = re.sub(r"\s+", " ", "".join(t[1] for t in toks[k2 + 1:close])).strip()
                om = re.fullmatch(r"(?is)ORDER BY\s+(.+?)(?:\s+(ASC|DESC))?", inner)
                consumed = close
                if om and om.group(1).strip().lower() == col.strip().lower():
                    desc = (om.group(2) or "").upper() == "DESC"
                    return f"array_join(sort_array({agg}({col}){', false' if desc else ''}), {sep})", consumed
                c.warn(f"{up} ... WITHIN GROUP (ORDER BY {inner[9:60]}) dropped: ordering by another column needs sort_array over struct(key, value); result order is not guaranteed")
        return f"array_join({agg}({col}), {sep})", consumed


# ------------------------------------------------------------------ statement level
def split_statements(toks):
    out, cur, depth = [], [], 0
    for t in toks:
        if t == ("op", "("):
            depth += 1
        elif t == ("op", ")"):
            depth -= 1
        if t == ("op", ";") and depth == 0:
            out.append(cur)
            cur = []
        else:
            cur.append(t)
    out.append(cur)
    return [s for s in out if any(t[0] not in ("ws", "comment") for t in s)]


def significant(toks):
    return [(i, t) for i, t in enumerate(toks) if t[0] not in ("ws", "comment")]


def move_top_to_limit(toks, ctx):
    """T-SQL `SELECT TOP n` -> trailing `LIMIT n` of the same SELECT scope."""
    toks = list(toks)
    i = 0
    while i < len(toks):
        if toks[i][0] == "word" and toks[i][1].upper() == "TOP":
            # must follow SELECT [DISTINCT|ALL]
            sig = [t for t in toks[:i] if t[0] not in ("ws", "comment")]
            if not sig or sig[-1][1].upper() not in ("SELECT", "SEL", "DISTINCT", "ALL"):
                i += 1
                continue
            j = i + 1
            while toks[j][0] == "ws":
                j += 1
            if toks[j] == ("op", "("):
                e = j
                depth = 0
                while True:
                    depth += toks[e] == ("op", "(")
                    depth -= toks[e] == ("op", ")")
                    if depth == 0:
                        break
                    e += 1
                n = txt(toks[j + 1:e])
                k = e + 1
            else:
                n, k = toks[j][1], j + 1
            if k < len(toks) and toks[k][0] == "ws" and k + 1 < len(toks) and toks[k + 1][1].upper() == "PERCENT":
                ctx.todo("TOP n PERCENT has no direct Spark equivalent")
                k += 2
            # depth at i
            depth = 0
            for t in toks[:i]:
                depth += t == ("op", "(")
                depth -= t == ("op", ")")
            # find scope end
            e2, d = len(toks), depth
            for q in range(k, len(toks)):
                if toks[q] == ("op", "("):
                    d += 1
                elif toks[q] == ("op", ")"):
                    d -= 1
                    if d < depth:
                        e2 = q
                        break
            toks[e2:e2] = [("ws", " "), ("word", "LIMIT"), ("ws", " "), ("num", n)]
            del toks[i:k]
            while i < len(toks) and toks[i][0] == "ws" and toks[i - 1][0] == "ws":
                del toks[i]
            continue
        i += 1
    return toks


def post_process(sql: str, ctx: Ctx) -> str:
    # FETCH FIRST n ROWS ONLY
    sql = re.sub(r"(?is)\bFETCH\s+(?:FIRST|NEXT)\s+(\d+)\s+ROWS?\s+ONLY\b", r"LIMIT \1", sql)
    sql = re.sub(r"(?is)\bFETCH\s+(?:FIRST|NEXT)\s+ROW\s+ONLY\b", "LIMIT 1", sql)
    # MySQL LIMIT a, b
    sql = re.sub(r"(?is)\bLIMIT\s+(\d+)\s*,\s*(\d+)\b", r"LIMIT \2 OFFSET \1", sql)
    # OFFSET n ROWS
    sql = re.sub(r"(?is)\bOFFSET\s+(\d+)\s+ROWS?\b", r"OFFSET \1", sql)
    # FROM DUAL
    sql = re.sub(r"(?is)\s+FROM\s+DUAL\b", "", sql)
    sql = re.sub(r"(?is)\bMINUS\b", "EXCEPT", sql)
    # NOLOCK hints
    sql = re.sub(r"(?is)\s+WITH\s*\(\s*NOLOCK\s*\)", "", sql)
    # schema prefix dbo.
    sql = re.sub(r"(?i)\bdbo\.", "", sql) if ctx.d == "tsql" else sql
    return sql


def statement_checks(sig, ctx: Ctx):
    words = [t[1].upper() for _, t in sig[:6]]
    if not words:
        return
    first = words[0]
    if first in ("UPDATE", "DELETE", "MERGE"):
        ctx.todo(f"{first} only works on Delta/Iceberg/Hudi tables in Spark; plain Parquet/Hive tables are immutable")
    if first == "DECLARE" or first == "SET" and any(t[1].startswith("@") for _, t in sig[:3]):
        ctx.todo("script variables (DECLARE/SET): use Python variables / spark.sql(f'...') or named :param markers")
    if first in ("CREATE", "DROP") and "INDEX" in words[:3]:
        ctx.warn("indexes do not exist in Spark; statement will fail - remove it")
    if first == "CREATE" and ("TEMP" in words or "TEMPORARY" in words) and "TABLE" in words:
        ctx.warn("CREATE TEMP TABLE -> CREATE OR REPLACE TEMPORARY VIEW")
    if first == "SELECT" and "INTO" in [t[1].upper() for _, t in sig]:
        ctx.todo("SELECT ... INTO: use CREATE TABLE ... AS SELECT")
    if "QUALIFY" in [t[1].upper() for _, t in sig]:
        ctx.warn("QUALIFY support depends on your Spark version; if unsupported wrap the query in a subquery filtering on the window column")
    if any(t[0] == "word" and t[1].startswith("@") for _, t in sig) and first != "SET" and ctx.d != "bigquery":
        ctx.todo("@parameters are not supported by spark.sql(); use named :param markers (Spark 3.4+) or f-strings")
    if any(t[0] == "word" and t[1].startswith("#") for _, t in sig):
        ctx.todo("#temp tables: use CREATE TEMPORARY VIEW")


_ROWNUM_COND = re.compile(r"(?is)\s*ROWNUM\s*(<=|<|=)\s*(\d{1,9})\s*")
_SET_OPS = {"UNION", "INTERSECT", "MINUS", "EXCEPT"}
_WINDOW_ROWNUM = "row_number() OVER (ORDER BY monotonically_increasing_id())"


def _match_paren(toks, i):
    depth = 0
    for k in range(i, len(toks)):
        if toks[k] == ("op", "("):
            depth += 1
        elif toks[k] == ("op", ")"):
            depth -= 1
            if depth == 0:
                return k
    return -1


def _top_words(toks):
    """(index, UPPERCASE word) for every word at parenthesis depth 0"""
    out, depth = [], 0
    for i, t in enumerate(toks):
        if t == ("op", "("):
            depth += 1
        elif t == ("op", ")"):
            depth -= 1
        elif depth == 0 and t[0] == "word":
            out.append((i, t[1].upper()))
    return out


def rownum_pass(toks, ctx):
    """Oracle ROWNUM:
    * `WHERE ... ROWNUM <= n` / `< n` / `= 1` at any query level -> LIMIT (condition removed)
    * ROWNUM in a select list -> row_number() OVER (ORDER BY monotonically_increasing_id()); together with a ROWNUM bound
      and an alias (`ROWNUM rn ... WHERE ROWNUM <= n`) the bound becomes a filter on the alias, which keeps Oracle's meaning
    Anything else (>, >=, GROUP BY / DISTINCT / aggregates / OR / set operations at the same level) is left for a TODO."""
    if not any(k == "word" and v.upper() == "ROWNUM" for k, v in toks):
        return toks
    out, i = [], 0
    while i < len(toks):
        if toks[i] == ("op", "("):
            j = _match_paren(toks, i)
            if j < 0:
                return toks
            out += [("op", "(")] + rownum_pass(toks[i + 1:j], ctx) + [toks[j]]
            i = j + 1
        else:
            out.append(toks[i])
            i += 1
    return _rownum_level(out, ctx)


def _rownum_level(toks, ctx):
    tw = _top_words(toks)
    sel = next((i for i, w in tw if w == "SELECT"), None)
    if sel is None:
        return toks
    after = [(i, w) for i, w in tw if i > sel]
    if any(w in _SET_OPS for _, w in after) or any(w in _SET_OPS for i, w in tw if i < sel):
        return toks
    frm = next((i for i, w in after if w == "FROM"), None)
    wh = next((i for i, w in after if w == "WHERE"), None)
    stop_words = {"GROUP", "HAVING", "ORDER", "CONNECT", "START", "FETCH", "LIMIT", "FOR", "MODEL"}
    wh_end = None
    if wh is not None:
        wh_end = next((i for i, w in after if i > wh and w in stop_words), len(toks))
    sel_end = frm if frm is not None else (wh if wh is not None else len(toks))
    sel_toks = toks[sel + 1:sel_end]
    # --- select-list ROWNUM
    sel_idx = {k for k, t in enumerate(sel_toks) if t[0] == "word" and t[1].upper() == "ROWNUM"}
    depth0 = []
    d = 0
    for k, t in enumerate(sel_toks):
        if t == ("op", "("):
            d += 1
        elif t == ("op", ")"):
            d -= 1
        elif d == 0 and k in sel_idx:
            depth0.append(k)
    # --- WHERE ROWNUM bound
    bound, new_where = None, None
    if wh is not None:
        wtoks = toks[wh + 1:wh_end]
        has_or = any(w == "OR" for i, w in after if wh < i < wh_end)
        parts, cur, d = [], [], 0
        for t in wtoks:
            if t == ("op", "("):
                d += 1
            elif t == ("op", ")"):
                d -= 1
            if d == 0 and t[0] == "word" and t[1].upper() == "AND":
                parts.append(cur)
                cur = []
            else:
                cur.append(t)
        parts.append(cur)
        hits = [k for k, p in enumerate(parts) if _ROWNUM_COND.fullmatch("".join(t[1] for t in p))]
        if hits and not has_or:
            k = hits[0]
            m = _ROWNUM_COND.fullmatch("".join(t[1] for t in parts[k]))
            op, n = m.group(1), int(m.group(2))
            if op == "=" and n != 1:
                return toks                                   # ROWNUM = 2 never matches in Oracle: not a meaningful port
            lim = n - 1 if op == "<" else n
            if op == "=":
                lim = 1
            bound = (op, n, lim)
            rest = [p for q, p in enumerate(parts) if q != k]
            new_where = []
            for q, p in enumerate(rest):
                while p and p[0][0] == "ws":
                    p = p[1:]
                while p and p[-1][0] == "ws":
                    p = p[:-1]
                new_where += ([("ws", " "), ("word", "AND"), ("ws", " ")] if q else []) + list(p)
            if new_where:
                new_where = [("ws", " ")] + new_where
    others_in_where = wh is not None and any(t[0] == "word" and t[1].upper() == "ROWNUM" for t in toks[wh + 1:wh_end]) and bound is None
    if others_in_where:
        return toks
    # --- level must be a plain query for a ROWNUM bound / select-list ROWNUM to be portable
    word_set = {w for _, w in after}
    sel_text = "".join(t[1] for t in sel_toks).upper()
    is_agg = bool(re.search(r"\b(COUNT|SUM|AVG|MIN|MAX|LISTAGG|STDDEV|VARIANCE)\s*\(", sel_text)) and "OVER" not in sel_text
    if bound is not None:
        if word_set & {"GROUP", "HAVING", "LIMIT", "FETCH", "CONNECT", "START", "MODEL"} or is_agg or "DISTINCT" in {t[1].upper() for t in sel_toks[:3] if t[0] == "word"} or "UNIQUE" in {t[1].upper() for t in sel_toks[:3] if t[0] == "word"}:
            return toks
    # --- apply
    alias = None
    new_sel = list(sel_toks)
    if depth0:
        for k in reversed(depth0):
            rest = []
            for q in range(k + 1, len(new_sel)):           # only the next two significant tokens matter (keeps this linear)
                if new_sel[q][0] not in ("ws", "comment"):
                    rest.append(new_sel[q])
                    if len(rest) == 2:
                        break
            a = None
            if rest and rest[0][0] == "word" and rest[0][1].upper() == "AS" and len(rest) > 1 and rest[1][0] in ("word", "dq"):
                a = rest[1][1]
            elif rest and rest[0][0] in ("word", "dq") and rest[0][1].upper() not in ("FROM", "INTO", "BULK"):
                a = rest[0][1]
            if a is not None:
                alias = alias or a
            new_sel[k] = ("word", _WINDOW_ROWNUM)
        if alias is None:
            pass
        ctx.warn("ROWNUM in the select list -> row_number() OVER (ORDER BY monotonically_increasing_id()): numbers follow the current row order and use a single partition")
    head = toks[:sel + 1] + new_sel
    mid_start = sel_end
    tail_where = toks[wh_end:] if wh is not None else toks[sel_end:]
    if wh is None:
        pre_where = []
        where_part = []
    else:
        pre_where = toks[sel_end:wh]
        where_part = ([("word", "WHERE")] + new_where + [("ws", " ")]) if (bound is not None and new_where) else \
                     ([] if bound is not None else toks[wh:wh_end])
    body = head + pre_where + where_part + tail_where
    if bound is None:
        return body
    op, n, lim = bound
    has_order = any(w == "ORDER" for _, w in after)
    if depth0 and alias is not None:
        while body and body[-1][0] in ("ws", "comment"):
            body = body[:-1]
        cmp_op = "<" if op == "<" else ("=" if op == "=" else "<=")
        return toks[:sel] + [("word", "SELECT"), ("ws", " "), ("op", "*"), ("ws", " "), ("word", "FROM"), ("ws", " "), ("op", "(")] + body[sel:] + \
            [("op", ")"), ("ws", " "), ("word", "_rn_w"), ("ws", " "), ("word", "WHERE"), ("ws", " "), ("word", alias), ("ws", " "),
             ("op", cmp_op), ("ws", " "), ("num", str(n))]
    if has_order:
        ctx.warn("ROWNUM <= n applies before ORDER BY in Oracle; LIMIT applies after - results can differ")
    while body and body[-1][0] in ("ws", "comment"):
        body = body[:-1]
    return body + [("ws", " "), ("word", "LIMIT"), ("ws", " "), ("num", str(lim))]


def bigquery_prepass(toks, ctx: Ctx):
    text = "".join(t[1] for t in toks)
    # unquoted project.dataset.table / dataset.table after FROM / JOIN -> backticked (handled by the rewriter)
    text = re.sub(r"(?i)\b(FROM|JOIN)(\s+)(?![`(])([A-Za-z_][\w-]*\.[\w-]+(?:\.[\w-]+)?)\b(?!\s*\()",
                  lambda m: f"{m.group(1)}{m.group(2)}`{m.group(3)}`", text)
    # UNNEST in FROM (comma or CROSS JOIN) -> LATERAL VIEW explode
    def unnest(m):
        if re.search(r"(?i)WITH\s+OFFSET", m.group(0)):
            ctx.todo("UNNEST ... WITH OFFSET: use LATERAL VIEW posexplode(...)")
            return m.group(0)
        return f" LATERAL VIEW explode({m.group(1)}) _u_{m.group(2)} AS {m.group(2)}"
    text = re.sub(r"(?is)(?:,|\bCROSS\s+JOIN)\s*UNNEST\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\)\s*(?:AS\s+)?(\w+)", unnest, text)
    # DDL clauses without Spark equivalent
    if re.match(r"(?is)\s*CREATE\b", text):
        for pat, what in ((r"(?is)\s+PARTITION\s+BY\s+.*?(?=\s+(?:CLUSTER\s+BY|OPTIONS|AS)\b)", "PARTITION BY"),
                          (r"(?is)\s+CLUSTER\s+BY\s+.*?(?=\s+(?:OPTIONS|AS)\b)", "CLUSTER BY"),
                          (r"(?is)\s+OPTIONS\s*\(.*?\)(?=\s+AS\b)", "OPTIONS(...)")):
            if re.search(pat, text):
                ctx.warn(f"{what} clause dropped (BigQuery-specific); add PARTITIONED BY (col) in Spark if needed")
                text = re.sub(pat, "", text)
        text = re.sub(r"(?is)^(\s*CREATE\s+OR\s+REPLACE\s+TABLE)", r"\1", text)
    if re.search(r"(?i)\*\s+EXCEPT\s*\(", text):
        ctx.warn("SELECT * EXCEPT(...) needs Spark 4 / Databricks; on older Spark list the columns explicitly")
    if re.search(r"(?i)\*\s+REPLACE\s*\(", text):
        ctx.todo("SELECT * REPLACE(...) has no Spark equivalent - list the columns")
    if re.match(r"(?is)\s*(BEGIN|IF\b|WHILE\b|LOOP\b|EXECUTE\s+IMMEDIATE|FOR\b)", text):
        ctx.todo("BigQuery scripting block: orchestrate in Python and run each statement with spark.sql(...)")
    return tokenize(text)


def convert_statement(toks, ctx: Ctx) -> str:
    from .sql_dialects import _PRE
    ctx.ddl = bool(toks) and bool(re.match(r"(?is)\s*(?:--[^\n]*\n\s*)*(CREATE|ALTER)\b", "".join(t[1] for t in toks[:40])))
    if ctx.name in _PRE or ctx.name in ("db2", "netezza", "vertica", "clickhouse", "hana", "informix", "firebird", "impala"):
        from .sql_dialects import prepass as dialect_prepass
        text = dialect_prepass("".join(t[1] for t in toks), ctx)
        if text.lstrip().startswith("--") and "\n" not in text.strip():
            return text.strip()
        toks = tokenize(text)
    if ctx.ddl and ctx.d not in ("teradata", "spark", "databricks"):
        from .sql_dialects import ddl_types, CH_TYPES
        types = dict(TYPES, **CH_TYPES) if ctx.d == "clickhouse" else TYPES
        toks = tokenize(ddl_types("".join(t[1] for t in toks), ctx, types))
    if ctx.d == "bigquery":
        toks = bigquery_prepass(toks, ctx)
    if ctx.d == "teradata":
        from .teradata_sql import prepass
        text = prepass("".join(t[1] for t in toks), ctx)
        if text.lstrip().startswith("--") and "\n" not in text.strip():
            return text.strip()                       # dropped statement: kept as a comment
        if re.match(r"(?is)\s*(USE|CREATE\s+(TABLE|OR\s+REPLACE))\b", text) and not re.search(r"(?is)\bSELECT\b", text):
            return text.strip()
        toks = tokenize(text)
    if ctx.d in ("tsql", "redshift") and any(t[1].upper() == "TIES" for t in toks):
        from .teradata_sql import top_with_ties
        toks = tokenize(top_with_ties("".join(t[1] for t in toks), ctx))
    toks = rownum_pass(toks, ctx)
    sig = significant(toks)
    statement_checks(sig, ctx)
    if ctx.d in ("tsql", "teradata", "redshift", "hana", "netezza", "vertica", "informix", "firebird"):
        toks = move_top_to_limit(toks, ctx)
    sql = Rewriter(ctx).render(toks).strip()
    sql = post_process(sql, ctx)
    if ctx.d == "teradata":
        sql = re.sub(r"(?is)^(\s*(?:--[^\n]*\n\s*)*)SEL\b", r"\1SELECT", sql)
    if ctx.d == "duckdb" and re.search(r"(?i)\*\s+EXCLUDE\b", sql):
        ctx.warn("SELECT * EXCLUDE -> Spark `* EXCEPT (...)` (check your Spark version)")
        sql = re.sub(r"(?i)\*\s+EXCLUDE\b", "* EXCEPT", sql)
    sql = re.sub(r"(?is)^CREATE\s+(?:LOCAL\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+TABLE\s+(\w+)\s+AS\b",
                 r"CREATE OR REPLACE TEMPORARY VIEW \1 AS", sql)
    return sql


def tables_in(sql: str) -> list:
    names = re.findall(r"(?is)\b(?:FROM|JOIN)\s+([A-Za-z_`][\w.`]*)", sql)
    ctes = set(re.findall(r"(?is)\b(\w+)\s+AS\s*\(", sql))
    seen = []
    for n in names:
        if n.lower() not in ctes and n not in seen and n.lower() not in ("select", "lateral", "unnest"):
            seen.append(n)
    return seen


def convert_sql(source: str, report: Report, dialect: str = "generic", out: str = "pyspark") -> str:
    return convert_sql_ex(source, report, dialect, out)[0]


def _exec_marker(c, execs):
    m = re.search(r"SPARQIX_EXEC_(\d+)", c)
    return execs[int(m.group(1))] if m and int(m.group(1)) < len(execs) and len(c) < 80 else c


def convert_sql_ex(source: str, report: Report, dialect: str = "generic", out: str = "pyspark", bq_views: bool = False):
    """like convert_sql but also returns the BigQuery table ids referenced"""
    ctx = Ctx(dialect, report, bq_views)
    from .sql_dialects import alias_note
    if alias_note(dialect):
        report.note(alias_note(dialect))
    macros = []
    if dialect == "teradata":
        from .teradata_macros import extract_macros
        source, macros = extract_macros(source, ctx)
    toks = tokenize(source)
    stmts = split_statements(toks)
    converted = []
    for s in stmts:
        # strip a leading line number context
        converted.append(convert_statement(s, ctx))
        ctx.line += 1
    if macros:
        from .teradata_macros import macro_code
    execs = getattr(ctx, "execs", [])
    converted = [_exec_marker(c, execs) for c in converted]
    if out == "sql":
        parts = [c if c.startswith("--") and "\n" not in c else c + ";" for c in converted if c.strip() and not c.startswith("-- __EXEC__")]
        for mc in macros:
            parts.append(f"-- TODO(sparqix): Teradata macro {mc['name']}: run it from Python (use --from sql with pyspark output, which emits a function)")
        return "\n\n".join(parts) + "\n", ctx.bq_tables

    lines = ["from pyspark.sql import SparkSession", "",
             'spark = SparkSession.builder.appName("converted").getOrCreate()', ""]
    all_tables = []
    for s in converted:
        for t in tables_in(s):
            if t not in all_tables:
                all_tables.append(t)
    if dialect == "bigquery" and ctx.bq_tables and not bq_views:
        lines.append("# BigQuery tables: read with the spark-bigquery connector and register as views/catalog tables:")
        for t in ctx.bq_tables:
            lines.append(f"#   spark.read.format('bigquery').load({t!r})")
        lines.append("")
    if all_tables:
        lines.append("# Tables referenced - register DataFrames as views if they are not already catalog tables:")
        for t in all_tables:
            lines.append(f"#   <df>.createOrReplaceTempView({t.strip('`')!r})")
        lines.append("")
    if macros:
        lines += macro_code(macros, lambda sql_text: convert_sql(sql_text, report, dialect, out="sql"))
    for i, s in enumerate(converted):
        if s.startswith("--") and "\n" not in s.strip():
            if s.startswith("-- __EXEC__"):
                from .teradata_macros import exec_call
                call = exec_call(s)
                lines.append(call if call else "# TODO(sparqix): EXEC with non-literal arguments: call the macro function by hand")
                if not call:
                    report.todo("macro", "EXEC with non-literal arguments: call the generated macro function yourself")
            else:
                lines.append("# " + cm(s.strip().lstrip("-").strip()))
            lines.append("")
            continue
        body = s.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
        is_query = re.match(r"(?is)^\s*(--[^\n]*\n\s*)*(select|with)\b", s) is not None
        var = "result" if len(converted) == 1 else f"result_{i + 1}"
        lines.append(f'{var + " = " if is_query else ""}spark.sql("""')
        lines.append(body)
        lines.append('""")')
        lines.append("")
    return "\n".join(lines).rstrip() + "\n", ctx.bq_tables
