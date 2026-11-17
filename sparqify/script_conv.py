"""Hive / Impala scripts (.hql) and Teradata BTEQ scripts -> a PySpark script that runs each statement with spark.sql().

Statements are split on `;` (quote and comment aware), tool commands are handled (SET, ADD JAR, COMPUTE STATS, INVALIDATE METADATA, .LOGON,
.EXPORT, .IF ERRORCODE ...), and the SQL itself goes through the SQL converter (dialect hive / teradata -> Spark SQL).
Script variables (${hiveconf:x}, ${var:x}, SET x=...) are substituted at runtime from the `P` dict at the top of the file.
"""
from __future__ import annotations

import re

from .core import Report, Unsupported, cm
from .sql_conv import convert_sql


def split_statements(src: str):
    out, buf, i, n = [], [], 0, len(src)
    while i < n:
        ch = src[i]
        if ch in "'\"`":
            j = i + 1
            while j < n and src[j] != ch:
                j += 2 if src[j] == "\\" else 1
            buf.append(src[i:j + 1])
            i = j + 1
        elif src.startswith("--", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            buf.append("\n")           # drop comment but keep line structure
            i = j
        elif src.startswith("/*", i) and not src.startswith("/*+", i):
            j = src.find("*/", i + 2)
            i = n if j < 0 else j + 2
            buf.append(" ")
        elif ch == ";":
            out.append("".join(buf).strip())
            buf = []
            i += 1
        else:
            buf.append(ch)
            i += 1
    if "".join(buf).strip():
        out.append("".join(buf).strip())
    return [s for s in out if s]


def _lit(sql: str) -> str:
    return '"""' + sql.replace("\\", "\\\\").replace('"""', '\\"\\"\\"') + '"""'


_HEADER = '''import re
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("{name}").enableHiveSupport().getOrCreate()

P = {{}}   # script variables: fill in or pass in; used for ${{...}} substitution


def q(sql):
    """Substitute ${{hiveconf:x}} / ${{hivevar:x}} / ${{var:x}} / ${{x}} from P."""
    def sub(m):
        k = m.group(1).split(":")[-1]
        if k not in P:
            raise KeyError("script variable not set: " + k)
        return str(P[k])
    return re.sub(r"\\$\\{{(?:hiveconf:|hivevar:|var:)?([\\w.]+)\\}}", sub, sql)

'''


def convert_hive(source: str, report: Report) -> str:
    lines = [_HEADER.format(name="hive_script")]
    stmts = split_statements(source)
    if not stmts:
        report.todo("hive", "no statements found")
    dyn = False
    for n, st in enumerate(stmts, 1):
        where = f"stmt {n}"
        low = " ".join(st.lower().split())
        m = re.match(r"(?is)^set\s+(?:(?:hivevar|hiveconf|var):)?([\w.\-]+)\s*=\s*(.*)$", st)
        if m:
            k, v = m.group(1), m.group(2).strip().strip("'\"")
            if re.match(r"(?i)(hive|mapred|mapreduce|tez|spark\.|dfs|yarn|hadoop|parquet|orc|io\.|fs\.)", k) and not re.match(r"(?i)spark\.sql", k):
                report.note(f"SET {k} ignored (Hive/engine tuning): use spark.sql.* settings if you need an equivalent")
            elif re.match(r"(?i)spark\.sql", k):
                lines.append(f"spark.conf.set({k!r}, {v!r})")
            else:
                lines.append(f"P.setdefault({k!r}, {v!r})")
            continue
        if re.match(r"(?i)^set\s*$|^set\s+-v$", st):
            continue
        if re.match(r"(?is)^add\s+(jar|file|archive)\b", st):
            report.todo(where, f"{st.split()[0].upper()} {st.split()[1].upper()}: ship the dependency with the cluster/job instead: {st[:60]}")
            lines.append(f"# TODO(sparqify): {cm(' '.join(st.split())[:100])}")
            continue
        if re.match(r"(?is)^invalidate\s+metadata\s*$", st):
            report.note("INVALIDATE METADATA (no table) ignored: Spark reads the catalog directly")
            continue
        m = re.match(r"(?is)^(?:invalidate\s+metadata|refresh)\s+([\w.`]+)$", st)
        if m:
            lines.append(f"spark.sql({_lit('REFRESH TABLE ' + m.group(1))})")
            continue
        m = re.match(r"(?is)^compute\s+(incremental\s+)?stats\s+([\w.`]+)(.*)$", st)
        if m:
            lines.append(f"spark.sql({_lit('ANALYZE TABLE ' + m.group(2) + ' COMPUTE STATISTICS')})")
            if m.group(3).strip():
                report.warn(where, "COMPUTE STATS column list / incremental options ignored")
            continue
        m = re.match(r"(?is)^alter\s+table\s+([\w.`]+)\s+recover\s+partitions$", st)
        if m:
            lines.append(f"spark.sql({_lit('MSCK REPAIR TABLE ' + m.group(1))})")
            continue
        if re.match(r"(?is)^(upsert\s+into|transform\b)|\bTRANSFORM\s*\(.*\)\s*USING\b", st):
            report.todo(where, f"not convertible automatically (Kudu UPSERT / TRANSFORM ... USING script): {st[:70]}")
            lines.append(f"# TODO(sparqify): {cm(' '.join(st.split())[:100])}")
            continue
        sql = st
        sql = re.sub(r"(?i)\bSTRAIGHT_JOIN\b", "", sql)
        sql = re.sub(r"/\*\+\s*(SHUFFLE|NOSHUFFLE|CLUSTERED|NOCLUSTERED)[^*]*\*/", "", sql, flags=re.I)
        sql = re.sub(r"(?i)\[\s*(SHUFFLE|NOSHUFFLE|BROADCAST)\s*\]", lambda m: "/*+ BROADCAST */" if False else "", sql)
        sql = re.sub(r"(?i)\bzeroifnull\(\s*([^()]+?)\s*\)", r"COALESCE(\1, 0)", sql)
        sql = re.sub(r"(?i)\bnullifzero\(\s*([^()]+?)\s*\)", r"NULLIF(\1, 0)", sql)
        if re.search(r"(?i)\bpartition\s*\(\s*\w+\s*(,|\))", sql) and re.match(r"(?is)^insert\s+overwrite", sql) and not dyn:
            dyn = True
            lines.append("spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')  # Hive-style dynamic-partition overwrite")
            report.note("dynamic partition overwrite: spark.sql.sources.partitionOverwriteMode=dynamic set (Hive's default behaviour)")
        # keep ${var} placeholders out of the SQL parser
        holders = {}
        def keep(m):
            k = f"__V{len(holders)}__"
            holders[k] = m.group(0)
            return k
        sql = re.sub(r"\$\{[^}]*\}", keep, sql)
        sub = Report()
        try:
            conv = convert_sql(sql, sub, dialect="hive", out="sql").strip().rstrip(";").strip() or sql
        except Exception as e:                       # noqa: BLE001 - keep the statement as written
            conv = sql
            report.warn(where, f"SQL converter could not parse the statement ({e.__class__.__name__}); kept as written")
        for k, v in holders.items():
            conv = conv.replace(k, v)
        for ln, msg in sub.todos:
            report.todo(where, msg)
        for ln, msg in sub.warnings:
            report.warn(where, msg)
        lines.append(f"spark.sql(q({_lit(conv)}))")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------------------------- BTEQ
_BTEQ_HEAD = '''import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bteq_script").getOrCreate()

'''


def _bteq_sql(st: str, report: Report, where: str):
    s = st.strip()
    s = re.sub(r"(?is)^\s*locking\s+(row|table|database)?[^;]*?\s+(for|in)\s+(access|read|write|exclusive)\s+(nowait\s+)?", "", s)
    s = re.sub(r"(?i)\bsel\b(?!\s*\()", "SELECT", s)
    s = re.sub(r"(?is)^ins\b", "INSERT", s)
    s = re.sub(r"(?is)^upd\b", "UPDATE", s)
    s = re.sub(r"(?is)^del\b", "DELETE", s)
    s = re.sub(r"(?is)^(delete\s+from\s+[\w.]+)\s+all$", r"\1", s)
    m = re.match(r"(?is)^(?:bt|et)$", s)
    if m:
        return None, "transaction markers (BT/ET) ignored: use Delta transactions per statement"
    m = re.match(r"(?is)^database\s+(\w+)$", s)
    if m:
        return f"USE {m.group(1)}", None
    m = re.match(r"(?is)^collect\s+(?:statistics|stats)\s+(?:.*?\s+)?on\s+([\w.]+)", s)
    if m:
        return f"ANALYZE TABLE {m.group(1)} COMPUTE STATISTICS", None
    s = re.sub(r"(?is)^create\s+(?:multiset\s+|set\s+)?(?:volatile\s+|global\s+temporary\s+)?table", "CREATE TABLE", s)
    s = re.sub(r"(?is)\s+(?:unique\s+)?primary\s+index\s*\([^)]*\)", "", s)
    s = re.sub(r"(?is)\s+with\s+data\b(?!\s*and)", "", s)
    s = re.sub(r"(?is)\s+on\s+commit\s+preserve\s+rows", "", s)
    if re.search(r"(?is)\bwith\s+no\s+data\b", s):
        s = re.sub(r"(?is)\s+with\s+no\s+data\b", "", s)
        s = f"{s} LIMIT 0" if re.match(r"(?is)^create table .* as\s*\(?\s*select", s) and "limit" not in s.lower() else s
        report.warn(where, "WITH NO DATA approximated; check the generated CTAS")
    return s, None


def convert_bteq(source: str, report: Report) -> str:
    lines = [_BTEQ_HEAD]
    # BTEQ commands start with '.' at line start and end with ';' (or newline); split into units
    units, buf = [], []
    for raw in source.splitlines():
        t = raw.strip()
        if not buf and t.startswith("."):
            units.append(t.rstrip(";").strip())
            continue
        buf.append(raw)
        if t.endswith(";"):
            units.append("\n".join(buf))
            buf = []
    if buf and "".join(buf).strip():
        units.append("\n".join(buf))
    sql_units = []
    for u in units:
        if u.startswith("."):
            sql_units.append(u)
        else:
            sql_units.extend(split_statements(u) or [])
    export = None
    nstmt = 0
    for u in sql_units:
        nstmt += 1
        where = f"stmt {nstmt}"
        if u.startswith("."):
            cmd = u[1:].strip()
            up = cmd.upper()
            if up.startswith(("LOGON", "LOGOFF", "LOGOFF", "SET ", "OS ", "REMARK", "ERRORLEVEL")) or up in ("LOGOFF",):
                if up.startswith("OS "):
                    report.todo(where, f"BTEQ .OS command not converted: {cmd}")
                    lines.append(f"# TODO(sparqify): .{cm(cmd)}")
                else:
                    report.note(f".{cmd.split()[0].upper()} ignored (connection / session settings): configure the Spark catalog instead")
                continue
            if up.startswith("QUIT") or up.startswith("EXIT"):
                m = re.match(r"(?i)(?:quit|exit)\s*(\d+)?", cmd)
                lines.append(f"sys.exit({int(m.group(1) or 0)})")
                continue
            m = re.match(r"(?is)if\s+errorcode\s*(<>|!=|>|=|==)\s*(\d+)\s+then\s+\.?(quit|exit)\s*(\d+)?", cmd)
            if m:
                report.note("IF ERRORCODE ... THEN QUIT: a failing statement raises an exception here, which stops the script (exit code differs)")
                continue
            m = re.match(r"(?is)if\s+activitycount\s*(<>|!=|>=|<=|>|<|=|==)\s*(\d+)\s+then\s+\.?(quit|exit)\s*(\d+)?", cmd)
            if m:
                op = {"=": "==", "<>": "!="}.get(m.group(1), m.group(1))
                lines.append(f"if _last is not None and _last.count() {op} {m.group(2)}:  # TODO(sparqify): ACTIVITYCOUNT only equals the row count of the previous SELECT")
                lines.append(f"    sys.exit({int(m.group(4) or 0)})")
                report.todo(where, "ACTIVITYCOUNT after DML is not available in Spark; verify this check")
                lines.insert(1, "_last = None") if "_last = None" not in lines else None
                continue
            m = re.match(r"(?is)export\s+(report|data|indicdata)\s+file\s*=\s*(\S+)", cmd)
            if m:
                export = m.group(2).strip("'\"")
                continue
            if up.startswith("EXPORT") and "RESET" in up:
                export = None
                continue
            report.todo(where, f"BTEQ command not converted: .{cmd[:60]}")
            lines.append(f"# TODO(sparqify): .{cm(cmd[:100])}")
            continue
        sql, note = _bteq_sql(u, report, where)
        if sql is None:
            report.note(note)
            continue
        sub = Report()
        try:
            conv = convert_sql(sql, sub, dialect="teradata", out="sql").strip().rstrip(";").strip() or sql
        except Exception as e:                       # noqa: BLE001
            conv = sql
            report.warn(where, f"SQL converter could not parse the statement ({e.__class__.__name__}); kept as written")
        for _, msg in sub.todos:
            report.todo(where, msg)
        for _, msg in sub.warnings:
            report.warn(where, msg)
        is_select = re.match(r"(?is)^\s*(select|with)\b", conv) is not None
        if export and is_select:
            lines.append(f"spark.sql({_lit(conv)}).write.mode('overwrite').csv({export!r}, header=True)")
            report.note(f"{where}: EXPORT written as a Spark CSV directory at {export}; REPORT-format layout is not reproduced")
        elif is_select:
            lines.append(f"_last = spark.sql({_lit(conv)})")
            lines.append("_last.show(truncate=False)")
            if "_last = None" not in lines:
                lines.insert(1, "_last = None")
        else:
            lines.append(f"spark.sql({_lit(conv)})")
    return "\n".join(lines) + "\n"
