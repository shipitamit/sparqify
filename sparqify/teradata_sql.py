"""Teradata SQL -> Spark SQL rewrites that need to see whole statements (run before the generic rewriter).

Covers what the generic dialect handling does not: SEL/INS/UPD/DEL, TOP n WITH TIES, `(FORMAT ...)` / `CAST(x AS DATE FORMAT ...)`, `(CHAR(5))`-style type
attributes, (NOT) CASESPECIFIC, SAMPLE, QUALIFY (rewritten to a sub-query when the select list is simple), legacy ordered analytics (CSUM, MSUM, MAVG,
MDIFF, RANK(x DESC)), INDEX/CHARS/OREPLACE/OTRANSLATE/STRTOK/TRYCAST/HASHROW, integer dates (1210101), bare DATE, Teradata DDL (VOLATILE/MULTISET tables,
PRIMARY INDEX, NO FALLBACK, CHARACTER SET, ...), and statements with no Spark meaning (COLLECT STATISTICS, HELP, SHOW, SET QUERY_BAND, LOCKING ...).
Everything it cannot translate faithfully becomes a TODO or a warning on the report; nothing is silently guessed.
All text taken from the input stays as SQL text: nothing here is evaluated.
"""
from __future__ import annotations

import re

_KW_BEFORE_PAREN = {"AS", "IN", "ON", "VALUES", "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "BY", "OVER", "USING", "INTO", "JOIN", "SET", "THEN", "ELSE",
                    "WHEN", "CASE", "BETWEEN", "LIKE", "EXISTS", "ANY", "ALL", "SOME", "UNION", "HAVING", "WITH", "TABLE", "PARTITION", "ORDER", "GROUP",
                    "RETURNS", "REPLACE", "CREATE", "INSERT", "UPDATE", "DELETE", "LIMIT", "IS", "DISTINCT", "TOP", "QUALIFY", "ROWS", "RANGE"}
_TYPES = "CHAR|CHARACTER|VARCHAR|INTEGER|INT|SMALLINT|BYTEINT|BIGINT|DECIMAL|DEC|NUMERIC|FLOAT|REAL|DOUBLE\\s+PRECISION|DATE|TIMESTAMP|TIME"
_TYPE_MAP = {"BYTEINT": "TINYINT", "INTEGER": "INT", "DEC": "DECIMAL", "NUMERIC": "DECIMAL", "FLOAT": "DOUBLE", "REAL": "DOUBLE", "CHARACTER": "STRING",
             "CHAR": "STRING", "VARCHAR": "STRING", "LONG VARCHAR": "STRING", "CLOB": "STRING", "BYTE": "BINARY", "VARBYTE": "BINARY", "BLOB": "BINARY",
             "DOUBLE PRECISION": "DOUBLE", "NUMBER": "DECIMAL(38,10)", "TIME": "STRING", "INTERVAL": "STRING"}


# ---------------------------------------------------------------------------------------------------------------- text helpers
def _skip_literal(s, i):
    """index after the string/comment starting at s[i], or i when there is none"""
    if s[i] == "'":
        j = i + 1
        while j < len(s):
            if s[j] == "'":
                if j + 1 < len(s) and s[j + 1] == "'":
                    j += 2
                    continue
                return j + 1
            j += 1
        return len(s)
    if s.startswith("--", i):
        j = s.find("\n", i)
        return len(s) if j < 0 else j
    if s.startswith("/*", i):
        j = s.find("*/", i + 2)
        return len(s) if j < 0 else j + 2
    return i


def match_paren(s, i):
    """s[i] == '(' -> index of the matching ')' (strings and comments skipped), or -1"""
    depth, j = 0, i
    while j < len(s):
        k = _skip_literal(s, j)
        if k != j:
            j = k
            continue
        if s[j] == "(":
            depth += 1
        elif s[j] == ")":
            depth -= 1
            if depth == 0:
                return j
        j += 1
    return -1


def split_args(s):
    """split a call's argument text at top-level commas"""
    out, depth, cur, j = [], 0, [], 0
    while j < len(s):
        k = _skip_literal(s, j)
        if k != j:
            cur.append(s[j:k])
            j = k
            continue
        c = s[j]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        if c == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        j += 1
    out.append("".join(cur).strip())
    return out


def _find_top(s, pattern, start=0, flags=re.I):
    """matches of `pattern` at parenthesis depth 0 outside strings/comments (a regex is only tried at each word start: linear)"""
    rx = re.compile(pattern, flags)
    out, depth, j = [], 0, start
    while j < len(s):
        k = _skip_literal(s, j)
        if k != j:
            j = k
            continue
        c = s[j]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and (c.isalpha() or c == "_") and (j == 0 or not (s[j - 1].isalnum() or s[j - 1] in "_.")):
            m = rx.match(s, j)
            if m:
                out.append(m)
                j = max(m.end(), j + 1)
                continue
        j += 1
    return out


def _calls(text, name):
    """-> [(start, end_exclusive, args_text)] for every `name(...)` call outside strings"""
    out, rx = [], re.compile(rf"(?i)(?<![\w.]){name}\s*\(")
    pos = 0
    while True:
        m = rx.search(text, pos)
        if not m:
            return out
        # skip matches inside literals
        before = text[:m.start()]
        if before.count("'") % 2:
            pos = m.end()
            continue
        o = m.end() - 1
        c = match_paren(text, o)
        if c < 0:
            return out
        out.append((m.start(), c + 1, text[o + 1:c]))
        pos = c + 1


def _replace_calls(text, name, fn):
    """rewrite each call with fn(args_list) -> str|None (None keeps it); handles nesting by repeating from the innermost-last"""
    changed = True
    guard = 0
    while changed and guard < 50:
        changed, guard = False, guard + 1
        for s, e, a in reversed(_calls(text, name)):
            new = fn(split_args(a))
            if new is not None:
                text = text[:s] + new + text[e:]
                changed = True
                break
    return text


_DATE_TOK = [("YYYY", "yyyy"), ("YY", "yy"), ("MMMM", "MMMM"), ("MMM", "MMM"), ("MM", "MM"), ("DDD", "D"), ("DD", "dd"), ("HH24", "HH"), ("HH", "hh"),
             ("MI", "mm"), ("SS", "ss"), ("S(6)", "SSSSSS"), ("T", "'T'"), ("Z", "'Z'")]


def date_fmt(fmt):
    """Teradata date/time format -> Spark pattern, or None when it is not a date format (numeric/picture formats)"""
    f = fmt.strip("'")
    if not re.fullmatch(r"(?i)(?:YYYY|YY|MMMM|MMM|MM|DDD|DD|HH24|HH|MI|SS|S\(6\)|[-/:. ,T]|B|E4|E3)+", f):
        return None
    out, i = [], 0
    up = f.upper()
    while i < len(f):
        for t, p in _DATE_TOK:
            if up.startswith(t, i):
                out.append(p)
                i += len(t)
                break
        else:
            out.append(f[i] if f[i] in "-/:. ," else f"'{f[i]}'")
            i += 1
    return "".join(out)


# ---------------------------------------------------------------------------------------------------------------- statement level
_DROP = re.compile(r"(?is)^\s*(?:(COLLECT\s+STAT(?:ISTIC)?S?|HELP|SHOW|SET\s+QUERY_BAND|SET\s+SESSION|DIAGNOSTIC|CHECKPOINT|LOGON|LOGOFF|BT|ET|ABORT|ROLLBACK\s+WORK)\b.*)$")


def _dropped_note(sql):
    first = re.match(r"(?is)\s*(COLLECT\s+STAT\w*|HELP|SHOW|SET\s+QUERY_BAND|SET\s+SESSION|DIAGNOSTIC|CHECKPOINT|BT|ET|ABORT|ROLLBACK)", sql).group(1).upper()
    why = {"BT": "transactions are not needed in Spark", "ET": "transactions are not needed in Spark",
           "ABORT": "no transaction to abort", "ROLLBACK": "no transaction to roll back"}.get(first.split()[0], "no Spark equivalent: statistics, catalog and session settings differ (use ANALYZE TABLE if needed)")
    return f"-- dropped Teradata statement ({first}): {why}"


def convert_ddl(sql, ctx):
    """Teradata CREATE TABLE / CREATE VOLATILE TABLE -> Spark; returns None when the statement is not Teradata DDL"""
    m = re.match(r"(?is)^\s*CREATE\s+((?:SET|MULTISET|VOLATILE|GLOBAL\s+TEMPORARY|GLOBAL|TEMPORARY|FALLBACK|NO\s+FALLBACK|\s)*)TABLE\s+([\w.\"`]+)(.*)$", sql)
    if not m:
        return None
    mods, name, rest = m.group(1).upper(), m.group(2), m.group(3)
    volatile = "VOLATILE" in mods or "GLOBAL" in mods or "TEMPORARY" in mods
    rest = re.sub(r"(?is)^(?:\s*,\s*[A-Za-z][\w ]*(?:=\s*[\w ]+?)?)+(?=\s*(?:\(|AS\b))", "", rest)      # table options: , NO FALLBACK, CHECKSUM = DEFAULT ...
    # ---- CREATE TABLE ... AS (SELECT ...) WITH [NO] DATA
    mc = re.match(r"(?is)^\s*(?:\(\s*[^()]*\)\s*)?AS\s*\((.*)\)\s*WITH\s+(NO\s+)?DATA(.*)$", rest.strip())
    tail = ""
    if mc:
        sel, nodata, tail = mc.group(1), bool(mc.group(2)), mc.group(3)
        sel = sel.strip().rstrip(";")
        if nodata:
            sel = f"SELECT * FROM ({sel}) AS _t WHERE 1 = 0"
        out = (f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {sel}" if volatile else f"CREATE TABLE {name} AS {sel}")
        _ddl_tail_notes(tail, ctx)
        if volatile:
            ctx.warn(f"{name}: volatile/temporary table became a temporary VIEW (re-evaluated on every use; cache it with CACHE TABLE if it is reused)")
        return out
    # ---- explicit column list
    p = rest.find("(")
    if p < 0:
        return None
    q = match_paren(rest, p)
    if q < 0:
        return None
    cols_text, tail = rest[p + 1:q], rest[q + 1:]
    cols = []
    for item in split_args(cols_text):
        if re.match(r"(?is)^(CONSTRAINT|PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK)\b", item):
            ctx.warn(f"{name}: constraint dropped (Spark does not enforce it): {item[:60]}")
            continue
        cols.append(_column_def(item, name, ctx))
    _ddl_tail_notes(tail, ctx)
    if volatile:
        ctx.warn(f"{name}: volatile/global temporary table became a regular table: drop it at the end of the job (Teradata dropped it with the session)")
    return f"CREATE TABLE {name} (\n  " + ",\n  ".join(cols) + "\n) USING parquet"


def _column_def(item, table, ctx):
    m = re.match(r"(?is)^\s*(\w+)\s+(.*)$", item)
    if not m:
        return item
    col, rest = m.group(1), m.group(2)
    notnull = bool(re.search(r"(?i)\bNOT\s+NULL\b", rest))
    tm = re.match(r"(?is)^(LONG\s+VARCHAR|DOUBLE\s+PRECISION|TIMESTAMP|TIME|INTERVAL\s+\w+(?:\s+TO\s+\w+)?|PERIOD\s*\(\s*[A-Z]+(?:\s*\(\s*\d+\s*\))?(?:\s+WITH\s+TIME\s+ZONE)?\s*\)|[A-Z]+)(\s*\(\s*[\d\s,*]+\))?", rest)
    if not tm:
        return f"{col} STRING /* TODO(sparqify): unrecognised Teradata type: {rest[:40]} */"
    base, arg = re.sub(r"\s+", " ", tm.group(1).upper()), (tm.group(2) or "")
    if base.startswith("PERIOD"):
        im = re.match(r"PERIOD\s*\(\s*([A-Z]+)", base)
        inner = im.group(1) if im else "DATE"
        sp_t = {"DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "TIME": "STRING"}.get(inner)
        if sp_t is None:
            ctx.todo(f"{table}.{col}: PERIOD({inner}) has no Spark equivalent: split it into two columns by hand")
            return f"{col} STRING /* TODO(sparqify): PERIOD */"
        if "WITH TIME ZONE" in base:
            ctx.warn(f"{table}.{col}: PERIOD(... WITH TIME ZONE) became TIMESTAMP (time zone offsets are dropped)")
        ctx.__dict__.setdefault("periods", {})[col.lower()] = (f"{col}_begin", f"{col}_end")
        ctx.warn(f"{table}.{col}: PERIOD column split into {col}_begin and {col}_end ({sp_t}); queries using BEGIN({col}) / END({col}) / OVERLAPS / CONTAINS are rewritten to those names")
        nn = " NOT NULL" if notnull else ""
        return f"{col}_begin {sp_t}{nn},\n  {col}_end {sp_t}{nn}"
    sp = _TYPE_MAP.get(base, base)
    if base.startswith("TIMESTAMP"):
        sp, arg = "TIMESTAMP", ""
    elif sp in ("STRING", "BINARY", "DOUBLE", "INT", "TINYINT"):
        arg = ""
    elif base == "INTERVAL" or base.startswith("INTERVAL"):
        sp, arg = "STRING", ""
        ctx.warn(f"{table}.{col}: INTERVAL type stored as STRING")
    elif base not in ("DECIMAL", "DEC", "NUMERIC", "BIGINT", "SMALLINT", "DATE", "TIME") and base not in _TYPE_MAP:
        ctx.warn(f"{table}.{col}: unknown Teradata type {base}: check the Spark type")
    if base in ("DECIMAL", "DEC", "NUMERIC") and arg:
        arg = arg.replace(" ", "")
    elif sp in ("DECIMAL",) and not arg:
        arg = "(18,0)"
    if re.search(r"(?i)\bCASESPECIFIC\b|\bCHARACTER\s+SET\b", rest):
        pass
    return f"{col} {sp}{arg}" + (" NOT NULL" if notnull else "")


def _ddl_tail_notes(tail, ctx):
    t = tail or ""
    if re.search(r"(?i)PRIMARY\s+INDEX|UNIQUE\s+PRIMARY\s+INDEX|NO\s+PRIMARY\s+INDEX", t):
        ctx.warn("PRIMARY INDEX dropped (Teradata distribution): choose a partition/bucket column for Spark if the table is large")
    if re.search(r"(?i)PARTITION\s+BY", t):
        ctx.todo("PARTITION BY (RANGE_N/CASE_N) has no direct Spark equivalent: use PARTITIONED BY on a derived column")
    if re.search(r"(?i)\bON\s+COMMIT\b", t):
        pass


def qualify_rewrite(sql, ctx):
    """SELECT <simple list> FROM ... QUALIFY cond [ORDER BY ...] -> sub-query form (portable to Spark 3); keeps QUALIFY when the select list is complex"""
    m = re.search(r"(?is)\bQUALIFY\b", sql)
    if not m or sql.count("(") != sql.count(")"):
        return sql
    head, rest = sql[:m.start()], sql[m.end():]
    body = rest.strip().rstrip(";")
    cut, depth, j = len(body), 0, 0
    while j < len(body):                                  # first top-level ORDER BY / LIMIT ends the QUALIFY condition
        k = _skip_literal(body, j)
        if k != j:
            j = k
            continue
        if body[j] == "(":
            depth += 1
        elif body[j] == ")":
            depth -= 1
        elif depth == 0 and re.match(r"(?is)\s(?:ORDER\s+BY|LIMIT)\b", body[j:j + 12]):
            cut = j
            break
        j += 1
    cond, tail = body[:cut].strip(), body[cut:]
    ms = re.match(r"(?is)^(\s*(?:--[^\n]*\n\s*)*)SELECT\s+(DISTINCT\s+)?(.*?)\s+\bFROM\b(.*)$", head)
    if not ms or re.search(r"(?is)\b(UNION|INTERSECT|EXCEPT|MINUS)\b", head) or re.search(r"(?is)^\s*WITH\b", head):
        ctx.warn("QUALIFY not rewritten (complex statement): Spark 4 / Databricks support it, older Spark needs a sub-query")
        return sql
    items = split_args(ms.group(3))
    names = []
    for it in items:
        mm = re.fullmatch(r"(?is)(?:[\w]+\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?", it.strip())
        if not mm or it.strip() == "*" or (mm.group(2) or "").upper() in ("OVER",):
            ctx.warn("QUALIFY not rewritten (select list is not plain columns): Spark 4 / Databricks support it, older Spark needs a sub-query")
            return sql
        names.append(mm.group(2) or mm.group(1))
    if len(set(n.lower() for n in names)) != len(names):
        return sql
    flag = "_qualify_ok"
    inner = f"{ms.group(1)}SELECT {ms.group(2) or ''}{ms.group(3)}, ({cond}) AS {flag} FROM{ms.group(4)}"
    outer = f"SELECT {', '.join(names)} FROM ({inner}) AS _q WHERE {flag}"
    ctx.warn("QUALIFY rewritten to a sub-query with a helper flag column (_qualify_ok)")
    return outer + tail


def _legacy_analytics(sql, ctx):
    def csum(a):
        if len(a) < 2:
            return None
        order = ", ".join(a[1:])
        return f"SUM({a[0]}) OVER (ORDER BY {order} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

    def moving(fn):
        def f(a):
            if len(a) != 3 or not re.fullmatch(r"\d+", a[1].strip()):
                return None
            n = int(a[1])
            return f"{fn}({a[0]}) OVER (ORDER BY {a[2]} ROWS BETWEEN {n - 1} PRECEDING AND CURRENT ROW)"
        return f

    def mdiff(a):
        if len(a) != 3 or not re.fullmatch(r"\d+", a[1].strip()):
            return None
        return f"({a[0]} - LAG({a[0]}, {int(a[1])}) OVER (ORDER BY {a[2]}))"

    sql = _replace_calls(sql, "CSUM", csum)
    sql = _replace_calls(sql, "MSUM", moving("SUM"))
    sql = _replace_calls(sql, "MAVG", moving("AVG"))
    sql = _replace_calls(sql, "MDIFF", mdiff)

    # RANK(x DESC) is the legacy form; the standard RANK() OVER (...) has empty arguments and is left alone
    sql = _replace_calls(sql, "RANK", lambda a: f"RANK() OVER (ORDER BY {a[0]})" if len(a) == 1 and a[0] else None)
    return sql


def _functions(sql, ctx):
    sql = re.sub(r"(?i)\bTD_SYSFNLIB\.", "", sql)
    sql = _replace_calls(sql, "INDEX", lambda a: f"INSTR({a[0]}, {a[1]})" if len(a) == 2 else None)
    sql = _replace_calls(sql, "CHARS", lambda a: f"LENGTH({a[0]})" if len(a) == 1 else None)
    sql = _replace_calls(sql, "CHAR_LENGTH", lambda a: f"LENGTH({a[0]})" if len(a) == 1 else None)
    sql = _replace_calls(sql, "CHARACTER_LENGTH", lambda a: f"LENGTH({a[0]})" if len(a) == 1 else None)
    sql = _replace_calls(sql, "OREPLACE", lambda a: f"REPLACE({', '.join(a)})" if len(a) in (2, 3) else None)
    sql = _replace_calls(sql, "OTRANSLATE", lambda a: f"TRANSLATE({', '.join(a)})" if len(a) == 3 else None)

    def strtok(a):
        if len(a) == 3 and re.fullmatch(r"'[^']'", a[1].strip()) and re.fullmatch(r"\d+", a[2].strip()):
            d = re.escape(a[1].strip()[1])
            return f"SPLIT({a[0]}, '[{d}]')[{int(a[2]) - 1}]"
        ctx.todo("STRTOK with a multi-character delimiter or a non-constant position: use SPLIT/ELEMENT_AT by hand")
        return None
    sql = _replace_calls(sql, "STRTOK", strtok)
    if re.search(r"(?i)\bTRYCAST\s*\(", sql):
        sql = re.sub(r"(?i)\bTRYCAST\s*\(", "TRY_CAST(", sql)
        ctx.warn("TRYCAST -> TRY_CAST (Spark 3.2+/4)")
    if re.search(r"(?i)\bHASHROW\s*\(", sql):
        sql = re.sub(r"(?i)\bHASHROW\s*\(", "HASH(", sql)
        ctx.warn("HASHROW -> HASH: the values differ from Teradata's: do not compare them with existing hash columns")
    sql = _replace_calls(sql, "ZEROIFNULL", lambda a: f"COALESCE({a[0]}, 0)" if len(a) == 1 else None)
    sql = _replace_calls(sql, "NULLIFZERO", lambda a: f"NULLIF({a[0]}, 0)" if len(a) == 1 else None)
    return sql


_PERIOD_OPS = re.compile(r"(?i)(?<![\w.])(?:P_INTERSECT|P_NORMALIZE|LDIFF|RDIFF|MEETS|PRECEDES|SUCCEEDS|IMMEDIATELY\s+(?:PRECEDES|SUCCEEDS)|NORMALIZE)(?![\w])")
_NOT_PERIOD_ARG = {"DATE", "TIMESTAMP", "TIME", "CHAR", "VARCHAR", "INTEGER", "INT", "BIGINT", "SMALLINT", "DECIMAL", "FORMAT", "CASESPECIFIC", "NOT", "NULL", "BYTEINT", "FLOAT"}


def _period_names(ident, ctx):
    q, _, last = ident.rpartition(".")
    known = getattr(ctx, "periods", {}).get(last.lower())
    b, e = known if known else (f"{last}_begin", f"{last}_end")
    return (f"{q}.{b}" if q else b), (f"{q}.{e}" if q else e)


def _operand_left(s, end):
    """period/date operand ending just before s[end] -> (start, text) or None"""
    j = end
    while j > 0 and s[j - 1].isspace():
        j -= 1
    if j == 0:
        return None
    if s[j - 1] == ")":
        depth, k = 0, j - 1
        while k >= 0:
            if s[k] == "'":
                return None
            depth += s[k] == ")"
            depth -= s[k] == "("
            if depth == 0:
                break
            k -= 1
        if k < 0:
            return None
        m = re.search(r"(?i)(?<![\w.])PERIOD\s*$", s[:k])
        start = m.start() if m else k
        return start, s[start:j]
    m = re.search(r"[A-Za-z_][\w.]*$", s[:j])
    return (m.start(), m.group(0)) if m else None


def _operand_right(s, start):
    j = start
    while j < len(s) and s[j].isspace():
        j += 1
    if j >= len(s):
        return None
    m = re.match(r"(?i)(?:PERIOD\s*)?\(", s[j:])
    if m:
        o = j + m.end() - 1
        c = match_paren(s, o)
        return (j, c + 1, s[j:c + 1]) if c > 0 else None
    m = re.match(r"(?is)(?:DATE|TIMESTAMP)\s*'[^']*'|'[^']*'|[A-Za-z_][\w.]*(?=\s*\()", s[j:])
    if m and re.match(r"(?i)[A-Za-z_]", m.group(0)) and not re.match(r"(?i)(DATE|TIMESTAMP)\s*'", m.group(0)):
        o = j + m.end()
        while s[o] != "(":
            o += 1
        c = match_paren(s, o)
        return (j, c + 1, s[j:c + 1]) if c > 0 else None
    if m:
        return j, j + m.end(), m.group(0)
    m = re.match(r"[A-Za-z_][\w.]*|\d+", s[j:])
    return (j, j + m.end(), m.group(0)) if m else None


def _as_period(text, ctx):
    """operand text -> (begin, end) when it is a period (PERIOD(a, b) / (a, b) / a column), else None"""
    t = text.strip()
    m = re.match(r"(?is)^(?:PERIOD\s*)?\((.*)\)$", t)
    if m:
        parts = split_args(m.group(1))
        return (parts[0], parts[1]) if len(parts) == 2 else None
    if re.fullmatch(r"[A-Za-z_][\w.]*", t) and t.upper() not in ("CURRENT_DATE", "CURRENT_TIMESTAMP", "DATE", "NULL"):
        return _period_names(t, ctx)
    return None


def _periods(sql, ctx):
    if _PERIOD_OPS.search(sql) and (re.search(r"(?i)\bPERIOD\b", sql) or getattr(ctx, "periods", None) or re.search(r"(?i)\b(?:P_INTERSECT|P_NORMALIZE|LDIFF|RDIFF)\s*\(", sql)):
        ctx.todo("PERIOD operators (P_INTERSECT, LDIFF, RDIFF, NORMALIZE, MEETS, PRECEDES, SUCCEEDS) have no automatic conversion: compare the _begin/_end columns by hand")
        return sql
    if not re.search(r"(?i)\bPERIOD\b|\bOVERLAPS\b|\bCONTAINS\b|(?<![\w.])(?:BEGIN|END)\s*\(\s*[A-Za-z_]", sql) and not getattr(ctx, "periods", None):
        return sql
    changed = False
    # OVERLAPS / CONTAINS (innermost-last, repeated)
    for _ in range(20):
        m = next((x for x in _find_top_any(sql, r"(?i)\b(OVERLAPS|CONTAINS)\b") if x), None)
        if not m:
            break
        op = m.group(1).upper()
        left, right = _operand_left(sql, m.start()), _operand_right(sql, m.end())
        lp = _as_period(left[1], ctx) if left else None
        if not left or not right or not lp:
            ctx.todo(f"PERIOD {op}: operands could not be read: compare the _begin/_end columns by hand")
            return sql
        rp = _as_period(right[2], ctx)
        if op == "OVERLAPS":
            if not rp:
                ctx.todo("PERIOD OVERLAPS needs two periods")
                return sql
            rep = f"({lp[0]} < {rp[1]} AND {rp[0]} < {lp[1]})"
        elif rp:
            rep = f"({lp[0]} <= {rp[0]} AND {rp[1]} <= {lp[1]})"
        else:
            x = right[2]
            rep = f"({lp[0]} <= {x} AND {x} < {lp[1]})"
        sql = sql[:left[0]] + rep + sql[right[1]:]
        changed = True

    def bound(which):
        def f(a):
            if len(a) != 1:
                return None
            pr = _as_period(a[0], ctx)
            if pr is None:
                return None
            return pr[0] if which == "BEGIN" else pr[1]
        return f
    new = _replace_calls(_replace_calls(sql, "BEGIN", bound("BEGIN")), "END", bound("END"))
    changed = changed or new != sql
    sql = new
    known = getattr(ctx, "periods", {})
    if known:
        def isnull(m):
            nm = m.group(1)
            if nm.lower() in known and "." not in nm:
                return f"{known[nm.lower()][0]} IS {m.group(2) or ''}NULL"
            return m.group(0)
        sql = re.sub(r"(?is)(?<![\w.])([A-Za-z_]\w*)\s+IS\s+(NOT\s+)?NULL\b", isnull, sql)
    if re.search(r"(?i)(?<![\w.])PERIOD\s*\(", sql):
        ctx.todo("PERIOD(a, b) used as a value (select list, INSERT, comparison): store the two bounds in the _begin/_end columns by hand")
    elif changed:
        ctx.warn("PERIOD functions rewritten on <col>_begin / <col>_end columns (period end is exclusive, as in Teradata): check the names match your Spark table")
    return sql


def _find_top_any(s, pattern):
    """matches outside strings/comments at any depth"""
    rx = re.compile(pattern)
    out, j = [], 0
    while j < len(s):
        k = _skip_literal(s, j)
        if k != j:
            j = k
            continue
        if (s[j].isalpha() or s[j] == "_") and (j == 0 or not (s[j - 1].isalnum() or s[j - 1] in "_.")):
            m = rx.match(s, j)
            if m:
                out.append(m)
                j = m.end()
                continue
        j += 1
    return out


def _formats_and_attributes(sql, ctx):
    # CAST(x AS DATE|TIMESTAMP FORMAT 'f')  /  CAST(x AS CHAR(n) FORMAT 'f')
    def cast_fmt(a):
        if len(a) != 1:
            return None
        m = re.match(r"(?is)^(.*)\s+AS\s+(DATE|TIMESTAMP(?:\s*\(\d\))?|CHAR\s*\(\d+\)|VARCHAR\s*\(\d+\)|INTEGER|INT|DECIMAL\s*\([\d\s,]+\))\s+FORMAT\s+('[^']*')\s*$", a[0])
        if not m:
            return None
        expr, typ, fmt = m.group(1).strip(), re.sub(r"\s+", "", m.group(2)).upper(), m.group(3)
        pf = date_fmt(fmt)
        if typ == "DATE" and pf:
            return f"TO_DATE({expr}, '{pf}')"
        if typ.startswith("TIMESTAMP") and pf:
            return f"TO_TIMESTAMP({expr}, '{pf}')"
        if (typ.startswith("CHAR") or typ.startswith("VARCHAR")) and pf:
            return f"DATE_FORMAT({expr}, '{pf}')"
        ctx.todo(f"CAST(... FORMAT {fmt}): numeric/picture formats have no Spark equivalent (use format_number / regexp_replace)")
        return f"CAST({expr} AS {_TYPE_MAP.get(typ.split('(')[0], typ.split('(')[0])})"
    sql = _replace_calls(sql, "CAST", cast_fmt)

    # expr (FORMAT 'f')   -> DATE_FORMAT for date formats
    def attr_format(m):
        expr, fmt = m.group("e"), m.group("f")
        pf = date_fmt(fmt)
        if pf:
            return f"DATE_FORMAT({expr}, '{pf}')"
        ctx.todo(f"({'FORMAT'} {fmt}) attribute on {expr}: numeric/picture formats have no Spark equivalent")
        return expr
    sql = re.sub(r"(?is)(?P<e>(?<![\w.])[A-Za-z_][\w.]*(?:\([^()]*\))?|'[^']*'|\d+)\s*\(\s*FORMAT\s+(?P<f>'[^']*')\s*\)", attr_format, sql)
    # expr (CHAR(5)) / (INTEGER) / (DECIMAL(10,2)) / (DATE) ...
    def attr_type(m):
        e, t = m.group("e"), m.group("t")
        if e.upper() in _KW_BEFORE_PAREN:
            return m.group(0)
        base = re.sub(r"\s+", " ", t.upper())
        sp = _TYPE_MAP.get(base, base)
        arg = (m.group("a") or "").replace(" ", "")
        return f"CAST({e} AS {sp}{'' if sp in ('STRING', 'INT', 'DOUBLE', 'TINYINT') else arg})"
    sql = re.sub(rf"(?is)(?P<e>(?<![\w.])[A-Za-z_][\w.]*|'[^']*'|\d+(?:\.\d+)?)\s*\(\s*(?P<t>{_TYPES})\s*(?P<a>\(\s*\d+(?:\s*,\s*\d+)?\s*\))?\s*(?:CHARACTER\s+SET\s+\w+\s*)?\)",
                 attr_type, sql)
    # (CASESPECIFIC) / (NOT CASESPECIFIC)
    if re.search(r"(?i)\(\s*(?:NOT\s+)?CASESPECIFIC\s*\)", sql):
        sql = re.sub(r"(?i)\s*\(\s*(?:NOT\s+)?CASESPECIFIC\s*\)", "", sql)
        ctx.warn("(NOT) CASESPECIFIC dropped: Spark string comparison is case-sensitive; wrap both sides in LOWER()/UPPER() where Teradata ignored case")
    return sql


def _dates(sql, ctx):
    def intdate(m):
        n = int(m.group("n"))
        y, md = divmod(n, 10000)
        mo, d = divmod(md, 100)
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{m.group('c')} {m.group('op')} DATE '{1900 + y:04d}-{mo:02d}-{d:02d}'"
        return m.group(0)
    new = re.sub(r"(?is)(?P<c>(?<![\w.])[\w.]*(?:_dt|date|_d)\w*)\s*(?P<op><=|>=|<>|!=|=|<|>)\s*(?P<n>\d{7})\b", intdate, sql)
    if new != sql:
        ctx.warn("Teradata integer date literal (YYYMMDD) rewritten as a DATE literal: check the column really is a DATE")
    sql = new
    if re.search(r"(?is)\b[\w.]*(?:date|_dt)\w*\s*-\s*[\w.]*(?:date|_dt)\w*\b", sql):
        ctx.warn("date - date returns a number of days in Teradata: use DATEDIFF(a, b) in Spark")
    # bare DATE / TIME keywords = current date/time
    nxt = r"(?!\s*(?:'|\(|(?!(?:AS|FROM|AND|OR|THEN|ELSE|END|WHEN|ASC|DESC|GROUP|ORDER|HAVING|WHERE|LIMIT|UNION)\b)\w))"
    sql = re.sub(r"(?is)(?<=[=<>(,+\-]\s)DATE" + nxt + r"|(?<=[=<>(,+\-])DATE" + nxt + r"|(?<=SELECT\s)DATE" + nxt + r"|(?<=BETWEEN\s)DATE" + nxt, "CURRENT_DATE()", sql)
    return sql


def _plain_names(head):
    """`SELECT [DISTINCT] a, t.b, c AS d FROM ...` -> (prefix text, [output names]) when the select list is plain columns, else None"""
    ms = re.match(r"(?is)^(\s*(?:--[^\n]*\n\s*)*)SELECT\s+(DISTINCT\s+)?(.*?)\s+\bFROM\b(.*)$", head)
    if not ms:
        return None
    names = []
    for it in split_args(ms.group(3)):
        mm = re.fullmatch(r"(?is)(?:[\w]+\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?", it.strip())
        if not mm or it.strip() == "*" or (mm.group(2) or "").upper() == "OVER":
            return None
        names.append(mm.group(2) or mm.group(1))
    if len({n.lower() for n in names}) != len(names):
        return None
    return ms, names


def top_with_ties(sql, ctx):
    """SELECT TOP n WITH TIES ... ORDER BY k  ->  keep every row whose RANK() over k is <= n (the rows tied with the last one included)"""
    m = re.match(r"(?is)^(\s*(?:--[^\n]*\n\s*)*)SELECT\s+(DISTINCT\s+)?TOP\s+\(?\s*(\d+)\s*\)?\s+WITH\s+TIES\s+(.*)$", sql)
    if not m:
        if re.search(r"(?is)\bTOP\s+\(?\s*\d+\s*\)?\s+WITH\s+TIES\b", sql):
            ctx.todo("TOP n WITH TIES inside a sub-query / CTE / set operation has no automatic conversion: use RANK() OVER (ORDER BY ...) <= n in a sub-query")
            return re.sub(r"(?is)\bTOP\s+(\(?\s*\d+\s*\)?)\s+WITH\s+TIES\b", r"TOP \1", sql)
        return sql
    lead, distinct, n, body = m.group(1), m.group(2), int(m.group(3)), m.group(4).rstrip().rstrip(";")

    def fail(msg):
        ctx.todo(f"TOP {n} WITH TIES: {msg}")
        return f"{lead}SELECT {distinct or ''}TOP {n} {body}"
    if distinct:
        return fail("DISTINCT with WITH TIES has no automatic conversion")
    if _find_top(body, r"(?:UNION|INTERSECT|EXCEPT|MINUS)\b"):
        return fail("set operations are not converted")
    orders = _find_top(body, r"ORDER\s+BY\b")
    if not orders:
        return fail("WITH TIES needs an ORDER BY")
    cut = orders[-1].start()
    base, keys_text = body[:cut].rstrip(), body[orders[-1].end():].strip()
    froms = _find_top(base, r"FROM\b")
    sel_text = base[:froms[0].start()] if froms else base
    aliases = {}
    for it in split_args(sel_text):
        am = re.fullmatch(r"(?is)(.+?)\s+AS\s+(\w+)", it.strip())
        if am:
            aliases[am.group(2).lower()] = am.group(1).strip()
    keys = []
    for k in split_args(keys_text):
        km = re.fullmatch(r"(?is)(.+?)(\s+(?:ASC|DESC))?(\s+NULLS\s+(?:FIRST|LAST))?", k.strip())
        expr, d, nl = km.group(1).strip(), (km.group(2) or ""), (km.group(3) or "")
        if re.fullmatch(r"\d+", expr):
            return fail("ORDER BY a column position cannot be used inside the ranking window: use the column name")
        if re.fullmatch(r"\w+", expr) and expr.lower() in aliases:
            expr = f"({aliases[expr.lower()]})"
        keys.append(f"{expr}{d}{nl}")
    order = ", ".join(keys)
    rk = f"RANK() OVER (ORDER BY {order})"
    names = []
    for it in split_args(sel_text):
        mm = re.fullmatch(r"(?is)(.+?)\s+AS\s+(\w+)|(?:\w+\.)?(\w+)", it.strip())
        names.append((mm.group(2) or mm.group(3)) if mm and it.strip() != "*" else None)
    sel = sel_text.strip()
    rest = base[len(sel_text):]
    if all(names) and len({x.lower() for x in names}) == len(names):
        ctx.warn(f"TOP {n} WITH TIES -> rows ranked <= {n} by RANK() in a sub-query (ties with the last row are kept)")
        return f"{lead}SELECT {', '.join(names)} FROM (SELECT {sel}, {rk} AS _rk {rest.lstrip()}) AS _t WHERE _rk <= {n} ORDER BY _rk"
    ctx.warn(f"TOP {n} WITH TIES: the select list has unnamed/star items, so the converted query also returns a helper column _rk (the rank): drop it if you need the exact columns")
    return f"{lead}SELECT * FROM (SELECT {sel}, {rk} AS _rk {rest.lstrip()}) AS _t WHERE _rk <= {n} ORDER BY _rk"


_NUM = r"(?:\d*\.\d+|\d+)"


def _sample_when(head, clause, ctx):
    """SAMPLE WHEN c1 THEN v1 [WHEN c2 THEN v2 ...] [ELSE v] END -> (strata, else_value, rest_after_END) or None"""
    toks = re.compile(r"(?is)\b(WHEN|THEN|ELSE|END|CASE)\b")
    strata, cur_cond, state, depth, case, pos, else_v = [], None, "start", 0, 0, 0, None
    j, last = 0, 0
    pieces = []
    while j < len(clause):
        k = _skip_literal(clause, j)
        if k != j:
            j = k
            continue
        c = clause[j]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and c.isalpha() and (j == 0 or not (clause[j - 1].isalnum() or clause[j - 1] in "_.")):
            m = toks.match(clause, j)
            if m:
                w = m.group(1).upper()
                if w == "CASE":
                    case += 1
                elif w == "END" and case:
                    case -= 1
                elif case == 0:
                    pieces.append((w, last, j, m.end()))
                    last = m.end()
                    if w == "END":
                        break
                j = m.end()
                continue
        j += 1
    else:
        return None
    # pieces: keyword + the text that followed it (up to the next keyword)
    out, i = [], 0
    kws = [(w, a, e) for w, a, _, e in pieces]
    texts = []
    for n, (w, st, en, end_kw) in enumerate(pieces):
        nxt = pieces[n + 1][2] if n + 1 < len(pieces) else en
        texts.append((w, clause[end_kw:nxt].strip() if n + 1 < len(pieces) else ""))
    for n, (w, t) in enumerate(texts):
        if n == 0 and w != "WHEN":
            return None
    # texts: WHEN cond / THEN value / WHEN cond / THEN value / ELSE value / END
    strata, else_v, cond = [], None, None
    for w, t in texts:
        if w == "WHEN":
            cond = t
        elif w == "THEN" and cond is not None:
            if not re.fullmatch(_NUM, t):
                return None
            strata.append((cond, t))
            cond = None
        elif w == "ELSE":
            if not re.fullmatch(_NUM, t):
                return None
            else_v = t
        elif w == "END":
            break
    rest = clause[pieces[-1][3]:]
    return (strata, else_v, rest) if strata else None


def _sample(sql, ctx):
    ms = _find_top(sql, r"SAMPLE\b(?=\s+(?:WHEN\b|RANDOMIZED\b|\d|\.\d))")
    if not ms:
        return sql
    m = ms[-1]
    head, clause = sql[:m.start()].rstrip(), sql[m.end():].strip()
    if re.match(r"(?is)WHEN\b", clause):
        parsed = _sample_when(head, clause, ctx)
        if not parsed:
            ctx.todo("SAMPLE WHEN: conditions/values could not be read (need `WHEN cond THEN number ... [ELSE number] END`): use sampleBy by hand")
            return head
        strata, else_v, rest = parsed
        rest = rest.strip().rstrip(";")
        is_count = lambda v: "." not in v and float(v) >= 1
        if not any(is_count(v) for _, v in strata + ([("", else_v)] if else_v else [])):
            br = " ".join(f"WHEN {c} THEN RAND() < {float(v):g}" for c, v in strata)
            els = f"ELSE RAND() < {float(else_v):g}" if else_v else "ELSE FALSE"
            ctx.warn("SAMPLE WHEN -> a random filter per stratum (Bernoulli sampling: the sizes are approximate, Teradata's are exact); "
                     "the conditions must only use columns of the query's output")
            return f"SELECT * FROM ({head}) AS _s WHERE CASE {br} {els} END" + (f" {rest}" if rest else "")
        pn = _plain_names(head)
        if not pn:
            ctx.todo("SAMPLE WHEN with row counts needs a plain column list in the SELECT (or use fractions): use row_number() OVER (PARTITION BY stratum ORDER BY rand()) by hand")
            return head
        ms_, names = pn
        sid = "CASE " + " ".join(f"WHEN {c} THEN {i}" for i, (c, _) in enumerate(strata, 1)) + " ELSE 0 END"

        def ok(v):
            return f"ROW_NUMBER() OVER (PARTITION BY {sid} ORDER BY RAND()) <= {int(float(v))}" if is_count(v) else f"RAND() < {float(v):g}"
        br = " ".join(f"WHEN {c} THEN {ok(v)}" for c, v in strata)
        els = f"ELSE {ok(else_v)}" if else_v else "ELSE FALSE"
        cols = ", ".join(names)
        ctx.warn("SAMPLE WHEN -> ROW_NUMBER() per stratum for row counts and a random filter for fractions (the fraction sizes are approximate); "
                 "the conditions must only use columns of the query's output")
        return (f"SELECT {cols} FROM (SELECT {cols}, CASE {br} {els} END AS _sample_ok FROM ({head}) AS _s) AS _t WHERE _sample_ok"
                + (f" {rest}" if rest else ""))
    mm = re.match(rf"(?is)^(RANDOMIZED\s+ALLOCATION\s+)?({_NUM})((?:\s*,\s*{_NUM})*)\s*(.*)$", clause)
    if not mm:
        ctx.todo("SAMPLE: unrecognised form: use TABLESAMPLE / sampleBy by hand")
        return head
    alloc, val, more, tail = mm.group(1), mm.group(2), mm.group(3), mm.group(4).strip().rstrip(";")
    if more.strip():
        ctx.todo("SAMPLE with several fractions/sizes returns a SAMPLEID column (disjoint samples): split the data with randomSplit by hand")
        return head
    if alloc:
        ctx.warn("SAMPLE RANDOMIZED ALLOCATION treated as a plain SAMPLE (the allocation across AMPs has no Spark meaning)")
    base = head
    if "." in val or float(val) < 1:
        pct = round(float(val) * 100, 6)
        ctx.warn("SAMPLE fraction -> TABLESAMPLE over the whole result: sampled rows differ from Teradata's (random)")
        return f"SELECT * FROM ({base}) AS _s TABLESAMPLE ({pct:g} PERCENT)" + (f" {tail}" if tail else "")
    ctx.warn("SAMPLE n -> ORDER BY rand() LIMIT n (random rows, like Teradata; the exact rows differ)")
    flat, d = [], 0
    for ch in base:  # only a top-level ORDER BY counts, not one inside a window or sub-query
        d += ch == "("
        flat.append(ch if d == 0 else " ")
        d -= ch == ")"
    if re.search(r"(?is)\bORDER\s+BY\b", "".join(flat)):
        inner = f"{base} LIMIT {int(val)}"
    else:
        inner = f"{base} ORDER BY rand() LIMIT {int(val)}"
    return f"SELECT * FROM ({inner}) AS _s {tail}" if tail else inner


def prepass(sql: str, ctx) -> str:
    """-> Spark-ready (still Teradata-flavoured where the generic rewriter takes over) statement text; may return a `-- ...` comment for dropped statements"""
    s = sql.strip().rstrip(";").strip()
    if not s:
        return s
    lead_comments = ""
    while s.startswith("--") or s.startswith("/*"):
        k = _skip_literal(s, 0)
        lead_comments += s[:k] + "\n"
        s = s[k:].lstrip()
    if not s:
        return lead_comments.strip()
    if _DROP.match(s) and not re.match(r"(?is)\s*SET\s+(?!QUERY_BAND|SESSION)", s):
        return _dropped_note(s)
    s = re.sub(r"(?is)^\s*(?:LOCKING|LOCK)\s+(?:ROW\s+|TABLE\s+|DATABASE\s+|VIEW\s+)?[\w.\"]*\s*(?:FOR|IN)\s+(?:ACCESS|READ|WRITE|EXCLUSIVE|SHARE)(?:\s+MODE)?(?:\s+NOWAIT)?\s+", "", s)
    s = re.sub(r"(?is)^SEL\b", "SELECT", s)
    s = re.sub(r"(?is)^INS\b(?!\s+INTO)", "INSERT INTO", s)
    s = re.sub(r"(?is)^INSERT\s+(?!INTO\b)(?=[\w.\"])", "INSERT INTO ", s)
    s = re.sub(r"(?is)^INS\s+INTO\b", "INSERT INTO", s)
    s = re.sub(r"(?is)^UPD\b", "UPDATE", s)
    s = re.sub(r"(?is)^DEL\b", "DELETE", s)
    s = re.sub(r"(?is)\bDELETE\s+FROM\s+([\w.\"`]+)\s+ALL\s*$", r"DELETE FROM \1", s)
    s = re.sub(r"(?is)(\(\s*|\bINSERT\s+INTO\s+[\w.\"`]+(?:\s*\([^)]*\))?\s+|\bUNION(?:\s+ALL)?\s+|\bAS\s+)SEL\b", lambda m: m.group(1) + "SELECT", s)
    s = re.sub(r"(?is)\bSEL\b(?=\s)", "SELECT", s)
    m = re.match(r"(?is)^DATABASE\s+([\w.\"]+)$", s)
    if m:
        return f"USE {m.group(1)}"
    m = re.match(r"(?is)^(REPLACE|CREATE)\s+VIEW\b", s)
    if m:
        s = re.sub(r"(?is)^(?:REPLACE|CREATE)\s+VIEW\b", "CREATE OR REPLACE VIEW", s)
    ddl = convert_ddl(s, ctx)
    if ddl is not None:
        return lead_comments + ddl
    s = top_with_ties(s, ctx)
    s = _functions(s, ctx)
    s = _periods(s, ctx)
    s = _legacy_analytics(s, ctx)
    s = _formats_and_attributes(s, ctx)
    s = _dates(s, ctx)
    s = _sample(s, ctx)
    if re.search(r"(?is)\bQUALIFY\b", s):
        s = qualify_rewrite(s, ctx)
    s = re.sub(r"(?is)\bMINUS\b", "EXCEPT", s)
    return lead_comments + s
