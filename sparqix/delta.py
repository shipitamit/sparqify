"""Delta Lake / Unity Catalog output pass (`--delta`, `--catalog C [--schema S]`).

 * writes: parquet/orc and saveAsTable -> Delta; reads of a path written as Delta use format('delta')
 * names:  one/two-part table names -> catalog.schema.table (spark.table, saveAsTable, insertInto and inside SQL;
           temp views, CTEs and 3-part names are left alone)
 * DDL:    STORED AS PARQUET/ORC/AVRO, USING parquet/orc -> USING DELTA
 * upserts: INSERT ... ON CONFLICT (keys) DO UPDATE/NOTHING -> MERGE INTO (always, since Spark SQL has no ON CONFLICT)
Every rewrite is reported; nothing here was run against a real workspace."""
from __future__ import annotations

import ast
import re

from .core import Report
from .optimize import _offsets, _span

IDENT = r"(?:`[^`]+`|[A-Za-z_][\w$]*)"
NAME = rf"{IDENT}(?:\s*\.\s*{IDENT}){{0,2}}"
_RESERVED = {"WHERE", "JOIN", "ON", "LEFT", "RIGHT", "INNER", "FULL", "CROSS", "GROUP", "ORDER", "LIMIT", "HAVING", "UNION", "USING", "LATERAL", "WINDOW", "SELECT", "NATURAL", "OUTER"}
_NOT_TABLE = {"SET", "SELECT", "VALUES", "LATERAL", "UNNEST", "TABLE", "WITH", "VIEW", "IF", "OVERWRITE", "TEMP", "TEMPORARY", "EXISTS", "NOT", "DELTA", "DUAL"}


# ------------------------------------------------------------------ SQL text
def _split_statements(text: str):
    out, cur, q, i = [], [], None, 0
    while i < len(text):
        ch = text[i]
        if q:
            cur.append(ch)
            if ch == q:
                q = None
        elif ch in "'\"`":
            q = ch; cur.append(ch)
        elif ch == ";":
            out.append("".join(cur) + ";"); cur = []
        else:
            cur.append(ch)
        i += 1
    if "".join(cur).strip():
        out.append("".join(cur))
    return out


def _parts(name: str):
    return [p.strip() for p in re.findall(IDENT, name)]


def _qualify(name: str, catalog: str, schema: str):
    ps = _parts(name)
    if len(ps) == 3:
        return name
    if len(ps) == 2:
        return f"{catalog}.{name.replace(' ', '')}"
    return f"{catalog}.{schema}.{name}"


def _in_special_from(text: str, pos: int) -> bool:
    """True when the FROM at `pos` belongs to EXTRACT(.. FROM ..) / TRIM(.. FROM ..) / SUBSTRING / IS DISTINCT FROM"""
    if re.search(r"\bDISTINCT\s+$", text[:pos], re.I):
        return True
    depth, i = 0, pos - 1
    while i >= 0:
        c = text[i]
        if c == ")":
            depth += 1
        elif c == "(":
            if depth == 0:
                m = re.search(r"(\w+)\s*$", text[:i])
                return bool(m and m.group(1).upper() in ("EXTRACT", "TRIM", "SUBSTRING", "OVERLAY", "POSITION"))
            depth -= 1
        i -= 1
    return False


def qualify_sql(text: str, catalog: str, schema: str, skip: set, report: Report) -> str:
    ctes = {m.group(1).lower() for m in re.finditer(rf"(?:\bWITH\s+(?:RECURSIVE\s+)?|,\s*)({IDENT})\s+AS\s*\(", text, re.I)}
    skip = {s.lower() for s in skip} | ctes
    kw = re.compile(rf"\b(FROM|JOIN|INTO|UPDATE|TABLE|USING)\b(\s+IF\s+(?:NOT\s+)?EXISTS)?(\s+)({NAME})(?![\w$.])", re.I)
    out, last = [], 0
    comma_re = re.compile(rf"(?:\s+(?:AS\s+)?({IDENT}))?\s*,\s*({NAME})(?![\w$.])(?!\s*\()", re.I)
    def fix(name, pos_kw, start):
        ps = _parts(name)
        bare = name.replace("`", "").replace(" ", "").lower()
        if (ps[0].upper() in _NOT_TABLE and len(ps) == 1) or bare in skip or "$" in name or "{" in name:
            return name
        new = _qualify(name, catalog, schema)
        if new != name:
            report.note(f"table {name.strip()} -> {new}")
        return new
    for m in kw.finditer(text):
        word = m.group(1).upper()
        if word == "FROM" and _in_special_from(text, m.start()):
            continue
        s, e = m.start(4), m.end(4)
        if s < last:
            continue
        if word in ("FROM", "JOIN", "USING", "UPDATE") and re.match(r"\s*\(", text[e:]):
            continue                                          # table function / subquery call
        out.append(text[last:s]); out.append(fix(m.group(4), word, s)); last = e
        if word == "FROM":                                   # comma-joined tables: FROM a x, b y
            pos = e
            while True:
                mm = comma_re.match(text, pos)
                if not mm or (mm.group(1) and mm.group(1).upper() in _RESERVED):
                    break
                out.append(text[last:mm.start(2)]); out.append(fix(mm.group(2), "FROM", mm.start(2))); last = pos = mm.end(2)
    out.append(text[last:])
    return "".join(out)


def ddl_to_delta(text: str, report: Report) -> str:
    def repl(m):
        report.note(f"DDL storage format {m.group(0).strip()} -> USING DELTA")
        return " USING DELTA"
    new = re.sub(r"\s+STORED\s+AS\s+(?:PARQUET|ORC|AVRO|TEXTFILE|RCFILE|SEQUENCEFILE)\b", repl, text, flags=re.I)
    new = re.sub(r"\s+USING\s+(?:parquet|orc|avro|csv|json)\b(?=[^;]*)", lambda m: repl(m) if re.match(r"\s*CREATE\b", new, re.I) else m.group(0), new, flags=re.I)
    if re.match(r"\s*CREATE\s+EXTERNAL\s+TABLE", new, re.I):
        report.warn("ddl", "CREATE EXTERNAL TABLE kept: on Unity Catalog external tables need an external location; Delta managed tables are usually simpler")
    return new


def upsert_to_merge(stmt: str, report: Report) -> str:
    m = re.match(rf"(?is)^(\s*)INSERT\s+INTO\s+({NAME})\s*\(([^)]*)\)\s*(.*?)\s+ON\s+CONFLICT\s*\(([^)]*)\)\s*(DO\s+NOTHING|DO\s+UPDATE\s+SET\s+(.*?))\s*(;?)\s*$", stmt)
    if not m:
        if re.search(r"(?i)ON\s+DUPLICATE\s+KEY\s+UPDATE", stmt):
            report.todo("upsert", "MySQL ON DUPLICATE KEY UPDATE needs the unique key columns: rewrite as MERGE INTO ... ON <keys>")
        elif re.search(r"(?i)\bON\s+CONFLICT\b", stmt):
            report.todo("upsert", "ON CONFLICT form not recognised (needs INSERT INTO t (cols) ... ON CONFLICT (keys) DO ...): rewrite as MERGE INTO")
        return stmt
    ind, table, cols, src, keys, action, sets, semi = m.groups()
    cols = [c.strip() for c in cols.split(",")]
    keys = [k.strip() for k in keys.split(",")]
    src = src.strip()
    if re.match(r"(?i)^VALUES\b", src):
        using = f"(SELECT * FROM ({src}) AS v({', '.join(cols)}))"
    elif re.match(r"(?i)^\(?\s*SELECT\b", src):
        using = f"({src.strip()})" if not src.startswith("(") else src
    else:
        report.todo("upsert", "ON CONFLICT source is neither SELECT nor VALUES")
        return stmt
    on = " AND ".join(f"tgt.{k} = src.{k}" for k in keys)
    out = f"{ind}MERGE INTO {table} AS tgt\nUSING {using} AS src({', '.join(cols)})\nON {on}"
    if action.upper().startswith("DO UPDATE"):
        s = re.sub(r"(?i)\bEXCLUDED\.", "src.", sets)
        tname = _parts(table)[-1]
        s = re.sub(rf"(?i)\b{re.escape(tname)}\.", "tgt.", s)
        out += f"\nWHEN MATCHED THEN UPDATE SET {s}"
    out += f"\nWHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('src.' + c for c in cols)}){semi}"
    report.note(f"INSERT ... ON CONFLICT on {table} rewritten as MERGE INTO (Delta/Iceberg tables only)")
    return out


def process_sql_text(text: str, report: Report, *, delta: bool, catalog: str | None, schema: str, skip: set) -> str:
    out = []
    for stmt in _split_statements(text):
        stmt = upsert_to_merge(stmt, report)
        if delta:
            stmt = ddl_to_delta(stmt, report)
        if catalog:
            stmt = qualify_sql(stmt, catalog, schema, skip, report)
        out.append(stmt)
    return "".join(out)


# ------------------------------------------------------------------ Python code
def _chain_attrs(n):
    names = []
    while isinstance(n, ast.Call) or isinstance(n, ast.Attribute):
        if isinstance(n, ast.Call):
            n = n.func
        else:
            names.append(n.attr); n = n.value
    return names, n


def process_python(code: str, report: Report, *, delta: bool, catalog: str | None, schema: str) -> str:
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return code
    if not code.isascii():
        report.note("delta pass skipped: non-ASCII source")
        return code
    starts = _offsets(code)
    edits = []
    skip = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("createOrReplaceTempView", "createTempView", "registerTempTable", "createGlobalTempView") \
                and n.args and isinstance(n.args[0], ast.Constant):
            skip.add(str(n.args[0].value))
    for m in re.finditer(r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(" + IDENT + ")", code):
        skip.add(m.group(1).strip("`"))

    delta_paths = set()
    # --- writes
    for n in ast.walk(tree):
        if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)):
            continue
        attr, (chain, _) = n.func.attr, _chain_attrs(n.func.value)
        is_write = "write" in chain or "writeTo" in chain
        if delta and is_write and attr in ("parquet", "orc") and len(n.args) == 1 and not n.keywords:
            s, e = _span(starts, n)
            base = ast.get_source_segment(code, n.func.value)
            arg = ast.get_source_segment(code, n.args[0])
            edits.append((s, e, f"{base}.format('delta').save({arg})"))
            if isinstance(n.args[0], ast.Constant):
                delta_paths.add(n.args[0].value)
            else:
                report.warn(f"line {n.lineno}", "Delta write to a non-literal path: reads of that path must use format('delta').load(...) too")
            report.note(f"line {n.lineno}: .{attr}(path) -> Delta (format('delta').save)")
        elif delta and is_write and attr in ("parquet", "orc"):
            report.warn(f"line {n.lineno}", f".{attr}() with extra options kept as {attr}: convert to Delta manually")
        elif delta and is_write and attr in ("csv", "json", "text"):
            report.note(f"line {n.lineno}: {attr} output kept (interchange format, not converted to Delta)")
        elif delta and is_write and attr == "saveAsTable" and "format" not in chain:
            s = n.func.end_col_offset
            pos = starts[n.func.end_lineno - 1] + n.func.end_col_offset - len(".saveAsTable")
            edits.append((pos, pos, ".format('delta')"))
            report.note(f"line {n.lineno}: saveAsTable -> Delta table")
        elif delta and is_write and attr == "format" and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value in ("parquet", "orc"):
            s, e = _span(starts, n.args[0])
            edits.append((s, e, "'delta'"))
            report.note(f"line {n.lineno}: format('{n.args[0].value}') -> format('delta')")
    # --- reads of paths written as Delta
    for n in ast.walk(tree):
        if delta and isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("parquet", "orc") \
                and "read" in _chain_attrs(n.func.value)[0] and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value in delta_paths:
            s, e = _span(starts, n)
            base = ast.get_source_segment(code, n.func.value)
            edits.append((s, e, f"{base}.format('delta').load({ast.get_source_segment(code, n.args[0])})"))
            report.note(f"line {n.lineno}: read of Delta path {n.args[0].value!r} uses format('delta')")
    # --- names and embedded SQL
    for n in ast.walk(tree):
        if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.args):
            continue
        a0 = n.args[0]
        if n.func.attr in ("table", "saveAsTable", "insertInto", "forName") and catalog:
            tgt = n.args[1] if n.func.attr == "forName" and len(n.args) > 1 else a0
            if isinstance(tgt, ast.Constant) and isinstance(tgt.value, str):
                nm = tgt.value
                if nm not in skip and "." not in nm.replace(f"{catalog}.", "", 1) or (nm not in skip and len(nm.split(".")) < 3):
                    if len(nm.split(".")) < 3 and nm not in skip:
                        new = _qualify(nm, catalog, schema)
                        s, e = _span(starts, tgt)
                        edits.append((s, e, repr(new)))
                        report.note(f"line {n.lineno}: table {nm} -> {new}")
        if n.func.attr == "sql":
            if isinstance(a0, ast.Constant) and isinstance(a0.value, str):
                s, e = _span(starts, a0)
                seg = code[s:e]
                q = re.match(r'^[rRbBuU]*("""|\'\'\'|"|\')', seg)
                if q:
                    pre, ql = q.group(0), len(q.group(1))
                    body = seg[len(pre):len(seg) - ql]
                    new_body = process_sql_text(body, report, delta=delta, catalog=catalog, schema=schema, skip=skip)
                    if ql == 1:
                        new_body = new_body.replace("\n", " ")           # single-quoted literal: no newlines
                    if new_body != body:
                        edits.append((s, e, pre + new_body + seg[len(seg) - ql:]))
            elif isinstance(a0, ast.JoinedStr):
                report.warn(f"line {n.lineno}", "f-string SQL not rewritten (table names/ON CONFLICT): review manually")
    if delta and any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("save", "saveAsTable") for n in ast.walk(tree)) and edits:
        report.note("Delta tables benefit from periodic OPTIMIZE (and VACUUM): schedule them for large or frequently written tables")
    out, lastpos = code, len(code) + 1
    for s, e, text in sorted(set(edits), key=lambda x: (x[0], x[1]), reverse=True):
        if e > lastpos:
            continue
        out, lastpos = out[:s] + text + out[e:], s
    try:
        ast.parse(out)
    except SyntaxError:
        report.warn("delta", "Delta pass produced invalid Python; returning the original conversion")
        return code
    return out
