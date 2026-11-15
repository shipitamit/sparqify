"""Teradata load utilities -> PySpark: FastLoad, MultiLoad and TPT (Teradata Parallel Transporter) job scripts.

All three describe 'read a file with a layout, apply INSERT/UPDATE/DELETE to a table'. We extract file name, delimiter / fixed layout, columns and the DML,
and emit a function that reads the file with Spark and appends / exports. Logon details (.LOGON, TdpId, UserName, UserPassword) are never emitted.
Every value that came from the script reaches the output only through repr() or as a validated identifier.
"""
from __future__ import annotations

import re

from .core import Report, cm
from .teradata_sql import _TYPE_MAP, split_args

_IDENT = re.compile(r"[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){0,2}")
_COL = re.compile(r"[A-Za-z_]\w*")


def _ident(x, rep, what):
    x = (x or "").strip().strip('"')
    if _IDENT.fullmatch(x):
        return x
    rep.todo("tdload", f"{what} name {cm(x)[:40]!r} is not a plain identifier: fill it in by hand")
    return "TODO_" + what


def _strip(src):
    src = re.sub(r"(?s)/\*.*?\*/", " ", src)
    return re.sub(r"(?m)^[ \t]*--[^\n]*$", "", src)


def _spark_type(t):
    base = re.sub(r"\(.*", "", t.strip(), flags=re.S).strip().upper()
    if base in ("DECIMAL", "DEC", "NUMERIC"):
        m = re.search(r"\(([\d, ]+)\)", t)
        return "decimal(" + m.group(1).replace(" ", "") + ")" if m else "decimal(18,0)"
    if base in ("BIGINT", "SMALLINT", "DATE"):
        return base.lower()
    if base.startswith("TIMESTAMP"):
        return "timestamp"
    return _TYPE_MAP.get(base, "STRING").lower()


def _width(t):
    m = re.search(r"\((\d+)", t)
    return int(m.group(1)) if m else None


def _unq(s):
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"":
        return s[1:-1].replace("''", "'")
    return s


# ------------------------------------------------------------------------------------------------------------------ parsers
def parse_fastload(src, rep):
    loads, delim, cols, path = [], "|", [], None
    vartext = False
    for st in [s.strip() for s in _strip(src).split(";")]:
        if re.match(r"(?i)LOGON\b|LOGOFF\b|SLEEP\b|TENACITY\b|SESSIONS\b|ERRLIMIT\b|CLEAR\b|HELP\b|SHOW\b|QUIT\b|OS\b", st):
            if re.match(r"(?i)LOGON\b", st):
                rep.note("LOGON credentials are not copied into the output: configure the Spark/JDBC connection separately")
            continue
        m = re.match(r"(?is)SET\s+RECORD\s+VARTEXT(?:\s+(\"[^\"]*\"|'[^']*'))?", st)
        if m:
            vartext = True
            delim = _unq(m.group(1)) if m.group(1) else "|"
            continue
        m = re.match(r"(?is)DEFINE\s+(.*?)\s+FILE\s*=\s*(\S+)$", st)
        if m:
            path = _unq(m.group(2))
            cols = []
            for part in split_args(m.group(1)):
                cm_ = re.fullmatch(r"(?is)(\w+)\s*\((.+)\)", part.strip())
                if cm_:
                    cols.append((cm_.group(1), cm_.group(2).strip()))
            continue
        m = re.match(r"(?is)BEGIN\s+LOADING\s+([\w.\"]+)", st)
        if m:
            loads.append({"target": m.group(1), "cols": cols, "path": path, "delim": delim if vartext else None, "tool": "FastLoad"})
            continue
        m = re.match(r"(?is)INSERT\s+(?:INTO\s+)?([\w.\"]+)\s*(\(.*?\))?\s*VALUES\s*\((.*)\)$", st)
        if m and loads:
            names = [a.strip().lstrip(":") for a in split_args(m.group(3))]
            tcols = [c.strip() for c in split_args(m.group(2)[1:-1])] if m.group(2) else None
            loads[-1].update(values=names, tcols=tcols)
            continue
        if re.match(r"(?i)RECORD\s+\d", st):
            rep.note("FastLoad RECORD n,m (start/end record) not applied: filter rows by hand if needed")
    return loads


def parse_multiload(src, rep):
    text = _strip(src)
    layouts, labels, imports, cur, lay, target = {}, {}, [], None, None, None
    stmts = [s.strip() for s in re.split(r";[ \t]*(?:\n|$)", text) if s.strip()]
    for st in stmts:
        st1 = re.sub(r"^\.", "", st)
        if re.match(r"(?i)LOGON\b|LOGOFF\b|LOGTABLE\b|ROUTE\b|SET\b|DATABASE\b|END\s+MLOAD\b", st1):
            if re.match(r"(?i)LOGON\b", st1):
                rep.note("LOGON credentials are not copied into the output: configure the Spark/JDBC connection separately")
            continue
        m = re.match(r"(?is)BEGIN\s+(?:IMPORT\s+)?MLOAD\s+TABLES?\s+([\w.\",\s]+?)(?:\s+WORKTABLES|\s+ERRORTABLES|\s+SESSIONS|$)", st1)
        if m:
            target = re.split(r"\s*,\s*", m.group(1).strip())[0]
            continue
        m = re.match(r"(?is)LAYOUT\s+(\w+)", st1)
        if m:
            lay = layouts.setdefault(m.group(1), [])
            continue
        m = re.match(r"(?is)FIELD\s+(\w+)\s+(\*|\d+)\s+(.+)$", st1)
        if m and lay is not None:
            lay.append((m.group(1), m.group(3).strip(), None if m.group(2) == "*" else int(m.group(2))))
            continue
        m = re.match(r"(?is)FILLER\s+(\w+)\s+(\*|\d+)\s+(.+)$", st1)
        if m and lay is not None:
            lay.append(("_filler_" + m.group(1), m.group(3).strip(), None if m.group(2) == "*" else int(m.group(2))))
            continue
        m = re.match(r"(?is)DML\s+LABEL\s+(\w+)(.*)$", st1)
        if m:
            cur = m.group(1)
            labels[cur] = {"opts": " ".join(m.group(2).split()), "sql": []}
            continue
        if re.match(r"(?is)(INSERT|UPDATE|DELETE)\b", st1) and cur:
            labels[cur]["sql"].append(" ".join(st1.split()))
            continue
        m = re.match(r"(?is)IMPORT\s+INFILE\s+(\S+)(.*)$", st1)
        if m:
            rest = m.group(2)
            fm = re.search(r"(?is)FORMAT\s+(VARTEXT)(?:\s+(\"[^\"]*\"|'[^']*'))?", rest)
            lm = re.search(r"(?is)LAYOUT\s+(\w+)", rest)
            am = re.search(r"(?is)APPLY\s+(\w+(?:\s*,\s*\w+)*)", rest)
            if re.search(r"(?is)APPLY\s+\w+\s+WHERE\b", rest):
                rep.todo("tdload", "MultiLoad conditional APPLY ... WHERE: add the row filter by hand")
            imports.append({"path": _unq(m.group(1)), "vartext": bool(fm), "delim": (_unq(fm.group(2)) if fm and fm.group(2) else "|"),
                            "layout": lm.group(1) if lm else None, "apply": am.group(1) if am else None})
    loads = []
    for im in imports:
        names = re.split(r"\s*,\s*", im["apply"] or "")
        lab = {"sql": [x for nm in names for x in labels.get(nm, {"sql": []})["sql"]], "opts": " ".join(labels.get(nm, {"opts": ""})["opts"] for nm in names)}
        loads.append({"target": target, "path": im["path"], "delim": im["delim"] if im["vartext"] else None, "tool": "MultiLoad",
                      "cols": [(n, t) for n, t, _ in layouts.get(im["layout"], [])], "widths": [w for _, _, w in layouts.get(im["layout"], [])],
                      "dml": lab["sql"], "opts": lab["opts"], "label": im["apply"]})
    return loads


def parse_tpt(src, rep):
    text = _strip(src)
    schemas = {}
    for m in re.finditer(r"(?is)DEFINE\s+SCHEMA\s+(\w+)\s*(?:DESCRIPTION\s+'[^']*')?\s*\((.*?)\)\s*;", text):
        cols = []
        for part in split_args(m.group(2)):
            pm = re.fullmatch(r"(?is)(\w+)\s+(.+)", part.strip())
            if pm:
                cols.append((pm.group(1), pm.group(2).strip()))
        schemas[m.group(1).upper()] = cols
    ops = {}
    for m in re.finditer(r"(?is)DEFINE\s+OPERATOR\s+(\w+)(.*?)(?=DEFINE\s+(?:OPERATOR|SCHEMA|JOB)\b|\bAPPLY\b|\Z)", text):
        body = m.group(2)
        tm = re.search(r"(?is)\bTYPE\s+(\w+)", body)
        sm = re.search(r"(?is)\bSCHEMA\s+(\w+|\*)", body)
        attrs = {}
        for a in re.finditer(r"(?is)\b(?:VARCHAR|INTEGER|INT)\s+(?:ARRAY\s+)?(\w+)\s*=\s*('(?:[^']|'')*'|\"[^\"]*\"|[\w.@]+)", body):
            attrs[a.group(1).lower()] = _unq(a.group(2))
        ops[m.group(1).upper()] = {"type": (tm.group(1).upper() if tm else ""), "schema": sm.group(1).upper() if sm else None, "attrs": attrs}
    loads = []
    for m in re.finditer(r"(?is)APPLY\s*(.*?)\s*TO\s+OPERATOR\s*\(\s*(\w+)[^)]*\)\s*(?:SELECT\s+.*?\s+FROM\s+OPERATOR\s*\(\s*(\w+)[^)]*\))?\s*;", text):
        dml, consumer, producer = m.group(1), m.group(2).upper(), (m.group(3) or "").upper()
        c, p = ops.get(consumer, {"type": "", "attrs": {}}), ops.get(producer, {"type": "", "attrs": {}, "schema": None})
        for k in list(c["attrs"]) + list(p["attrs"]):
            if k in ("username", "userpassword", "tdpid", "logonmech", "logondata", "password"):
                rep.note("TPT logon attributes are not copied into the output: configure the Spark/JDBC connection separately")
                break
        sqls = re.findall(r"'((?:[^']|'')*)'", dml)
        stmts = [x for q in sqls for x in _split_stmts(q.replace("''", "'"))]
        sql = stmts[0] if stmts else ""
        cols = schemas.get(p.get("schema") or c.get("schema") or "", [])
        entry = {"tool": "TPT", "cols": cols, "path": None, "delim": None, "dml": stmts, "target": None}
        if p["type"] == "DATACONNECTOR":  # file -> Teradata
            a = p["attrs"]
            entry["path"] = a.get("filename")
            fmt = (a.get("format") or "delimited").lower()
            entry["delim"] = (a.get("textdelimiter") or "|") if fmt == "delimited" else None
            entry["target"] = c["attrs"].get("targettable")
            if entry["target"] is None:
                im = re.match(r"(?is)(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|DELETE)\s+([\w.\"]+)", sql)
                entry["target"] = im.group(1) if im else None
            entry["header"] = (a.get("skiprows") or "0") not in ("0", "")
        elif c["type"] == "DATACONNECTOR" and p["type"] in ("EXPORT", "SELECTOR"):  # Teradata -> file
            entry.update(export=True, path=c["attrs"].get("filename"), delim=c["attrs"].get("textdelimiter") or "|",
                         select=p["attrs"].get("selectstmt"))
        else:
            rep.todo("tdload", f"TPT APPLY to operator {cm(consumer)[:30]}: unsupported operator combination, convert by hand")
            continue
        loads.append(entry)
    for jm in re.finditer(r"(?is)DEFINE\s+OPERATOR\s+\w+\s+TYPE\s+DDL.*?\bARRAY\s+\w+\s*=\s*\[(.*?)\]", text):
        for q in re.findall(r"'((?:[^']|'')*)'", jm.group(1)):
            loads.append({"tool": "TPT", "ddl": q.replace("''", "'")})
    if re.search(r"(?i)DEFINE\s+JOB[^;]*;?.*\bSTEP\b", text):
        rep.note("TPT job steps are emitted in script order")
    return loads


# ------------------------------------------------------------------------------------------------------------------ UPDATE / DELETE / upsert -> MERGE
_SQL_WORDS = {"AND", "OR", "NOT", "NULL", "IS", "IN", "BETWEEN", "LIKE", "CASE", "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE", "CURRENT_DATE",
              "CURRENT_TIMESTAMP", "DATE", "TIMESTAMP", "INTERVAL", "EXISTS", "ANY", "ALL", "DISTINCT", "AS", "CAST", "DAY", "MONTH", "YEAR", "SELECT", "FROM"}


def _split_stmts(text):
    """split on ; outside quotes"""
    out, cur, q = [], [], False
    for ch in text:
        if ch == "'":
            q = not q
        if ch == ";" and not q:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return [" ".join(x.split()) for x in out if x.strip()]


def _qualify(expr, fields, tnames, bad):
    """:field -> s.field ; target column -> t.column (the target's own table / alias prefix is dropped)"""
    parts = re.split(r"('(?:[^']|'')*')", expr)

    def sub(m):
        w = m.group(0)
        if w.startswith(":"):
            f = fields.get(w[1:].lower())
            if f is None:
                bad.append(w)
                return w
            return "s." + f
        segs = w.split(".")
        if len(segs) == 1 and segs[0].upper() in _SQL_WORDS:
            return w
        if len(segs) > 1 and ".".join(segs[:-1]).lower() not in tnames:
            bad.append(w)
            return w
        return "t." + segs[-1]
    return "".join(p if i % 2 else re.sub(r":\w+|\b[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\b(?![\s]*\()", sub, p) for i, p in enumerate(parts))


def _xsql(expr, rep):
    """Teradata expression -> Spark expression, through the SQL converter (falls back to the text itself)"""
    from .sql_conv import convert_sql
    from .core import Report
    sub = Report()
    try:
        out = convert_sql("SELECT " + expr, sub, dialect="teradata", out="sql").strip().rstrip(";").strip()
    except Exception:
        return expr
    if sub.todos or not out.upper().startswith("SELECT ") or "\n" in out:
        return expr
    for _, m in sub.warnings:
        rep.warn("tdload", m)
    return out[7:].strip()


def _parse_dml(st, rep):
    st = st.strip().rstrip(";").strip()
    m = re.match(r"(?is)^UPDATE\s+([\w.\"]+)(?:\s+(?:AS\s+)?(?!SET\b)(\w+))?\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$", st)
    if m:
        return {"op": "update", "table": m.group(1), "alias": m.group(2), "set": m.group(3), "where": m.group(4)}
    m = re.match(r"(?is)^DELETE\s+(?:FROM\s+)?([\w.\"]+)(?:\s+(?:AS\s+)?(?!WHERE\b)(\w+))?(?:\s+WHERE\s+(.*))?$", st)
    if m:
        return {"op": "delete", "table": m.group(1), "alias": m.group(2), "where": m.group(3)}
    m = re.match(r"(?is)^INSERT\s+(?:INTO\s+)?([\w.\"]+)\s*(?:\((.*?)\))?\s*VALUES\s*\((.*)\)$", st)
    if m:
        return {"op": "insert", "table": m.group(1), "cols": m.group(2), "values": m.group(3)}
    return None


def _merge_lines(dml, n, fields, rep, tool):
    """-> (lines, ok): Delta MERGE statements for the UPDATE / DELETE / UPDATE+INSERT label contents"""
    parsed = []
    for d in dml:
        p = _parse_dml(d, rep)
        if p is None:
            rep.todo("tdload", f"{tool}: statement not recognised for MERGE conversion: {cm(d)[:80]}")
            return [], False
        parsed.append(p)
    ups = [p for p in parsed if p["op"] == "update"]
    ins = [p for p in parsed if p["op"] == "insert"]
    groups = []
    if len(ups) == 1 and len(ins) == 1 and ups[0]["table"].lower() == ins[0]["table"].lower() and parsed.index(ups[0]) < parsed.index(ins[0]):
        groups.append((ups[0], ins[0]))                          # UPDATE ... ; INSERT ...  =  upsert
        groups += [(p, None) for p in parsed if p["op"] == "delete"]
    else:
        groups = [(p, None) for p in parsed]
    lines, k = [], 0
    for first, extra in groups:
        k += 1
        view = f"stage_{n}_{k}"
        tbl = _ident(first["table"], rep, "table")
        tnames = {tbl.lower(), tbl.split(".")[-1].lower()}
        if first.get("alias"):
            tnames.add(first["alias"].lower())
        bad = []
        if first["op"] == "insert":
            cols = [c.strip().strip('"') for c in split_args(first["cols"] or "")]
            vals = [_qualify(v.strip(), fields, tnames, bad) for v in split_args(first["values"])]
            if not cols or len(cols) != len(vals) or not all(_COL.fullmatch(c) for c in cols) or bad:
                rep.todo("tdload", f"{tool}: INSERT without a plain column list / with unknown fields ({cm(', '.join(bad))[:60]}): write it by hand")
                return [], False
            vals = [_xsql(v, rep) for v in vals]
            q = f"INSERT INTO {tbl} ({', '.join(cols)}) SELECT {', '.join(vals)} FROM {view} AS s"
            lines += [f"    df.createOrReplaceTempView({view!r})", f"    spark.sql({q!r})"]
            continue
        if not first.get("where"):
            rep.todo("tdload", f"{tool}: {first['op'].upper()} without WHERE has no key to MERGE on: write it by hand")
            return [], False
        on = _qualify(first["where"], fields, tnames, bad)
        clauses = []
        if first["op"] == "update":
            sets = []
            for a in split_args(first["set"]):
                am = re.fullmatch(r"(?is)\s*([A-Za-z_]\w*)\s*=\s*(.+)", a)
                if not am:
                    bad.append(a.strip()[:20])
                    continue
                sets.append(f"{am.group(1)} = {_qualify(am.group(2), fields, tnames, bad)}")
            clauses.append(("MATCHED", "UPDATE SET " + ", ".join(sets), sets))
        else:
            clauses.append(("MATCHED", "DELETE", ["x"]))
        if extra:
            cols = [c.strip().strip('"') for c in split_args(extra["cols"] or "")]
            vals = [_qualify(v.strip(), fields, tnames, bad) for v in split_args(extra["values"])]
            if not cols or len(cols) != len(vals) or not all(_COL.fullmatch(c) for c in cols):
                rep.todo("tdload", f"{tool}: the upsert INSERT needs a plain column list: write it by hand")
                return [], False
            clauses.append(("NOT MATCHED", f"INSERT ({', '.join(cols)}) VALUES ({', '.join(vals)})", cols))
        if bad or not all(c[2] for c in clauses):
            rep.todo("tdload", f"{tool}: could not map {cm(', '.join(bad))[:60] or 'the SET list'} to the file fields / target columns: write the MERGE by hand")
            return [], False
        on_x, parts = _xsql(on, rep), []
        for kind, body, _ in clauses:
            parts.append(f"WHEN {kind} THEN {_xsql_set(body, rep)}")
        merge = f"MERGE INTO {tbl} AS t USING {view} AS s ON {on_x} " + " ".join(parts)
        keys = []
        for conj in re.split(r"(?i)\s+AND\s+", on):
            km = re.fullmatch(r"\s*t\.(\w+)\s*=\s*s\.(\w+)\s*|\s*s\.(\w+)\s*=\s*t\.(\w+)\s*", conj)
            if not km:
                keys = None
                break
            keys.append(km.group(2) or km.group(3))
        lines.append("    src = df")
        if keys:
            lines += ["    from pyspark.sql import Window",
                      f"    src = (src.withColumn('_ord', F.monotonically_increasing_id())",
                      f"           .withColumn('_rn', F.row_number().over(Window.partitionBy({', '.join(repr(x) for x in keys)}).orderBy(F.col('_ord').desc())))",
                      "           .filter('_rn = 1').drop('_ord', '_rn'))"]
            rep.warn("tdload", f"{tool}: several file rows with the same key ({', '.join(keys)}) would make Delta MERGE fail; only the LAST row per key is kept "
                               "(MultiLoad applies rows in order, so the last update wins): check this matches your data")
        else:
            rep.warn("tdload", f"{tool}: the MERGE condition is not a plain key equality, so file rows are not de-duplicated: Delta MERGE fails if one target row matches several file rows")
        lines += [f"    src.createOrReplaceTempView({view!r})", f"    spark.sql({merge!r})"]
        rep.warn("tdload", f"{tool}: {first['op'].upper()}{' + INSERT (upsert)' if extra else ''} -> MERGE INTO {cm(tbl)[:40]}: the target must be a Delta/Iceberg table; "
                           "set-based, so it is not row-by-row identical to MultiLoad's sequential apply")
    return lines, True


def _xsql_set(body, rep):
    if body.startswith("UPDATE SET "):
        items = [(a.split("=", 1)[0].strip(), _xsql(a.split("=", 1)[1].strip(), rep)) for a in split_args(body[11:])]
        return "UPDATE SET " + ", ".join(f"{c} = {e}" for c, e in items)
    m = re.match(r"INSERT \((.*?)\) VALUES \((.*)\)$", body, re.S)
    if m:
        return f"INSERT ({m.group(1)}) VALUES ({', '.join(_xsql(v.strip(), rep) for v in split_args(m.group(2)))})"
    return body


# ------------------------------------------------------------------------------------------------------------------ emitter
def emit(loads, rep, tool):
    lines = ["from pyspark.sql import SparkSession, functions as F", "",
             'spark = SparkSession.builder.appName("converted").getOrCreate()', ""]
    calls = []
    for n, ld in enumerate(loads, 1):
        fn = f"load_{n}"
        if "ddl" in ld:
            from .sql_conv import convert_sql
            out = convert_sql(ld["ddl"], rep, dialect="teradata", out="sql").strip().rstrip(";")
            body = "\n".join(l for l in out.splitlines() if not l.startswith("--")).strip()
            lines += [f"def {fn}():", f'    spark.sql({body!r})' if body else "    pass", ""]
            calls.append(fn)
            continue
        cols = [(c, t) for c, t in ld.get("cols", []) if _COL.fullmatch(c)]
        if ld.get("export"):
            sel = ld.get("select") or ""
            from .sql_conv import convert_sql
            q = convert_sql(sel, rep, dialect="teradata", out="sql").strip().rstrip(";") if sel else ""
            q = "\n".join(l for l in q.splitlines() if not l.startswith("--")).strip()
            lines += [f"def {fn}():", f'    """{tool} export: query result written to a delimited file"""',
                      f"    df = spark.sql({q!r})" if q else "    raise NotImplementedError('TODO(sparqix): export query not found')",
                      f"    df.write.mode('overwrite').option('sep', {ld['delim']!r}).csv({ld.get('path') or 'TODO_path'!r})", ""]
            calls.append(fn)
            continue
        path, delim = ld.get("path"), ld.get("delim")
        target = _ident(ld.get("target"), rep, "table")
        if not cols:
            rep.todo("tdload", f"{tool}: no column layout found for {cm(path)[:40]}: define the schema by hand")
        lines.append(f"def {fn}():")
        lines.append(f'    """{tool}: load {cm(path)[:60].replace(chr(34), chr(39))} into {cm(target)[:60]}"""')
        if delim is not None:
            schema = ", ".join(f"{c} STRING" for c, _ in cols) or "_c0 STRING"
            lines.append(f"    df = (spark.read.schema({schema!r}).option('sep', {delim!r}).option('header', {bool(ld.get('header'))!r})")
            lines.append(f"          .option('mode', 'PERMISSIVE').csv({path!r}))")
        else:
            widths = ld.get("widths") or []
            pos, exprs = 1, []
            for i, (c, t) in enumerate(cols):
                w = _width(t) or 0
                start = widths[i] if i < len(widths) and widths[i] else pos
                if not w:
                    rep.todo("tdload", f"{tool}: fixed-width field {c} has no length: set it by hand")
                exprs.append(f"F.substring('value', {start}, {w}).alias({c!r})")
                pos = start + w
            lines.append(f"    raw = spark.read.text({path!r})")
            lines.append(f"    df = raw.select({', '.join(exprs) or 'F.col(' + repr('value') + ')'})")
        casts = [f"F.trim(F.col({c!r})).cast({_spark_type(t)!r}).alias({c!r})" if _spark_type(t) != "string" else f"F.col({c!r})" for c, t in cols]
        if casts:
            lines.append(f"    df = df.select({', '.join(casts)})")
        dml = ld.get("dml") or []
        if ld.get("values"):
            vals = [v for v in ld["values"] if _COL.fullmatch(v)]
            lines.append(f"    df = df.select({', '.join(repr(v) for v in vals)})")
            if ld.get("tcols"):
                lines.append(f"    df = df.toDF({', '.join(repr(c) for c in ld['tcols'] if _COL.fullmatch(c))})")
            lines.append(f"    df.write.mode('append').insertInto({target!r})")
        elif dml:
            ins = [d for d in dml if re.match(r"(?i)INSERT", d)]
            other = [d for d in dml if not re.match(r"(?i)INSERT", d)]
            if len(ins) == 1 and not other:
                m = re.match(r"(?is)INSERT\s+(?:INTO\s+)?([\w.\"]+)\s*(\(.*?\))?\s*VALUES\s*\((.*)\)$", ins[0])
                if m:
                    vals = [a.strip().lstrip(":") for a in split_args(m.group(3))]
                    if all(_COL.fullmatch(v) for v in vals):
                        lines.append(f"    df = df.select({', '.join(repr(v) for v in vals)})")
                        if m.group(2):
                            tc = [c.strip() for c in split_args(m.group(2)[1:-1]) if _COL.fullmatch(c.strip())]
                            lines.append(f"    df = df.toDF({', '.join(repr(c) for c in tc)})")
                        lines.append(f"    df.write.mode('append').insertInto({_ident(m.group(1), rep, 'table')!r})")
                        lines.append("")
                        calls.append(fn)
                        continue
            fields = {c.lower(): c for c, _ in cols}
            mlines, ok = _merge_lines(dml, n, fields, rep, tool) if cols else ([], False)
            if ok:
                lines += mlines
            else:
                rep.todo("tdload", f"{tool}: UPDATE/DELETE/upsert DML ({cm(ld.get('label') or '')[:20]}) could not be turned into a MERGE: staged as a temp view, write the MERGE by hand")
                lines.append("    df.createOrReplaceTempView('stage_" + str(n) + "')")
                for d in dml:
                    lines.append("    # TODO(sparqix): translate to MERGE INTO ... USING stage_" + str(n) + ": " + cm(d)[:150])
                lines.append("    raise NotImplementedError('TODO(sparqix): DML apply')")
        else:
            rep.todo("tdload", f"{tool}: no INSERT/DML found for {cm(path)[:40]}")
            lines.append("    raise NotImplementedError('TODO(sparqix): no DML found')")
        lines.append("")
        calls.append(fn)
    lines.append("")
    lines += [f"{c}()" for c in calls]
    rep.note(f"{tool}: error tables, checkpoints, restart logs and session/sleep/tenacity settings have no Spark equivalent and are dropped; "
             "rejected rows: Spark's PERMISSIVE mode nulls bad fields, validate counts after loading")
    return "\n".join(lines).rstrip() + "\n"


def kind_of(src):
    if re.search(r"(?im)^[ \t]*DEFINE[ \t]+JOB\b", src):
        return "tpt"
    if re.search(r"(?im)^[ \t]*\.BEGIN[ \t]+(IMPORT[ \t]+)?MLOAD\b", src):
        return "multiload"
    if re.search(r"(?im)^[ \t]*BEGIN[ \t]+LOADING\b", src):
        return "fastload"
    return None


def convert_tdload(source: str, report: Report) -> str:
    kind = kind_of(source) or "fastload"
    parse = {"tpt": parse_tpt, "multiload": parse_multiload, "fastload": parse_fastload}[kind]
    loads = parse(source, report)
    tool = {"tpt": "TPT", "multiload": "MultiLoad", "fastload": "FastLoad"}[kind]
    report.note(f"Teradata {tool} script")
    if not loads:
        report.todo("file", f"no {tool} load/export definition found")
        return ""
    return emit(loads, report, tool)
