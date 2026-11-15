"""IBM DataStage job export (.dsx) -> PySpark.   BEST-EFFORT: the .dsx format is not publicly specified in detail.

The reader understands the BEGIN/END block structure, stages (DSRECORD), their properties and columns (DSSUBRECORD) and links (Partner).
Converted: Sequential File / Data Set, database connectors (as JDBC with FILL_ME), Transformer (derivations, constraints, stage variables),
Filter, Sort, Remove Duplicates, Join, Funnel, Copy, Peek.  Derivations use expr_xlate (lang "datastage").  Other stages are TODOs.
"""
from __future__ import annotations

import re

from .core import Report, Unsupported, cm, dq
from .expr_xlate import translate_ex

_KV = re.compile(r'^\s*(\w+)\s+(?:"(.*)"|(\S.*))?\s*$')


def parse_dsx(text: str):
    """-> list of jobs; job = {'name', 'records': [rec]}; rec = {'f': {key: val}, 'subs': [ {key: val} ]}"""
    jobs, job, rec, sub = [], None, None, None
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i]
        i += 1
        t = raw.strip()
        if not t:
            continue
        m = re.match(r"^BEGIN\s+(\w+)$", t)
        if m:
            kind = m.group(1)
            if kind == "DSJOB":
                job = {"name": "", "records": []}
                jobs.append(job)
            elif kind == "DSRECORD" and job is not None:
                rec = {"f": {}, "subs": []}
                job["records"].append(rec)
            elif kind == "DSSUBRECORD" and rec is not None:
                sub = {}
                rec["subs"].append(sub)
            continue
        m = re.match(r"^END\s+(\w+)$", t)
        if m:
            if m.group(1) == "DSSUBRECORD":
                sub = None
            elif m.group(1) == "DSRECORD":
                rec = None
            elif m.group(1) == "DSJOB":
                job = None
            continue
        m = _KV.match(raw)
        if not m or job is None:
            continue
        key, qv, bare = m.group(1), m.group(2), m.group(3)
        if bare is not None and bare.startswith('"'):          # multi-line quoted value
            val = bare[1:]
            while not val.endswith('"') and i < len(lines):
                val += "\n" + lines[i]
                i += 1
            qv = val[:-1] if val.endswith('"') else val
        val = qv if qv is not None else (bare or "")
        val = val.replace("\\(n)", "\n").replace("\\(A)", "\n")
        target = sub if sub is not None else (rec["f"] if rec is not None else None)
        if target is not None:
            target.setdefault(key, val)
        elif key == "Identifier":
            job["name"] = val
    return jobs


def _ident(s):
    return re.sub(r"\W", "_", s or "x")


def _prop(stage, *names):
    want = [n.lower() for n in names]
    for sub in stage["subs"]:
        if sub.get("Name", "").lower() in want and "Value" in sub:
            return sub["Value"]
    return None


def _cols(pin):
    return [(s.get("Name"), s.get("Derivation") or "") for s in pin["subs"] if s.get("Name") and ("SqlType" in s or "Derivation" in s or "Precision" in s)]


def _key_name(v: str) -> str:
    return re.split(r"[\s{]", (v or "").strip(), 1)[0]


def convert_datastage(source: str, report: Report) -> str:
    jobs = parse_dsx(source)
    if not jobs:
        report.todo("datastage", "no DSJOB found (is this a .dsx export?)")
        return ""
    lines = ["from pyspark.sql import SparkSession, functions as F", "", 'spark = SparkSession.builder.appName("datastage_job").getOrCreate()', ""]
    report.note("DataStage reader is best-effort (the .dsx format is not publicly specified in detail) - verify the generated graph against the job")
    names = []
    for job in jobs:
        fn = "job_" + _ident(job["name"]).lower()
        names.append(fn)
        lines += [f"def {fn}():", f'    """DataStage job {dq(job["name"])}"""']
        body = _job(job, report)
        lines += ["    " + l if l else "" for l in (body or ["pass"])]
        lines.append("")
    lines += ["", 'if __name__ == "__main__":'] + [f"    {n}()" for n in names]
    return "\n".join(lines) + "\n"


def _job(job, report):
    recs = job["records"]
    by_id = {r["f"].get("Identifier"): r for r in recs}
    stages = {i: r for i, r in by_id.items() if r["f"].get("OLEType", "") in ("CCustomStage", "CTransformerStage", "CSeqFileStage", "CHashedFileStage", "CODBCStage", "CCustomStage2") or r["f"].get("StageType")}
    pins = {i: r for i, r in by_id.items() if "Output" in r["f"].get("OLEType", "") or "Input" in r["f"].get("OLEType", "")}
    stages = {i: r for i, r in stages.items() if i not in pins}
    if not stages:
        report.todo(job["name"], "no stages found in the job")
    pin_stage = {}
    for sid, st in stages.items():
        for key in ("InputPins", "OutputPins"):
            for p in (st["f"].get(key) or "").replace(";", ",").split(","):
                if p.strip():
                    pin_stage[p.strip()] = sid
    incoming = {sid: [] for sid in stages}          # stage -> [(upstream stage, out pin id, in pin id)]
    for pid, p in pins.items():
        partner = p["f"].get("Partner")
        if "Output" in p["f"].get("OLEType", "") and partner and partner in pin_stage and pid in pin_stage:
            incoming[pin_stage[partner]].append((pin_stage[pid], pid, partner))
    lines = []
    out = {}        # out pin id -> var
    pending, done, guard = list(stages), set(), 0
    while pending and guard < 10000:
        guard += 1
        sid = pending.pop(0)
        if any(u not in done for u, _, _ in incoming[sid]):
            pending.append(sid)
            continue
        done.add(sid)
        st = stages[sid]
        name = st["f"].get("Name") or sid
        stype = st["f"].get("StageType") or st["f"].get("OLEType") or ""
        var = "s_" + _ident(name).lower()
        ins = [(out.get(p), p, q_) for _, p, q_ in incoming[sid] if out.get(p)]
        main = ins[0][0] if ins else None
        opins = [x.strip() for x in (st["f"].get("OutputPins") or "").replace(";", ",").split(",") if x.strip()]
        where = f"{job['name']}/{name}"
        low = stype.lower()
        is_src = not incoming[sid] and opins
        is_sink = incoming[sid] and not opins

        def link_name(pid):
            return pins.get(pid, {"f": {}})["f"].get("Name", pid)
        if "sequentialfile" in low or "seqfile" in low or "dataset" in low or "fileset" in low:
            path = _prop(st, "File", "Filename", "FileName", "FilePattern", "FileSet") or "FILL_ME"
            delim = _prop(st, "Delimiter", "FieldDelimiter") or ","
            hdr = (_prop(st, "FirstLineIsColumnNames", "First Line is Column Names", "FirstLineColumnNames") or "").lower() == "true"
            if is_src:
                if "dataset" in low:
                    lines.append(f"{var} = spark.read.parquet({path!r})  # TODO(sparqix): DataSet -> parquet (adjust)")
                    report.todo(where, "DataSet stage: store/read as Parquet or Delta instead of .ds descriptor files")
                else:
                    lines.append(f"{var} = spark.read.csv({path!r}, header={hdr}, sep={delim!r}, inferSchema=True)")
                    cols = [c for c, _ in _cols(pins.get(opins[0], {"subs": []}))]
                    if cols and not hdr:
                        lines.append(f"{var} = {var}.toDF({', '.join(repr(c) for c in cols)})")
                for p in opins:
                    out[p] = var
            elif main:
                if "dataset" in low:
                    lines.append(f"{main}.write.mode('overwrite').parquet({path!r})")
                else:
                    lines.append(f"{main}.write.mode('overwrite').csv({path!r}, header={hdr}, sep={delim!r})")
                    report.note(f"{where}: Spark writes a directory of part files, not a single file")
        elif re.search(r"connector|odbc|db2|oracle|teradata|dbstage|sybase|netezza|sqlserver", low):
            sql = _prop(st, "SQL", "Sql.SelectStatement", "SelectStatement", "UserDefinedSQL", "ReadSQL", "SQLStatement")
            tbl = _prop(st, "TableName", "Table", "Tablename", "TableOrQuery")
            if is_src:
                q = sql or f"SELECT * FROM {tbl or 'FILL_ME'}"
                lines.append(f"{var} = spark.read.format('jdbc').option('url', 'FILL_ME').option('query', {q!r}).load()  # TODO(sparqix): JDBC connection for {cm(stype)}")
                report.todo(where, f"{stype}: set JDBC url/credentials")
                if "#" in q:
                    report.todo(where, "query uses DataStage job parameters (#param#)")
                for p in opins:
                    out[p] = var
            elif main:
                act = (_prop(st, "WriteMode", "WriteMethod", "Write mode") or "insert").lower()
                t_ = tbl or "FILL_ME"
                if act in ("insert", "insert statement", "0") or not act:
                    lines.append(f"{main}.write.mode('append').saveAsTable({t_!r})")
                elif "replace" in act or "truncate" in act:
                    lines.append(f"{main}.write.mode('overwrite').saveAsTable({t_!r})")
                else:
                    lines.append(f"# TODO(sparqix): write mode {act!r} on {cm(t_)} needs MERGE")
                    report.todo(where, f"write mode {act!r} not converted (use a Delta MERGE)")
        elif "transformer" in low:
            if not main:
                report.todo(where, "Transformer without input link")
                continue
            svars = [(s.get("Name"), s.get("Derivation")) for s in st["subs"] if s.get("Name") and s.get("Derivation") and s.get("Name") not in ("Constraint",)]
            sv_names = set()
            cur = main
            ln_in = link_name(ins[0][1]) if ins else ""
            for nm, dv in svars:
                try:
                    code, _ = translate_ex(dv, "datastage", None, lambda w: report.warn(where, w))
                except Unsupported as e:
                    report.todo(where, f"stage variable {nm}: derivation not translated ({e}): {dv}")
                    lines.append(f"# TODO(sparqix): stage variable {cm(nm)} = {cm(dv)}")
                    continue
                lines.append(f"{var}_sv = {cur}.withColumn({nm!r}, {code})")
                cur = f"{var}_sv"
                sv_names.add(nm)
            for p in opins:
                pr = pins.get(p)
                if not pr:
                    continue
                cons = pr["f"].get("Constraint") or _prop(pr, "Constraint")
                base = cur
                lv = f"{var}_{_ident(link_name(p)).lower()}"
                if cons and cons.strip():
                    try:
                        cc, _ = translate_ex(cons, "datastage", None, lambda w: report.warn(where, w))
                        lines.append(f"{lv}_f = {base}.filter({cc})")
                        base = f"{lv}_f"
                    except Unsupported as e:
                        report.todo(where, f"constraint on {link_name(p)} not translated ({e}): {cons}")
                        lines.append(f"# TODO(sparqix): constraint {cm(cons)}")
                cols = []
                for cn, dv in _cols(pr):
                    if not dv.strip():
                        cols.append(f"F.col({cn!r})")
                        continue
                    try:
                        code, _ = translate_ex(dv, "datastage", None, lambda w: report.warn(where, w))
                        cols.append(f"{code}.alias({cn!r})")
                    except Unsupported as e:
                        report.todo(where, f"derivation of {cn} not translated ({e}): {dv}")
                        lines.append(f"# TODO(sparqix): derivation {cm(cn)} = {cm(dv)}")
                        cols.append(f"F.lit(None).alias({cn!r})")
                lines.append(f"{lv} = {base}.select({', '.join(cols)})" if cols else f"{lv} = {base}")
                out[p] = lv
            if len(opins) > 1:
                report.note(f"{where}: Transformer with several outputs: rows go to every output whose constraint is true, as in DataStage")
        elif "filter" in low:
            where_clause = _prop(st, "Where", "WhereClause", "Where Clause") or ""
            if not main:
                continue
            lines.append(f"{var} = {main}.filter(F.expr({where_clause!r}))" if where_clause else f"{var} = {main}")
            report.warn(where, "Filter stage: the Where clause is passed to F.expr; check it is valid Spark SQL")
            for p in opins:
                out[p] = var
        elif "sort" in low or "removeduplicates" in low or "remdup" in low:
            keys = [_key_name(s.get("Value")) for s in st["subs"] if s.get("Name", "").lower() == "key" and s.get("Value")]
            if not main:
                continue
            if "sort" in low:
                ks = ", ".join(f"F.col({k!r}).asc()" for k in keys)
                lines.append(f"{var} = {main}.orderBy({ks})")
                report.warn(where, "sort direction / options come from key properties: all keys sorted ascending here")
            else:
                lines.append(f"{var} = {main}.dropDuplicates({keys!r})" if keys else f"{var} = {main}.distinct()")
                report.note(f"{where}: DataStage keeps the first/last row per key (Duplicate To Retain); dropDuplicates keeps an arbitrary one")
            for p in opins:
                out[p] = var
        elif "join" in low and "lookup" not in low:
            keys = [_key_name(s.get("Value")) for s in st["subs"] if s.get("Name", "").lower() == "key" and s.get("Value")]
            how = {"inner": "inner", "leftouter": "left", "rightouter": "right", "fullouter": "outer"}.get((_prop(st, "JoinType", "Join Type", "Operator") or "inner").lower().replace(" ", ""), "inner")
            if len(ins) >= 2 and keys:
                lines.append(f"{var} = {ins[0][0]}.join({ins[1][0]}, on={keys!r}, how={how!r})")
            else:
                report.todo(where, "Join: need two input links and key properties")
                lines.append(f"# TODO(sparqix): Join {cm(name)}")
                if main:
                    lines.append(f"{var} = {main}")
            for p in opins:
                out[p] = var
        elif "funnel" in low:
            vs = [v for v, _, _ in ins]
            lines.append(f"{var} = {vs[0]}" + "".join(f".unionByName({v}, allowMissingColumns=True)" for v in vs[1:]))
            for p in opins:
                out[p] = var
        elif "copy" in low or "peek" in low or "modify_skip" in low:
            if "peek" in low and main:
                lines.append(f"{main}.show(truncate=False)")
            elif main:
                lines.append(f"{var} = {main}")
            for p in opins:
                out[p] = var if main else None
                if out[p] is None:
                    del out[p]
        else:
            lines.append(f"# TODO(sparqix): unsupported DataStage stage {cm(stype)} ({cm(name)})")
            report.todo(where, f"unsupported DataStage stage type {stype or '?'}")
            if main:
                lines.append(f"{var} = {main}")
                for p in opins:
                    out[p] = var
    if pending:
        report.todo(job["name"], "cyclic or unresolved links between: " + ", ".join(stages[p]["f"].get("Name", p) for p in pending))
    return lines
