"""Talend Open Studio / Data Integration job (.item XML) -> PySpark.

Handles the common data-flow components (file/DB input and output, tMap, tFilterRow, tAggregateRow, tSortRow, tUniqRow,
tUnite, tJoin, tLogRow, tReplace-free). Java expressions inside tMap are translated only when simple; everything else
becomes a TODO that keeps the original expression.  Triggers (OnSubjobOk, RunIf ...) are reported, not converted.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from .core import Report, safe_fromstring, cm


def _ln(tag):
    return tag.rsplit("}", 1)[-1]


def _attr(el, name):
    for k, v in el.attrib.items():
        if _ln(k) == name:
            return v
    return None


def _unq(v):
    v = (v or "").strip()
    return v[1:-1] if len(v) >= 2 and v[0] == v[-1] == '"' else v


def _params(node):
    p, tables = {}, {}
    for e in node:
        if _ln(e.tag) != "elementParameter":
            continue
        name = e.get("name")
        if e.get("field") == "TABLE":
            rows, cur = [], {}
            for ev in e:
                if _ln(ev.tag) != "elementValue":
                    continue
                ref = ev.get("elementRef")
                if ref in cur:
                    rows.append(cur); cur = {}
                cur[ref] = ev.get("value")
            if cur:
                rows.append(cur)
            tables[name] = rows
        else:
            p[name] = e.get("value")
    return p, tables


def _ident(s):
    return re.sub(r"\W", "_", s)


def _lit(v):
    v = (v or "").strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", v):
        return f"F.lit({v})"
    if re.fullmatch(r'"[^"]*"', v):
        return f"F.lit({v!r})".replace("\"", "'") if False else f"F.lit({_unq(v)!r})"
    if v in ("true", "false"):
        return f"F.lit({v.capitalize()})"
    if v == "null":
        return "F.lit(None)"
    return None


def _expr(e, where, report, types=None):
    """Translate a Talend Java expression to Column code; on failure keep the text for a human."""
    e = (e or "").strip()
    if not e:
        return "F.lit(None)"
    from .core import Unsupported
    from .expr_xlate import translate_ex
    _KIND[0] = "unk"
    try:
        code, _KIND[0] = translate_ex(e, "java", types, warn=lambda m: report.warn(where, m))
        return code
    except Unsupported as ex:
        report.todo(where, f"Java expression not translated ({ex}): {e}")
        cleaned = re.sub(r"\b\w+\.(\w+)\b", r"\1", e).replace('"', "'")
        _CMT.append(f"# TODO(sparqify): Java expression `{cm(e)}` is passed to F.expr unchanged and will not run as is")
        return f"F.expr({cleaned!r})"


_KIND = ["unk"]    # kind of the last translated expression
_CMT: list = []     # comment lines to emit before the statement being generated

_OPS = {"EQ": "==", "NEQ": "!=", "GT": ">", "GE": ">=", "LT": "<", "LE": "<=",
        "==": "==", "!=": "!=", ">": ">", ">=": ">=", "<": "<", "<=": "<="}


def _tmap(node, ins, report, name, types=None):
    """ins: {row label: (varname)}; returns list of (output label, code lines)."""
    data = None
    for c in node:
        if _ln(c.tag) == "nodeData":
            data = c
    out = {}
    del _CMT[:]
    if data is None:
        report.todo(name, "tMap without mapper data")
        return out
    tables = {t.get("name"): t for t in data if _ln(t.tag) == "inputTables"}
    main = None
    lines_pre = []
    for label, t in tables.items():
        if label not in ins:
            continue
        if main is None and not any(en.get("expression") for en in t if _ln(en.tag) == "mapperTableEntries"):
            main = label
    if main is None:
        main = next(iter(ins), None)
    cur = ins.get(main)
    var = _ident(name)
    cur_expr = cur
    for label, t in tables.items():
        if label == main or label not in ins:
            continue
        keys = [(en.get("name"), en.get("expression")) for en in t
                if _ln(en.tag) == "mapperTableEntries" and en.get("expression")]
        how = "left" if (t.get("joinModel", "LEFT_OUTER_JOIN") == "LEFT_OUTER_JOIN") else "inner"
        if t.get("lookupMode") not in (None, "LOAD_ONCE", "RELOAD", "CACHE_OR_RELOAD"):
            report.warn(name, f"lookup mode {t.get('lookupMode')} on {label} ignored")
        conds = []
        for k, ex in keys:
            m = re.fullmatch(r"(\w+)\.(\w+)", (ex or "").strip())
            if m:
                conds.append(f"{cur_expr}[{m.group(2)!r}] == {ins[label]}[{k!r}]")
            else:
                report.todo(name, f"lookup key expression not translated: {ex}")
        if not conds:
            report.todo(name, f"lookup {label}: no usable join keys")
            conds = ["F.lit(True)"]
        if t.get("matchingMode") == "UNIQUE_MATCH":
            report.note(f"{name}: UNIQUE_MATCH lookup {label} - Spark returns all matches; dedupe the lookup if needed")
        same = [k for k, ex in keys if re.fullmatch(r"\w+\." + re.escape(k), (ex or "").strip())]
        if keys and len(same) == len(keys):
            lines_pre.append(f"{var}_j = {cur_expr}.join({ins[label]}, on={same!r}, how={how!r})")
        else:
            lines_pre.append(f"{var}_j = {cur_expr}.join({ins[label]}, " + " & ".join(f"({c})" for c in conds) + f", {how!r})")
            report.warn(name, f"join with {label}: columns with the same name on both sides become ambiguous - select explicitly")
        cur_expr = f"{var}_j"
    for vt in (t for t in data if _ln(t.tag) == "varTables"):
        for en in vt:
            if _ln(en.tag) != "mapperTableEntries" or not en.get("expression"):
                continue
            ve = _expr(en.get("expression"), name, report, types)
            lines_pre.extend(_CMT)
            del _CMT[:]
            if types is not None:
                types["_v_" + en.get("name")] = _KIND[0]
            lines_pre.append(f"{var}_v = {cur_expr}.withColumn({'_v_' + en.get('name')!r}, {ve})")
            cur_expr = f"{var}_v"
    out_tables = [t for t in data if _ln(t.tag) == "outputTables"]
    for t in out_tables:
        label = t.get("name")
        lines = list(lines_pre)
        base = cur_expr
        filt = t.get("expressionFilter")
        if t.get("activateExpressionFilter") == "true" and filt:
            fe = _expr(filt, name, report, types)
            lines.append(f"{var}_{label} = {base}.filter({fe})")
            base = f"{var}_{label}"
        if t.get("reject") == "true" or t.get("rejectInnerJoin") == "true":
            report.todo(name, f"reject output {label}: build it with an anti-join / complementary filter")
        cols = []
        for en in t:
            if _ln(en.tag) != "mapperTableEntries":
                continue
            ex = _expr(en.get("expression"), name, report, types)
            cols.append(f"{ex}.alias({en.get('name')!r})")
        sel = f"{var}_{label}" if "." not in label else _ident(label)
        lines.extend(_CMT)
        del _CMT[:]
        lines.append(f"{sel} = {base}.select(" + ", ".join(cols) + ")" if cols else f"{sel} = {base}")
        lines = _CMT + lines if False else lines
        out[label] = (sel, lines)
    return out


def convert_talend(source: str, report: Report) -> str:
    try:
        root = safe_fromstring(source)
    except ET.ParseError as e:
        report.todo("talend", f"not valid XML: {e}")
        return ""
    nodes = {}
    order = []
    for n in root:
        if _ln(n.tag) != "node":
            continue
        p, tb = _params(n)
        uid = p.get("UNIQUE_NAME") or n.get("componentName")
        nodes[uid] = dict(el=n, comp=n.get("componentName"), p=p, t=tb, uid=uid)
        order.append(uid)
    if not nodes:
        report.todo("talend", "no components found in the input")
    types = {}
    kind = {"String": "str", "Integer": "num", "Long": "num", "Double": "num", "Float": "num", "BigDecimal": "num", "Short": "num",
            "Date": "date", "Boolean": "bool"}
    for n in root.iter():
        if _ln(n.tag) == "column" and n.get("name") and n.get("type"):
            types.setdefault(n.get("name"), kind.get(n.get("type").replace("id_", ""), "unk"))
    conns = [c for c in root if _ln(c.tag) == "connection"]
    incoming = {u: [] for u in nodes}
    for c in conns:
        kind = c.get("connectorName")
        s, t = c.get("source"), c.get("target")
        if kind in ("FLOW", "REJECT", "FILTER", "LOOKUP") and t in incoming:
            incoming[t].append((kind, c.get("label") or s, s))
        elif kind in ("SUBJOB_OK", "SUBJOB_ERROR", "COMPONENT_OK", "COMPONENT_ERROR", "RUN_IF", "ITERATE"):
            report.note(f"trigger {kind}: {s} -> {t} (ordering only; use `sparqify orchestrate` for scheduling, order is by data flow here)")
    lines = ["from pyspark.sql import SparkSession, functions as F", "", 'spark = SparkSession.builder.appName("talend_job").getOrCreate()', ""]
    produced = {}      # (uid) -> {label: var}
    done, guard = set(), 0
    pending = list(order)
    while pending and guard < 10000:
        guard += 1
        uid = pending.pop(0)
        nd = nodes[uid]
        deps = [s for k, l, s in incoming[uid] if k in ("FLOW", "FILTER", "LOOKUP", "REJECT")]
        if any(d not in done and d in nodes for d in deps):
            pending.append(uid)
            continue
        done.add(uid)
        comp, p, t = nd["comp"], nd["p"], nd["t"]
        var = _ident(uid)
        ins = {}
        for k, l, s in incoming[uid]:
            if k == "REJECT":
                report.todo(uid, f"reject link from {s} not converted")
                continue
            src = produced.get(s, {})
            v = src.get(l) or src.get("*")
            if v:
                ins[l] = v
        main = next(iter(ins.values()), None)
        if comp in ("tFileInputDelimited", "tFileInputCSV"):
            sep = _unq(p.get("FIELDSEPARATOR") or '";"') or ";"
            hdr = (p.get("HEADER") or "0") not in ("0", "")
            lines.append(f"{var} = spark.read.csv({_unq(p.get('FILENAME'))!r}, header={hdr}, sep={sep!r}, inferSchema=True)")
            if int(p.get("HEADER") or 0) > 1 or int(p.get("FOOTER") or 0) > 0:
                report.warn(uid, "HEADER > 1 / FOOTER: extra skipped rows are not handled")
            produced[uid] = {"*": var}
            _rename_from_schema(nd, var, lines, produced, report)
        elif comp in ("tFileInputJSON", "tFileInputParquet"):
            fmt = "json" if "JSON" in comp else "parquet"
            lines.append(f"{var} = spark.read.{fmt}({_unq(p.get('FILENAME'))!r})")
            produced[uid] = {"*": var}
        elif re.match(r"t\w*Input$", comp) and "QUERY" in p:
            q = _unq(p.get("QUERY")).replace('" + "', "")
            lines.append(f"{var} = spark.read.format('jdbc').option('url', 'FILL_ME').option('query', {q!r}).load()  # TODO(sparqify): JDBC connection from {cm(comp)}")
            report.todo(uid, f"{comp}: set JDBC url/credentials")
            produced[uid] = {"*": var}
        elif comp == "tFilterRow":
            if p.get("USE_ADVANCED") == "true":
                cond = _expr(_unq(p.get("ADVANCED_COND")), uid, report, types)
                lines.extend(_CMT)
                del _CMT[:]
            else:
                conds = []
                fnmap = {"LC": "F.lower({})", "UC": "F.upper({})", "LCFIRST": "F.lower({})", "UCFIRST": "F.initcap({})",
                         "LENGTH": "F.length({})", "ABS_VALUE": "F.abs({})", "MATCH": None}
                for r in t.get("CONDITIONS", []):
                    col = f"F.col({r.get('INPUT_COLUMN')!r})"
                    fn = (r.get("FUNCTION") or "EMPTY").upper()
                    if fn not in ("EMPTY", ""):
                        tpl = fnmap.get(fn)
                        if tpl is None:
                            report.todo(uid, f"filter function {fn} on {r.get('INPUT_COLUMN')} not converted")
                            continue
                        col = tpl.format(col)
                    opname = r.get("OPERATOR")
                    op = _OPS.get(opname)
                    rval = r.get("RVALUE")
                    rv = _lit(rval) or (_expr(rval, uid, report, types) if rval and re.search(r"[A-Za-z_(]", rval) else f"F.lit({_unq(rval)!r})")
                    lines.extend(_CMT)
                    del _CMT[:]
                    if op:
                        conds.append(f"({col} {op} {rv})")
                    else:
                        report.todo(uid, f"filter operator {opname} on {r.get('INPUT_COLUMN')} not converted")
                joiner = " | " if (p.get("LOGICAL_OP") or "").strip() in ("||", "OR") else " & "
                cond = joiner.join(conds) or "F.lit(True)"
            lines.append(f"{var} = {main}.filter({cond})")
            produced[uid] = {"*": var}
        elif comp == "tAggregateRow":
            g = [r["INPUT_COLUMN"] for r in t.get("GROUPBYS", [])]
            fn = {"count": "F.count", "sum": "F.sum", "avg": "F.avg", "min": "F.min", "max": "F.max", "first": "F.first", "last": "F.last",
                  "count_distinct": "F.countDistinct"}
            aggs = []
            for r in t.get("OPERATIONS", []):
                f = fn.get((r.get("FUNCTION") or "").lower())
                if not f:
                    report.todo(uid, f"aggregate function {r.get('FUNCTION')} not converted")
                    continue
                aggs.append(f"{f}(F.col({r.get('INPUT_COLUMN')!r})).alias({r.get('OUTPUT_COLUMN')!r})")
            gb = ", ".join(repr(x) for x in g)
            lines.append(f"{var} = {main}.groupBy({gb}).agg({', '.join(aggs)})" if g else f"{var} = {main}.agg({', '.join(aggs)})")
            produced[uid] = {"*": var}
        elif comp == "tSortRow":
            ks = [f"F.col({r['COLNAME']!r}).{'desc' if (r.get('ORDER') or r.get('SORT') or 'asc').lower().startswith('desc') else 'asc'}()" for r in t.get("CRITERIA", [])]
            lines.append(f"{var} = {main}.orderBy({', '.join(ks)})")
            produced[uid] = {"*": var}
        elif comp == "tUniqRow":
            ks = [r["COLUMN"] for r in t.get("UNIQUE_KEY", []) if (r.get("KEY_ATTRIBUTE") or "true") == "true"]
            lines.append(f"{var} = {main}.dropDuplicates({ks!r})" if ks else f"{var} = {main}.distinct()")
            produced[uid] = {"*": var}
        elif comp == "tUnite":
            vs = list(ins.values())
            lines.append(f"{var} = " + (vs[0] + "".join(f".unionByName({v})" for v in vs[1:]) if vs else "None"))
            produced[uid] = {"*": var}
        elif comp == "tMap":
            outs = _tmap(nd["el"], ins, report, uid, types)
            produced[uid] = {}
            for label, (v, ls) in outs.items():
                lines.extend(ls)
                produced[uid][label] = v
        elif comp == "tLogRow":
            lines.append(f"{main}.show(truncate=False)")
        elif comp in ("tFileOutputDelimited", "tFileOutputCSV"):
            sep = _unq(p.get("FIELDSEPARATOR") or '";"') or ";"
            lines.append(f"{main}.write.mode('overwrite').csv({_unq(p.get('FILENAME'))!r}, header=True, sep={sep!r})")
            report.note(f"{uid}: Spark writes a directory of part files, not a single file")
        elif comp in ("tFileOutputJSON", "tFileOutputParquet"):
            fmt = "json" if "JSON" in comp else "parquet"
            lines.append(f"{main}.write.mode('overwrite').{fmt}({_unq(p.get('FILENAME'))!r})")
        elif re.match(r"t\w*Output$", comp) and "TABLE" in p:
            act = (p.get("DATA_ACTION") or "INSERT").upper()
            tbl = _unq(p.get("TABLE"))
            if act in ("INSERT", "TRUNCATE_AND_INSERT") or not act:
                mode = "overwrite" if act == "TRUNCATE_AND_INSERT" else "append"
                lines.append(f"{main}.write.mode({mode!r}).saveAsTable({tbl!r})")
            else:
                lines.append(f"# TODO(sparqify): {cm(comp)} action {cm(act)} on {cm(tbl)} needs MERGE (use --delta tooling)")
                report.todo(uid, f"{comp} DATA_ACTION={act} not converted (use a Delta MERGE)")
        elif comp in ("tPrejob", "tPostjob", "tJava", "tJavaRow", "tRunJob", "tSetGlobalVar", "tWarn", "tDie"):
            report.todo(uid, f"{comp} not converted")
            lines.append(f"# TODO(sparqify): {cm(comp)} {cm(uid)} not converted")
            if main:
                produced[uid] = {"*": main}
        else:
            report.todo(uid, f"unsupported Talend component {comp}")
            lines.append(f"# TODO(sparqify): unsupported component {cm(comp)} ({cm(uid)})")
            if main:
                produced[uid] = {"*": main}
    if pending:
        report.todo("talend", "cyclic or unresolved flow between: " + ", ".join(pending))
    return "\n".join(lines) + "\n"


def _rename_from_schema(nd, var, lines, produced, report):
    """When the CSV has no header, name columns from the component schema."""
    p = nd["p"]
    if (p.get("HEADER") or "0") not in ("0", ""):
        return
    cols = [c.get("name") for m in nd["el"] if _ln(m.tag) == "metadata" and m.get("connector") == "FLOW"
            for c in m if _ln(c.tag) == "column"]
    if cols:
        lines[-1] = lines[-1].replace("header=False", "header=False")
        lines.append(f"{var} = {var}.toDF({', '.join(repr(c) for c in cols)})")
