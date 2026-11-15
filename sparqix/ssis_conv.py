"""SSIS package (.dtsx) data flow tasks -> PySpark.

Control flow (task order, schedules) is handled by `sparqix orchestrate`; this module converts what is inside each Data Flow Task:
OLE DB / ADO.NET / Flat File sources, Derived Column, Conditional Split, Data Conversion, Sort, Aggregate, Union All, Merge Join,
Lookup, Row Count / Multicast (pass-through) and OLE DB / Flat File destinations. Expressions use expr_xlate (lang "ssis").
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from .core import Report, Unsupported, safe_fromstring, cm, dq
from .expr_xlate import translate_ex


def _ln(el):
    return el.tag.rsplit("}", 1)[-1] if isinstance(el.tag, str) else ""


def _attr(el, name):
    for k, v in el.attrib.items():
        if k.rsplit("}", 1)[-1] == name:
            return v
    return None


def _props(el):
    return {_attr(p, "name"): (p.text or "").strip() for p in el if _ln(p) == "properties" for p in p if _ln(p) == "property"} \
        if any(_ln(c) == "properties" for c in el) else {}


def _ident(s):
    return re.sub(r"\W", "_", s)


_TYPES = {"i1": "tinyint", "i2": "smallint", "i4": "int", "i8": "long", "ui1": "smallint", "ui2": "int", "ui4": "long", "r4": "float",
          "r8": "double", "wstr": "string", "str": "string", "bool": "boolean", "dbTimeStamp": "timestamp", "dbDate": "date",
          "date": "timestamp", "numeric": "decimal(38,10)", "decimal": "decimal(28,10)", "cy": "decimal(19,4)", "guid": "string"}
_AGG = {"0": "groupby", "1": "count", "2": "countd", "3": "sum", "4": "avg", "5": "min", "6": "max"}


def _col_of_lineage(s: str) -> str:
    m = re.findall(r"\[([^\]]+)\]", s or "")
    return m[-1] if m else (s or "")


def convert_ssis(source: str, report: Report) -> str:
    try:
        root = safe_fromstring(source)
    except ET.ParseError as e:
        report.todo("ssis", f"not valid XML: {e}")
        return ""
    conns = {}
    for mgr in root.iter():
        if _ln(mgr) == "ConnectionManager":
            conns[_attr(mgr, "refId") or _attr(mgr, "ObjectName")] = (_attr(mgr, "ObjectName"), next(
                (_attr(x, "ConnectionString") for x in mgr.iter() if _attr(x, "ConnectionString")), ""))
    lines = ["from pyspark.sql import SparkSession, functions as F", "", 'spark = SparkSession.builder.appName("ssis_package").getOrCreate()', ""]
    found = False
    for ex in root.iter():
        if _ln(ex) != "Executable" or "pipeline" not in (_attr(ex, "ExecutableType") or "").lower():
            continue
        pipes = [e for e in ex.iter() if _ln(e) == "pipeline"]
        if not pipes:
            continue
        found = True
        tname = _attr(ex, "ObjectName") or "data_flow"
        fn = "df_" + _ident(tname).lower()
        lines += [f"def {fn}():", f'    """Data Flow Task: {dq(tname)}"""']
        body = _flow(pipes[0], conns, report, tname)
        lines += ["    " + l if l else "" for l in (body or ["pass"])]
        lines.append("")
    if not found:
        report.todo("ssis", "no Data Flow Tasks found (control-flow-only package: use `sparqix orchestrate`)")
        return ""
    names = [l[4:l.index("(")] for l in lines if l.startswith("def ")]
    lines += ["", 'if __name__ == "__main__":'] + [f"    {n}()" for n in names]
    report.note("SSIS: data flow tasks became functions; their order/precedence comes from `sparqix orchestrate FILE.dtsx`")
    return "\n".join(lines) + "\n"


def _flow(pipe, conns, report, tname):
    comps = {}
    for c in pipe.iter():
        if _ln(c) == "component":
            comps[_attr(c, "refId") or _attr(c, "name")] = c
    by_ref = {k: c for k, c in comps.items()}
    # paths: start (component.Outputs[out]) -> end (component.Inputs[in])
    def comp_of(ref):
        return ref.split(".Outputs[")[0].split(".Inputs[")[0]
    incoming = {k: [] for k in comps}
    for p in pipe.iter():
        if _ln(p) == "path":
            s, e = _attr(p, "startId"), _attr(p, "endId")
            ec = comp_of(e)
            if ec in incoming:
                out_name = re.search(r"\.Outputs\[([^\]]*)\]", s)
                in_name = re.search(r"\.Inputs\[([^\]]*)\]", e)
                incoming[ec].append((comp_of(s), out_name.group(1) if out_name else "", in_name.group(1) if in_name else ""))
    lines = []
    out = {}      # (component ref, output name) -> var
    pending, done, guard = list(comps), set(), 0
    while pending and guard < 10000:
        guard += 1
        ref = pending.pop(0)
        if any(s not in done for s, _, _ in incoming[ref] if s in comps):
            pending.append(ref)
            continue
        done.add(ref)
        c = comps[ref]
        name = _attr(c, "name") or ref
        cls = (_attr(c, "componentClassID") or "").split(".")[-1]
        pr = _props(c)
        var = "d_" + _ident(name).lower()
        ins = [(out.get((s, o)), i) for s, o, i in incoming[ref]]
        ins = [x for x in ins if x[0]]
        main = ins[0][0] if ins else None
        start = len(lines)
        where = f"{tname}/{name}"
        cmrefs = [(_attr(x, "connectionManagerRefId") or _attr(x, "connectionManagerID") or "") for x in c.iter() if _ln(x) == "connection"]
        cmstr = next((conns[k] for k in cmrefs if k in conns), ("", ""))

        if cls not in ("OLEDBSource", "ADONETSource", "ODBCSource", "FlatFileSource") and main is None:
            report.todo(where, f"{cls or 'component'} has no input path in the package (nothing converted)")
            lines.append(f"# TODO(sparqix): {cm(cls or 'component')} {cm(name)} has no input path")
            continue

        def reg(v, outname=None):
            outs = [o for o in c.iter() if _ln(o) == "output"]
            for o in outs:
                if _attr(o, "isErrorOut") == "true" or "error" in (_attr(o, "name") or "").lower():
                    continue
                out[(ref, _attr(o, "name"))] = v
        if cls in ("OLEDBSource", "ADONETSource", "ODBCSource"):
            sql = pr.get("SqlCommand") or ""
            tbl = pr.get("OpenRowset") or pr.get("TableOrViewName") or ""
            if not sql and not tbl:
                report.todo(where, f"{cls}: no SqlCommand/OpenRowset found (query comes from a variable?)")
            q = sql or f"SELECT * FROM {tbl or 'FILL_ME'}"
            lines.append(f"{var} = spark.read.format('jdbc').option('url', 'FILL_ME').option('query', {q!r}).load()  # TODO(sparqix): JDBC connection {cm(cmstr[0])}")
            report.todo(where, f"{cls}: set JDBC url/credentials (connection {cmstr[0] or '?'})")
            if "?" in q:
                report.todo(where, "source query has ? parameters")
            reg(var)
        elif cls == "FlatFileSource":
            path = cmstr[1] or "FILL_ME"
            lines.append(f"{var} = spark.read.csv({path!r}, header=True, inferSchema=True)  # TODO(sparqix): check delimiter / header / text qualifier of the flat-file connection")
            report.todo(where, "FlatFileSource: delimiter, header and qualifier come from the connection manager definition; verify them")
            reg(var)
        elif cls == "DerivedColumn":
            cur = main
            for o in c.iter():
                if _ln(o) != "outputColumn":
                    continue
                p = _props(o)
                ex = p.get("Expression")
                if not ex:
                    continue
                nm = _attr(o, "name")
                try:
                    code, _ = translate_ex(ex, "ssis", None, lambda w: report.warn(where, w))
                except Unsupported as e:
                    report.todo(where, f"Derived Column {nm}: expression not translated ({e}): {ex}")
                    lines.append(f"# TODO(sparqix): Derived Column {cm(nm)} = {cm(ex)}")
                    code = "F.lit(None)"
                lines.append(f"{var} = {cur}.withColumn({nm!r}, {code})")
                cur = var
            if cur == main:
                lines.append(f"{var} = {main}")
            reg(var)
        elif cls == "ConditionalSplit":
            outs = [o for o in c.findall("outputs/output")]
            conds = []
            default = None
            for o in sorted(outs, key=lambda x: int(_props(x).get("EvaluationOrder", "99") or 99)):
                p = _props(o)
                oname = _attr(o, "name")
                if p.get("IsDefaultOut") == "true" or "default" in oname.lower():
                    default = oname
                    continue
                if _attr(o, "isErrorOut") == "true" or "error" in oname.lower():
                    continue
                ex = p.get("Expression")
                try:
                    code, _ = translate_ex(ex, "ssis", None, lambda w: report.warn(where, w))
                except Unsupported as e:
                    report.todo(where, f"Conditional Split {oname}: expression not translated ({e}): {ex}")
                    code = "F.lit(False)"
                v = f"{var}_{_ident(oname).lower()}"
                prior = "".join(f" & ~({x})" for x in conds)
                lines.append(f"{v} = {main}.filter({code}{prior})")
                out[(ref, oname)] = v
                conds.append(code)
            if default:
                none = " & ".join(f"~({x})" for x in conds) or "F.lit(True)"
                lines.append(f"{var}_default = {main}.filter({none})")
                out[(ref, default)] = f"{var}_default"
            report.note(f"{where}: rows are routed to the first matching output, as in SSIS")
        elif cls == "DataConvert":
            cur = main
            for o in c.iter():
                if _ln(o) == "outputColumn":
                    p = _props(o)
                    src = _col_of_lineage(p.get("SourceInputColumnLineageID") or "")
                    ty = _TYPES.get(_attr(o, "dataType") or "", "string")
                    lines.append(f"{var} = {cur}.withColumn({_attr(o, 'name')!r}, F.col({src!r}).cast({ty!r}))")
                    cur = var
            if cur == main:
                lines.append(f"{var} = {main}")
            reg(var)
        elif cls == "Sort":
            keys = []
            for ic in c.iter():
                if _ln(ic) == "inputColumn":
                    p = _props(ic)
                    pos = int(p.get("NewSortKeyPosition", "0") or 0)
                    if pos:
                        keys.append((abs(pos), _attr(ic, "cachedName") or _col_of_lineage(_attr(ic, "lineageId")), pos < 0))
            keys.sort()
            ks = ", ".join(f"F.col({n!r}).{'desc' if d else 'asc'}()" for _, n, d in keys)
            elim = pr.get("EliminateDuplicates") == "true"
            lines.append(f"{var} = {main}.orderBy({ks})" + (".dropDuplicates()" if elim else ""))
            reg(var)
        elif cls == "Aggregate":
            gb, ag = [], []
            for o in c.iter():
                if _ln(o) != "outputColumn":
                    continue
                p = _props(o)
                act = _AGG.get(p.get("AggregationType", ""), None)
                src = _col_of_lineage(p.get("AggregationColumnId") or "") or _attr(o, "name")
                if act == "groupby":
                    gb.append((_attr(o, "name"), src))
                elif act:
                    f_ = {"count": "F.count", "countd": "F.countDistinct", "sum": "F.sum", "avg": "F.avg", "min": "F.min", "max": "F.max"}[act]
                    ag.append(f"{f_}(F.col({src!r})).alias({_attr(o, 'name')!r})" if p.get("AggregationType") != "1" or src else f"F.count(F.lit(1)).alias({_attr(o, 'name')!r})")
                elif p.get("AggregationType"):
                    report.todo(where, f"aggregation type {p.get('AggregationType')} on {_attr(o, 'name')} not converted")
            g = ", ".join(f"F.col({s!r}).alias({n!r})" for n, s in gb)
            lines.append(f"{var} = {main}.groupBy({g}).agg({', '.join(ag)})" if gb else f"{var} = {main}.agg({', '.join(ag)})")
            reg(var)
        elif cls == "UnionAll":
            vs = [v for v, _ in ins]
            lines.append(f"{var} = {vs[0]}" + "".join(f".unionByName({v}, allowMissingColumns=True)" for v in vs[1:]))
            report.warn(where, "Union All maps columns by name here; SSIS maps by the editor's column mapping")
            reg(var)
        elif cls == "MergeJoin":
            jt = {"0": "left", "1": "left", "2": "inner"}.get(pr.get("JoinType", "2"), "inner")
            keys = {}
            for i_el in c.findall("inputs/input"):
                iname = _attr(i_el, "name") or ""
                keys[iname] = sorted(((int(_props(ic).get("NewComparisonFlags", "0") or 0), _attr(ic, "cachedName")) for ic in i_el.iter() if _ln(ic) == "inputColumn" and _props(ic).get("NewComparisonFlags") is not None and _attr(ic, "cachedName")))
            ln = [n for _, n in keys.get("Merge Join Left Input", [])]
            rn = [n for _, n in keys.get("Merge Join Right Input", [])]
            by_in = {i: v for v, i in ins}
            left = by_in.get("Merge Join Left Input") or (ins[0][0] if ins else None)
            right = by_in.get("Merge Join Right Input") or (ins[1][0] if len(ins) > 1 else None)
            if pr.get("JoinType") == "0":
                jt = "outer"
            if left and right and ln and len(ln) == len(rn):
                if ln == rn:
                    lines.append(f"{var} = {left}.join({right}, on={ln!r}, how={jt!r})")
                else:
                    cond = " & ".join(f"({left}[{a!r}] == {right}[{b!r}])" for a, b in zip(ln, rn))
                    lines.append(f"{var} = {left}.join({right}, {cond}, {jt!r})")
            else:
                report.todo(where, "Merge Join: could not read the join keys")
                lines.append(f"# TODO(sparqix): Merge Join {cm(name)}")
                lines.append(f"{var} = {main}")
            reg(var)
        elif cls == "Lookup":
            sql = pr.get("SqlCommand") or ""
            if not sql:
                report.todo(where, "Lookup without an inline SQL reference query")
            ref_var = var + "_ref"
            lines.append(f"{ref_var} = spark.read.format('jdbc').option('url', 'FILL_ME').option('query', {sql!r}).load()  # TODO(sparqix): JDBC connection for the lookup")
            report.todo(where, "Lookup: set JDBC url/credentials for the reference table")
            pairs = [(_attr(ic, "cachedName"), p["JoinToReferenceColumn"]) for ic in c.iter() if _ln(ic) == "inputColumn" for p in [_props(ic)] if p.get("JoinToReferenceColumn")]
            nm = pr.get("NoMatchBehavior", "0")
            if pairs:
                cond = " & ".join(f"({main}[{a!r}] == {ref_var}[{b!r}])" for a, b in pairs)
                how = "left" if nm in ("1", "2") else "inner"
                lines.append(f"{var} = {main}.join({ref_var}, {cond}, {how!r})")
                report.note(f"{where}: no-match rows are kept (NoMatchBehavior={nm})" if how == "left" else f"{where}: no-match rows are dropped; SSIS would fail the component unless configured otherwise")
            else:
                report.todo(where, "Lookup join columns not found")
                lines.append(f"{var} = {main}")
            for o in c.findall("outputs/output"):
                on = _attr(o, "name") or ""
                if "no match" in on.lower():
                    lines.append(f"{var}_nomatch = {main}.join({ref_var}, {cond if pairs else 'F.lit(False)'}, 'left_anti')")
                    out[(ref, on)] = f"{var}_nomatch"
                elif "error" not in on.lower():
                    out[(ref, on)] = var
        elif cls in ("RowCount", "Multicast"):
            lines.append(f"{var} = {main}")
            reg(var)
            if cls == "RowCount":
                report.note(f"{where}: Row Count to variable {pr.get('VariableName', '?')} is not set; use {main}.count() if you need it")
        elif cls in ("OLEDBDestination", "ADONETDestination", "ODBCDestination"):
            tbl = pr.get("OpenRowset") or pr.get("TableOrViewName") or "FILL_ME"
            tbl = tbl.replace("[", "").replace("]", "").replace('"', "")
            lines.append(f"{main}.write.mode('append').saveAsTable({tbl!r})")
            if pr.get("FastLoadKeepIdentity") == "true" or pr.get("FastLoadKeepNulls") == "true":
                report.note(f"{where}: keep-identity / keep-nulls options are ignored")
        elif cls == "FlatFileDestination":
            path = cmstr[1] or "FILL_ME"
            lines.append(f"{main}.write.mode('overwrite').csv({path!r}, header=True)  # TODO(sparqix): check delimiter of the flat-file connection")
            report.todo(where, "FlatFileDestination: path/delimiter come from the connection manager; verify")
        else:
            lines.append(f"# TODO(sparqix): unsupported SSIS component {cm(cls or '?')} ({cm(name)})")
            report.todo(where, f"unsupported SSIS component {cls or '?'}")
            if main:
                lines.append(f"{var} = {main}")
                reg(var)
    if pending:
        report.todo(tname, "cyclic or unresolved paths between: " + ", ".join(_attr(comps[p], "name") or p for p in pending))
    return lines
