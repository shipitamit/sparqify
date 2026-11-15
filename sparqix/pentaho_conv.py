"""Pentaho Data Integration / Kettle transformation (.ktr XML) -> PySpark.

Steps: CsvInput, TextFileInput, TableInput, SelectValues, FilterRows, GroupBy, SortRows, Unique, MergeJoin, Calculator (basic
arithmetic / copy / constant), Constant, ValueMapper, Append, Dummy, TextFileOutput, TableOutput.  Other steps become TODOs.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from .core import Report, safe_fromstring, cm


def _t(el, tag, default=""):
    if el is None:
        return default
    v = el.findtext(tag)
    return v if v is not None else default


def _ident(s):
    return re.sub(r"\W", "_", s)


_CMP = {"=": "==", "<>": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}


def _value(v, report, where):
    if v is None:
        return None
    ty = _t(v, "type")
    text = _t(v, "text")
    if _t(v, "isnull") == "Y":
        return "F.lit(None)"
    if ty in ("Integer", "Number", "BigNumber") and re.fullmatch(r"-?\d+(\.\d+)?", text.strip() or "x"):
        return f"F.lit({text.strip()})"
    if ty == "Boolean":
        return f"F.lit({text.strip().lower() in ('true', 'y', '1')})"
    if ty == "Date":
        report.warn(where, f"date constant {text!r}: check the format")
    return f"F.lit({text!r})"


def _cond(c, report, where):
    """FilterRows <condition> tree -> Column code."""
    subs = c.find("conditions")
    if subs is not None and len(subs):
        parts = []
        for i, sc in enumerate(subs.findall("condition")):
            code = _cond(sc, report, where)
            op = _t(sc, "operator", "-")
            parts.append((op, code))
        out = parts[0][1]
        for op, code in parts[1:]:
            out = f"({out} {'|' if op.upper() == 'OR' else '&'} {code})"
        return f"(~{out})" if _t(c, "negated") == "Y" else out
    left, fn, right = _t(c, "leftvalue"), _t(c, "function"), _t(c, "rightvalue")
    lc = f"F.col({left!r})"
    rv = _value(c.find("value"), report, where) if not right else f"F.col({right!r})"
    if fn in _CMP:
        code = f"({lc} {_CMP[fn]} {rv})"
    elif fn == "IS NULL":
        code = f"{lc}.isNull()"
    elif fn == "IS NOT NULL":
        code = f"{lc}.isNotNull()"
    elif fn == "CONTAINS":
        code = f"{lc}.contains({rv})"
    elif fn == "STARTS WITH":
        code = f"{lc}.startswith({rv})"
    elif fn == "ENDS WITH":
        code = f"{lc}.endswith({rv})"
    elif fn == "LIKE":
        code = f"{lc}.like({rv})"
    elif fn == "IN LIST":
        vals = [x for x in _t(c.find("value"), "text").split(";")]
        code = f"{lc}.isin({vals!r})"
    else:
        report.todo(where, f"filter function {fn!r} not converted")
        code = "F.lit(True)"
    return f"(~{code})" if _t(c, "negated") == "Y" else code


def convert_pentaho(source: str, report: Report) -> str:
    try:
        root = safe_fromstring(source)
    except ET.ParseError as e:
        report.todo("pentaho", f"not valid XML: {e}")
        return ""
    steps = {}
    order = []
    for st in root.findall("step"):
        name = _t(st, "name")
        steps[name] = st
        order.append(name)
    if not steps:
        report.todo("pentaho", "no steps found in the input")
    hops = [(_t(h, "from"), _t(h, "to")) for h in root.iter("hop") if _t(h, "enabled", "Y") != "N"]
    incoming = {n: [] for n in steps}
    for a, b in hops:
        if b in incoming and a in steps:
            incoming[b].append(a)
    lines = ["from pyspark.sql import SparkSession, functions as F", "", 'spark = SparkSession.builder.appName("pentaho_transformation").getOrCreate()', ""]
    out = {}     # (step, port) -> var ; port is "*", "true", "false"
    pending, done, guard = list(order), set(), 0
    while pending and guard < 10000:
        guard += 1
        name = pending.pop(0)
        if any(d not in done for d in incoming[name]):
            pending.append(name)
            continue
        done.add(name)
        st = steps[name]
        typ = _t(st, "type")
        var = "s_" + _ident(name)
        ups = []
        for u in incoming[name]:
            ust = steps[u]
            port = "*"
            if _t(ust, "type") == "FilterRows":
                port = "true" if _t(ust, "send_true_to") == name else ("false" if _t(ust, "send_false_to") == name else "true")
            v = out.get((u, port)) or out.get((u, "*"))
            if v:
                ups.append(v)
        main = ups[0] if ups else None
        if typ in ("CsvInput", "TextFileInput"):
            path = _t(st, "filename") or _t(st.find("file"), "name")
            sep = _t(st, "separator", ",") or ","
            hdr = _t(st, "header", "Y") == "Y"
            lines.append(f"{var} = spark.read.csv({path!r}, header={hdr}, sep={sep!r}, inferSchema=True)")
            cols = [_t(f, "name") for f in st.findall("fields/field")]
            if cols and not hdr:
                lines.append(f"{var} = {var}.toDF({', '.join(repr(c) for c in cols)})")
            if _t(st, "enclosure") not in ("", '"'):
                report.warn(name, f"enclosure {_t(st, 'enclosure')!r}: set the `quote` option")
        elif typ == "TableInput":
            sql = _t(st, "sql").strip()
            lines.append(f"{var} = spark.read.format('jdbc').option('url', 'FILL_ME').option('query', {sql!r}).load()  # TODO(sparqix): JDBC connection {cm(_t(st, 'connection'))}")
            report.todo(name, f"TableInput: set JDBC url/credentials for connection {_t(st, 'connection')!r}")
            if "${" in sql or "?" in sql:
                report.todo(name, "SQL contains Kettle variables or ? parameters")
        elif typ == "SelectValues":
            cur = main
            sel = st.findall("fields/field")
            names = [(_t(f, "name"), _t(f, "rename")) for f in sel if _t(f, "name")]
            rem = [_t(r, "name") for r in st.findall("fields/remove")]
            if names:
                cols = ", ".join(f"F.col({a!r}).alias({b!r})" if b else f"F.col({a!r})" for a, b in names)
                if _t(st, "fields/select_unspecified") == "Y":
                    report.warn(name, "select_unspecified=Y: columns not listed are kept in Kettle but dropped here")
                cur = f"{cur}.select({cols})"
            if rem:
                cur = f"{cur}.drop({', '.join(repr(r) for r in rem)})"
            lines.append(f"{var} = {cur}")
        elif typ == "FilterRows":
            cnd = st.find("compare/condition")
            code = _cond(cnd, report, name) if cnd is not None else "F.lit(True)"
            t_to, f_to = _t(st, "send_true_to"), _t(st, "send_false_to")
            lines.append(f"{var}_true = {main}.filter({code})")
            out[(name, "true")] = f"{var}_true"
            if f_to:
                lines.append(f"{var}_false = {main}.filter(~{code})")
                out[(name, "false")] = f"{var}_false"
            if not t_to and not f_to:
                out[(name, "*")] = f"{var}_true"
            report.note(f"{name}: rows where the condition is NULL are in neither output here; check how your Kettle data handles NULLs")
        elif typ == "GroupBy":
            g = [_t(f, "name") or _t(f, "aggregate") for f in st.findall("group/field")]
            fn = {"SUM": "F.sum", "AVERAGE": "F.avg", "MIN": "F.min", "MAX": "F.max", "COUNT_ALL": "F.count", "COUNT_DISTINCT": "F.countDistinct",
                  "FIRST": "F.first", "LAST": "F.last", "MEDIAN": None, "STANDARD_DEVIATION": "F.stddev"}
            aggs = []
            for f in st.findall("fields/field"):
                ty = _t(f, "type").upper()
                tgt, subj = _t(f, "aggregate"), _t(f, "subject")
                f_ = fn.get(ty)
                if ty in ("COUNT_ALL", "COUNT_ANY"):
                    aggs.append(f"F.count(F.lit(1)).alias({tgt!r})")
                elif f_:
                    aggs.append(f"{f_}(F.col({subj!r})).alias({tgt!r})")
                else:
                    report.todo(name, f"aggregate type {ty} not converted")
            lines.append(f"{var} = {main}.groupBy({', '.join(repr(x) for x in g)}).agg({', '.join(aggs)})" if g else f"{var} = {main}.agg({', '.join(aggs)})")
            if _t(st, "give_back_row") == "Y" or _t(st, "all_rows") == "Y":
                report.todo(name, "'all rows' (window) mode not converted")
        elif typ == "SortRows":
            ks = [f"F.col({_t(f, 'name')!r}).{'asc' if _t(f, 'ascending', 'Y') == 'Y' else 'desc'}()" for f in st.findall("fields/field")]
            lines.append(f"{var} = {main}.orderBy({', '.join(ks)})")
        elif typ in ("Unique", "UniqueRowsByHashSet"):
            ks = [_t(f, "name") for f in st.findall("fields/field")]
            lines.append(f"{var} = {main}.dropDuplicates({ks!r})" if ks else f"{var} = {main}.distinct()")
        elif typ == "MergeJoin":
            a, b = _t(st, "step1"), _t(st, "step2")
            ka = [_t(k, ".") or (k.text or "") for k in st.findall("keys_1/key")]
            kb = [(k.text or "") for k in st.findall("keys_2/key")]
            how = {"INNER": "inner", "LEFT OUTER": "left", "RIGHT OUTER": "right", "FULL OUTER": "outer"}.get(_t(st, "join_type", "INNER"), "inner")
            va, vb = out.get((a, "*")) or out.get((a, "true")), out.get((b, "*")) or out.get((b, "true"))
            if ka == kb:
                lines.append(f"{var} = {va}.join({vb}, on={ka!r}, how={how!r})")
            else:
                cond = " & ".join(f"({va}[{x!r}] == {vb}[{y!r}])" for x, y in zip(ka, kb))
                lines.append(f"{var} = {va}.join({vb}, {cond}, {how!r})")
        elif typ == "Calculator":
            cur = main
            for cal in st.findall("calculation"):
                fld, ct = _t(cal, "field_name"), _t(cal, "calc_type")
                fa, fb = _t(cal, "field_a"), _t(cal, "field_b")
                ops = {"ADD": "+", "SUBTRACT": "-", "MULTIPLY": "*", "DIVIDE": "/"}
                if ct in ops:
                    expr = f"(F.col({fa!r}) {ops[ct]} F.col({fb!r}))"
                elif ct == "COPY_OF_FIELD":
                    expr = f"F.col({fa!r})"
                elif ct == "CONSTANT":
                    expr = f"F.lit({fa!r})"
                elif ct == "SQUARE_ROOT":
                    expr = f"F.sqrt(F.col({fa!r}))"
                elif ct == "SQUARE":
                    expr = f"(F.col({fa!r}) * F.col({fa!r}))"
                else:
                    report.todo(name, f"calculator type {ct} for {fld} not converted")
                    continue
                lines.append(f"{var} = {cur}.withColumn({fld!r}, {expr})")
                cur = var
            if cur == main:
                lines.append(f"{var} = {main}")
        elif typ == "Constant":
            cur = main
            for f in st.findall("fields/field"):
                val = _t(f, "nullif")
                ty = _t(f, "type")
                lit = f"F.lit({val})" if ty in ("Integer", "Number", "BigNumber") and re.fullmatch(r"-?\d+(\.\d+)?", val or "x") else f"F.lit({val!r})"
                lines.append(f"{var} = {cur}.withColumn({_t(f, 'name')!r}, {lit})")
                cur = var
        elif typ == "ValueMapper":
            fld, tgt = _t(st, "field_to_use"), _t(st, "target_field") or _t(st, "field_to_use")
            expr = None
            for f in st.findall("fields/field"):
                cond = f"(F.col({fld!r}) == F.lit({_t(f, 'source_value')!r}))"
                val = f"F.lit({_t(f, 'target_value')!r})"
                expr = f"F.when({cond}, {val})" if expr is None else f"{expr}.when({cond}, {val})"
            dflt = _t(st, "non_match_default")
            expr = (expr + (f".otherwise(F.lit({dflt!r}))" if dflt else f".otherwise(F.col({fld!r}))")) if expr else f"F.col({fld!r})"
            lines.append(f"{var} = {main}.withColumn({tgt!r}, {expr})")
        elif typ == "Append":
            hn, tn = _t(st, "head_name"), _t(st, "tail_name")
            vs = [out.get((x, "*")) or out.get((x, "true")) for x in (hn, tn)]
            lines.append(f"{var} = {vs[0]}.unionByName({vs[1]})")
        elif typ == "Dummy":
            lines.append(f"{var} = {main}")
        elif typ == "TextFileOutput":
            path = _t(st.find("file"), "name")
            sep = _t(st, "separator", ";") or ";"
            lines.append(f"{main}.write.mode('overwrite').csv({path!r}, header=True, sep={sep!r})")
            report.note(f"{name}: Spark writes a directory of part files, not a single file")
        elif typ == "TableOutput":
            tbl = ".".join(x for x in (_t(st, "schema"), _t(st, "table")) if x)
            mode = "overwrite" if _t(st, "truncate") == "Y" else "append"
            lines.append(f"{main}.write.mode({mode!r}).saveAsTable({tbl!r})")
        else:
            lines.append(f"# TODO(sparqix): unsupported step {cm(typ)} ({cm(name)})")
            report.todo(name, f"unsupported Pentaho step type {typ}")
            if main:
                lines.append(f"{var} = {main}")
        if (name, "*") not in out and (name, "true") not in out and not typ.endswith("Output") and any(l.startswith(f"{var} =") for l in lines[-3:]):
            out[(name, "*")] = var
    if pending:
        report.todo("pentaho", "cyclic or unresolved hops between: " + ", ".join(pending))
    return "\n".join(lines) + "\n"
