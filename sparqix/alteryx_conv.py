"""Alteryx workflow (.yxmd XML) -> PySpark.

Covers Input/Output Data (csv, and DB with a TODO), Filter (custom expression), Formula, Select, Sort, Summarize, Unique,
Union, Join, Sample (first N). Alteryx expressions are translated to Column code by expr_xlate; unsupported ones become a TODO.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from .core import Report, safe_fromstring, cm

_CMT: list = []     # TODO comment lines to emit before the statement being generated


def to_col(e: str, where, report: Report) -> str:
    """Alteryx expression -> PySpark Column code; unsupported input is kept as text with a TODO."""
    from .core import Unsupported
    from .expr_xlate import translate
    try:
        return translate(e or "", "alteryx", None, warn=lambda m: report.warn(where, m))
    except Unsupported as ex:
        report.todo(where, f"Alteryx expression not translated ({ex}): {e}")
        _CMT.append(f"# TODO(sparqix): Alteryx expression `{cm((e or '').strip())}` is not translated")
        return "F.lit(None).cast('string')"          # a bare F.lit(None) is a VOID column, which Spark cannot write to CSV or parquet


def _ident(s):
    return re.sub(r"\W", "_", s)


def convert_alteryx(source: str, report: Report) -> str:
    try:
        root = safe_fromstring(source)
    except ET.ParseError as e:
        report.todo("alteryx", f"not valid XML: {e}")
        return ""
    tools = {}
    for n in root.iter("Node"):
        g = n.find("GuiSettings")
        plugin = (g.get("Plugin") if g is not None else "") or ""
        cfg = n.find("Properties/Configuration")
        tools[n.get("ToolID")] = dict(plugin=plugin.rsplit(".", 1)[-1] if plugin else "", full=plugin, cfg=cfg if cfg is not None else ET.Element("x"),
                                      ann=(n.findtext("Properties/Annotation/DefaultAnnotationText") or "").strip())
    if not tools:
        report.todo("alteryx", "no tools found in the input")
    edges = []
    for c in root.iter("Connection"):
        o, d = c.find("Origin"), c.find("Destination")
        if o is not None and d is not None:
            edges.append((o.get("ToolID"), o.get("Connection"), d.get("ToolID"), d.get("Connection")))
    ins = {t: [] for t in tools}
    for o, oc, d, dc in edges:
        if d in ins:
            ins[d].append((dc, o, oc))
    lines = ["from pyspark.sql import SparkSession, functions as F", "", 'spark = SparkSession.builder.appName("alteryx_workflow").getOrCreate()', ""]
    out_var = {}   # (tool, output name) -> var
    pending, done, guard = list(tools), set(), 0
    while pending and guard < 10000:
        guard += 1
        tid = pending.pop(0)
        if any(o not in done for _, o, _ in ins[tid] if o in tools):
            pending.append(tid)
            continue
        done.add(tid)
        t = tools[tid]
        name, cfg, var = t["plugin"], t["cfg"], f"t{tid}"
        where = f"tool {tid} ({name})"
        del _CMT[:]
        start = len(lines)
        inp = {dc: out_var.get((o, oc)) for dc, o, oc in ins[tid]}
        main = inp.get("Input") or next(iter(inp.values()), None)
        if t["ann"]:
            lines.append(f"# {cm(t['ann'])}")
        if name in ("DbFileInput",):
            f = (cfg.findtext("File") or "").strip()
            if re.match(r"(?i)^(odbc|oledb|aka:|sqlserver|oracle)", f) or f.lower().startswith("aka:"):
                lines.append(f"{var} = spark.read.format('jdbc').option('url', 'FILL_ME').option('query', 'FILL_ME').load()  # TODO(sparqix): database input {cm(f[:40])}")
                report.todo(where, "database input: set JDBC url and query")
            elif f.lower().split("|")[0].endswith((".xlsx", ".xls", ".yxdb")):
                lines.append(f"{var} = spark.read.format('FILL_ME').load({f.split('|')[0]!r})  # TODO(sparqix): {cm(f.split('.')[-1][:4])} input")
                report.todo(where, f"{f}: no direct Spark reader (convert to csv/parquet first)")
            else:
                lines.append(f"{var} = spark.read.csv({f.split('|')[0]!r}, header=True, inferSchema=True)")
            out_var[(tid, "Output")] = var
        elif name == "DbFileOutput":
            f = (cfg.findtext("File") or "").strip().split("|")[0]
            lines.append(f"{main}.write.mode('overwrite').csv({f!r}, header=True)")
        elif name == "Filter":
            ex = to_col(cfg.findtext("Expression") or "", where, report)
            lines.append(f"{var}_t = {main}.filter({ex})")
            lines.append(f"{var}_f = {main}.exceptAll({var}_t)")
            report.note(f"{where}: False output uses exceptAll (row-multiset difference); rows where the condition is NULL land in False (check this matches Alteryx for your data)")
            out_var[(tid, "True")] = f"{var}_t"
            out_var[(tid, "False")] = f"{var}_f"
        elif name == "Formula":
            cur = main
            for i, ff in enumerate(cfg.iter("FormulaField")):
                ex = to_col(ff.get("expression"), where, report)
                lines.append(f"{var} = {cur}.withColumn({ff.get('field')!r}, {ex})")
                cur = var
            out_var[(tid, "Output")] = cur
        elif name == "Select":
            keep, drops, ren = [], [], {}
            for sf in cfg.iter("SelectField"):
                fld = sf.get("field")
                if fld.startswith("*"):
                    continue
                if sf.get("selected") == "False":
                    drops.append(fld)
                elif sf.get("rename"):
                    ren[fld] = sf.get("rename")
            cur = main
            if drops:
                cur = f"{cur}.drop({', '.join(repr(d) for d in drops)})"
            for a, b in ren.items():
                cur = f"{cur}.withColumnRenamed({a!r}, {b!r})"
            lines.append(f"{var} = {cur}")
            report.note(f"{where}: type changes / column reordering in Select are not applied")
            out_var[(tid, "Output")] = var
        elif name == "Sort":
            ks = [f"F.col({f.get('field')!r}).{'desc' if (f.get('order') or '').lower().startswith('desc') else 'asc'}()" for f in cfg.iter("Field")]
            lines.append(f"{var} = {main}.orderBy({', '.join(ks)})")
            out_var[(tid, "Output")] = var
        elif name == "Summarize":
            gb, ag = [], []
            fn = {"Sum": "F.sum", "Avg": "F.avg", "Min": "F.min", "Max": "F.max", "Count": "F.count", "CountDistinct": "F.countDistinct",
                  "First": "F.first", "Last": "F.last"}
            for sf in cfg.iter("SummarizeField"):
                act, fld = sf.get("action"), sf.get("field")
                if act == "GroupBy":
                    gb.append(fld)
                elif act in fn:
                    ag.append(f"{fn[act]}(F.col({fld!r})).alias({(sf.get('rename') or act + '_' + fld)!r})")
                else:
                    report.todo(where, f"summarize action {act} on {fld} not converted")
            g = ", ".join(repr(x) for x in gb)
            lines.append(f"{var} = {main}.groupBy({g}).agg({', '.join(ag)})" if gb else f"{var} = {main}.agg({', '.join(ag)})")
            out_var[(tid, "Output")] = var
        elif name == "Unique":
            ks = [f.get("field") for f in cfg.iter("Field")]
            lines.append(f"{var}_u = {main}.dropDuplicates({ks!r})")
            lines.append(f"{var}_d = {main}.exceptAll({var}_u)")
            out_var[(tid, "Unique")], out_var[(tid, "Duplicate")] = f"{var}_u", f"{var}_d"
        elif name == "Union":
            vs = [v for _, v in sorted((dc, v) for dc, v in inp.items() if v)]
            lines.append(f"{var} = " + (vs[0] + "".join(f".unionByName({v}, allowMissingColumns=True)" for v in vs[1:]) if vs else "None"))
            out_var[(tid, "Output")] = var
        elif name == "Join":
            left, right = inp.get("Left"), inp.get("Right")
            lk = [(j.findall("Field")[0].get("field")) for j in cfg.findall("JoinInfo") if j.get("connection") == "Left"]
            rk = [(j.findall("Field")[0].get("field")) for j in cfg.findall("JoinInfo") if j.get("connection") == "Right"]
            if not (left and right and lk and len(lk) == len(rk)):
                report.todo(where, "join keys or inputs not found")
                out_var[(tid, "Join")] = main
            else:
                same = lk == rk
                cond = " & ".join(f"({left}[{a!r}] == {right}[{b!r}])" for a, b in zip(lk, rk))
                if same:
                    lines.append(f"{var}_j = {left}.join({right}, on={lk!r}, how='inner')")
                else:
                    lines.append(f"{var}_j = {left}.join({right}, {cond}, 'inner')")
                lines.append(f"{var}_l = {left}.join({right}, {cond}, 'left_anti')")
                lines.append(f"{var}_r = {right}.join({left}, {cond.replace(left, 'X').replace(right, left).replace('X', right)}, 'left_anti')")
                out_var[(tid, "Join")], out_var[(tid, "Left")], out_var[(tid, "Right")] = f"{var}_j", f"{var}_l", f"{var}_r"
                report.warn(where, "Join: same-named non-key columns in both inputs become ambiguous; Alteryx renames them Right_*")
        elif name == "Sample":
            n = (cfg.findtext("N") or "").strip()
            lines.append(f"{var} = {main}.limit({n or 1})")
            out_var[(tid, "Output")] = var
        elif name in ("TextInput",):
            report.todo(where, "Text Input data not converted (embed it with spark.createDataFrame)")
            lines.append(f"# TODO(sparqix): {cm(where)} has literal data")
        elif name in ("BrowseV2", "Browse", "ToolContainer", "TextBox", "Comment", "Explorer"):
            continue
        else:
            report.todo(where, f"unsupported Alteryx tool {t['full'] or name}")
            lines.append(f"# TODO(sparqix): unsupported tool {cm(tid)} {cm(t['full'])}")
            if main:
                out_var[(tid, "Output")] = main
        lines[start:start] = _CMT
        del _CMT[:]
    if pending:
        report.todo("alteryx", "cyclic or unresolved connections")
    return "\n".join(lines) + "\n"
