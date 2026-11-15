"""Informatica Cloud Data Integration (IDMC/IICS) mapping JSON -> IR.   EXPERIMENTAL / best effort.

IICS stores a mapping as JSON: a root object with the mapping name, a list of `transformations`, each with a
name, a type and `fields` (name, datatype, precision, scale ...). The exact property names differ between
export versions and are not publicly documented in detail, so this reader discovers them heuristically:

  * transformation type  : any of type / transformationType / transformationClass / $$class text
  * field direction      : direction / portType / dir  (default: input+output)
  * links                : root `links` (transformation level; fields are linked by NAME, which is how IICS
                           propagates fields) or field level pairs when present
  * conditions / exprs   : properties such as filterCondition, joinCondition, joinType, lookupCondition,
                           lookupObject, expression (per field)

Unrecognised structures are reported as TODO instead of guessed. If your export does not load, send me a
sample (anonymised) and the key names can be added in `KEYS` below - or write the neutral IR JSON by hand.
"""
from __future__ import annotations

import json
import re

from .core import Report
from .infa_ir import Edge, Mapping, Node, Port, from_dict

KINDS = [("source", "source"), ("target", "target"), ("expression", "expression"), ("filter", "filter"),
         ("aggregat", "aggregator"), ("joiner", "joiner"), ("lookup", "lookup"), ("router", "router"),
         ("sorter", "sorter"), ("union", "union"), ("sequence", "sequence"), ("normaliz", "normalizer"),
         ("rank", "rank"), ("mapplet", "other")]
KEYS = {
    "type": ("type", "transformationType", "transformationClass", "$$class", "@type", "className"),
    "dir": ("direction", "portType", "dir", "fieldDirection"),
    "expr": ("expression", "expr", "formula"),
    "from": ("from", "fromTransformation", "fromTransformationName", "source", "fromName", "sourceTransformation"),
    "to": ("to", "toTransformation", "toTransformationName", "target", "toName", "targetTransformation"),
    "ffrom": ("fromField", "sourceField", "fromFieldName"),
    "fto": ("toField", "targetField", "toFieldName"),
}


def pick(d: dict, keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def find_mapping_root(obj):
    if isinstance(obj, dict):
        if isinstance(obj.get("transformations"), list):
            return obj
        for v in obj.values():
            r = find_mapping_root(v)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_mapping_root(v)
            if r is not None:
                return r
    return None


def kind_of(t: dict) -> str:
    text = " ".join(str(pick(t, (k,), "")) for k in KEYS["type"]).lower()
    for needle, kind in KINDS:
        if needle in text:
            return kind
    return "other"


def parse_idmc(text: str, report: Report) -> list:
    data = json.loads(text)
    if isinstance(data, dict) and "nodes" in data and "edges" in data and "name" in data:
        return [from_dict(data)]                       # neutral IR JSON
    root = find_mapping_root(data)
    if root is None:
        report.todo("idmc", "no `transformations` list found - not a recognised IDMC mapping export")
        return []
    m = Mapping(str(root.get("name") or root.get("mappingName") or "mapping"))
    for t in root["transformations"]:
        name = str(t.get("name"))
        kind = kind_of(t)
        node = Node(name, kind, type_name=str(pick(t, KEYS["type"], "")))
        for k, v in t.items():
            if isinstance(v, (str, int, float, bool)):
                node.attrs[k] = v
        for f in t.get("fields", []) or []:
            d = str(pick(f, KEYS["dir"], "inputoutput")).lower()
            direction = ("var" if "var" in d else "in" if d.startswith("in") and "out" not in d else
                         "out" if d.startswith("out") else "inout")
            node.ports.append(Port(str(f.get("name")), direction, pick(f, KEYS["expr"]), f.get("group"),
                                   {"datatype": f.get("type") or f.get("datatype")}))
        m.nodes[name] = node
    links = root.get("links") or root.get("connections") or []
    for l in links:
        a, b = pick(l, KEYS["from"]), pick(l, KEYS["to"])
        if a not in m.nodes or b not in m.nodes:
            report.todo(m.name, f"link {a!r} -> {b!r} refers to an unknown transformation")
            continue
        fl = l.get("fieldLinks") or l.get("fieldMap") or l.get("fields")
        pairs = []
        if isinstance(fl, list):
            for fp in fl:
                if isinstance(fp, dict):
                    x, y = pick(fp, KEYS["ffrom"] + ("from",)), pick(fp, KEYS["fto"] + ("to",))
                    if x and y:
                        pairs.append((x, y))
        if not pairs:                                   # IICS propagates by field name
            src_out = {p.name for p in m.nodes[a].ports if p.direction in ("out", "inout")} or {p.name for p in m.nodes[a].ports}
            dst_in = {p.name for p in m.nodes[b].ports if p.direction in ("in", "inout")} or {p.name for p in m.nodes[b].ports}
            pairs = [(n, n) for n in sorted(src_out & dst_in, key=lambda x: [p.name for p in m.nodes[a].ports].index(x))]
            report.note(f"link {a} -> {b}: fields linked by name (no field-level links found)")
        for x, y in pairs:
            m.edges.append(Edge(a, x, b, y))
    # type specific attribute normalisation
    for n in m.nodes.values():
        a = n.attrs
        low = {k.lower(): v for k, v in a.items()}
        if n.kind == "filter":
            a["Filter Condition"] = pick(low, ("filtercondition", "condition", "filter"))
        if n.kind == "joiner":
            a["Join Condition"] = pick(low, ("joincondition", "condition"))
            a["Join Type"] = {"inner": "Normal Join", "normal": "Normal Join", "left": "Detail Outer Join",
                              "right": "Master Outer Join", "full": "Full Outer Join"}.get(
                str(pick(low, ("jointype", "type_"), "inner")).lower().split()[0], "Normal Join")
        if n.kind in ("source", "target"):
            obj = pick(low, ("objectname", "object", "tablename", "sourceobject", "targetobject", "path"))
            if obj:
                obj = str(obj)
                db, _, tbl = obj.rpartition(".")
                a["name"], a["dbdname"] = tbl or obj, db or None
            a["databasetype"] = a.get("databasetype") or ""
        if n.kind == "lookup":
            a["Lookup table name"] = pick(low, ("lookupobject", "lookuptable", "objectname", "object"))
            a["Lookup condition"] = pick(low, ("lookupcondition", "condition"))
    return [m]
