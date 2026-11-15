"""PowerCenter repository export (XML, POWERMART) -> IR."""
from __future__ import annotations

import xml.etree.ElementTree as ET

from .core import Report, safe_fromstring
from .infa_ir import Edge, Mapping, Node, Port

KINDS = {
    "source qualifier": "sq", "application source qualifier": "sq", "expression": "expression", "filter": "filter",
    "aggregator": "aggregator", "joiner": "joiner", "lookup procedure": "lookup", "router": "router",
    "sorter": "sorter", "union": "union", "sequence": "sequence", "update strategy": "update",
    "normalizer": "normalizer", "rank": "rank", "stored procedure": "other", "custom transformation": "other",
    "java": "other", "transaction control": "other", "xml generator": "other", "xml parser": "other",
    "mapplet": "other", "input transformation": "other", "output transformation": "other",
}


def port_direction(pt: str) -> tuple[str, dict]:
    u = (pt or "").upper()
    flags = {"master": "MASTER" in u, "porttype": pt}
    if "LOCAL VARIABLE" in u or u == "VARIABLE":
        return "var", flags
    if "LOOKUP" in u:
        flags["lookup_return"] = "OUTPUT" in u
        return "lookup", flags
    if "INPUT" in u and "OUTPUT" in u:
        return "inout", flags
    if "INPUT" in u:
        return "in", flags
    return "out", flags


def _attrs(el):
    return {a.get("NAME"): a.get("VALUE") for a in el.findall("TABLEATTRIBUTE")}


def _transformation(el, inst_name) -> Node:
    ttype = el.get("TYPE", "")
    kind = KINDS.get(ttype.lower(), "other")
    node = Node(inst_name, kind, attrs=_attrs(el), type_name=ttype)
    for f in el.findall("TRANSFORMFIELD"):
        d, flags = port_direction(f.get("PORTTYPE"))
        flags.update({"exprtype": f.get("EXPRESSIONTYPE"), "datatype": f.get("DATATYPE"),
                      "sortkey": (f.get("ISSORTKEY") or "").upper() == "YES",
                      "sortdir": f.get("SORTDIRECTION"), "default": f.get("DEFAULTVALUE")})
        expr = f.get("EXPRESSION")
        node.ports.append(Port(f.get("NAME"), d, expr if expr not in (None, "", f.get("NAME")) else None,
                               f.get("GROUP"), flags))
    for g in el.findall("GROUP"):
        node.groups.append({"name": g.get("NAME"), "expr": g.get("EXPRESSION") or None,
                            "kind": "default" if "DEFAULT" in (g.get("TYPE") or "").upper() else
                            ("input" if (g.get("TYPE") or "").upper() == "INPUT" else "output"),
                            "order": int(g.get("ORDER") or 0)})
    return node


def _definition(el, kind) -> Node:
    attrs = {k.lower(): v for k, v in el.attrib.items()}
    node = Node(el.get("NAME"), kind, attrs=attrs, type_name=kind.upper())
    fields = el.findall("SOURCEFIELD") + el.findall("TARGETFIELD")
    for f in fields:
        node.ports.append(Port(f.get("NAME"), "out" if kind == "source" else "in", attrs={"datatype": f.get("DATATYPE")}))
    ff = el.find("FLATFILE")
    if ff is not None:
        node.attrs["flatfile"] = {k.lower(): v for k, v in ff.attrib.items()}
    return node


def parse_powercenter(xml_text: str, report: Report) -> list:
    root = safe_fromstring(xml_text)
    mappings = []
    for folder in root.iter("FOLDER"):
        sources = {s.get("NAME"): s for s in folder.findall("SOURCE")}
        targets = {t.get("NAME"): t for t in folder.findall("TARGET")}
        reusable = {t.get("NAME"): t for t in folder.findall("TRANSFORMATION")}
        for mp in folder.findall("MAPPING"):
            m = Mapping(mp.get("NAME"))
            local = {t.get("NAME"): t for t in mp.findall("TRANSFORMATION")}
            for p in mp.findall("MAPPINGVARIABLE"):
                m.params.append(p.get("NAME"))
            for inst in mp.findall("INSTANCE"):
                name, itype, tname = inst.get("NAME"), (inst.get("TYPE") or "").upper(), inst.get("TRANSFORMATION_NAME")
                if itype == "SOURCE" and tname in sources:
                    node = _definition(sources[tname], "source")
                    node.name = name
                elif itype == "TARGET" and tname in targets:
                    node = _definition(targets[tname], "target")
                    node.name = name
                elif tname in local or tname in reusable:
                    node = _transformation(local.get(tname) if local.get(tname) is not None else reusable[tname], name)
                else:
                    node = Node(name, "other", type_name=inst.get("TRANSFORMATION_TYPE") or itype)
                    report.todo(name, f"definition of {itype.lower()} {tname!r} not found in the export")
                m.nodes[name] = node
            for c in mp.findall("CONNECTOR"):
                m.edges.append(Edge(c.get("FROMINSTANCE"), c.get("FROMFIELD"), c.get("TOINSTANCE"), c.get("TOFIELD")))
            mappings.append(m)
    return mappings
