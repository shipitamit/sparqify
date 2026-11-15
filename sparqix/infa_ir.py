"""Neutral mapping model shared by the PowerCenter (XML) and IDMC (JSON) readers and the PySpark generator."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Port:
    name: str
    direction: str = "inout"      # in | out | inout | var | lookup
    expr: str | None = None
    group: str | None = None
    attrs: dict = field(default_factory=dict)   # master, sortkey, sortdir, exprtype, datatype, porttype ...


@dataclass
class Node:
    name: str
    kind: str                     # source target expression filter aggregator joiner lookup router sorter union
    ports: list = field(default_factory=list)   # sequence update normalizer rank other
    attrs: dict = field(default_factory=dict)
    groups: list = field(default_factory=list)  # router/union groups: {name, expr, kind: input|output|default, order}
    type_name: str = ""

    def port(self, name):
        return next((p for p in self.ports if p.name == name), None)


@dataclass
class Edge:
    src: str
    sport: str
    dst: str
    dport: str


@dataclass
class Mapping:
    name: str
    nodes: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)
    params: list = field(default_factory=list)


def to_dict(m: Mapping) -> dict:
    """serialisable form - also the documented hand-written input format (see README)"""
    return {
        "name": m.name,
        "nodes": [{"name": n.name, "kind": n.kind, "type_name": n.type_name, "attrs": n.attrs, "groups": n.groups,
                   "ports": [{"name": p.name, "direction": p.direction, "expr": p.expr, "group": p.group,
                              "attrs": p.attrs} for p in n.ports]} for n in m.nodes.values()],
        "edges": [{"src": e.src, "sport": e.sport, "dst": e.dst, "dport": e.dport} for e in m.edges],
    }


def from_dict(d: dict) -> Mapping:
    m = Mapping(d["name"])
    for n in d["nodes"]:
        node = Node(n["name"], n["kind"], [Port(**p) for p in n.get("ports", [])], n.get("attrs", {}),
                    n.get("groups", []), n.get("type_name", ""))
        m.nodes[node.name] = node
    m.edges = [Edge(**e) for e in d.get("edges", [])]
    return m
