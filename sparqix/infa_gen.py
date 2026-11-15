"""Mapping IR -> PySpark.  One function per mapping:  def m_name(spark, params=None, write=True)"""
from __future__ import annotations

import re

from .core import Report, Unsupported, format_chain, strip_parens, cm, tq, dq
from .infa_expr import InfaExpr, strip_q
from .infa_ir import Edge, Mapping, Node

HEADER = '''from pyspark.sql import functions as F, Window


def _unsupported(df, name, kind):
    """placeholder for transformations sparqix could not convert - fails loudly instead of returning wrong data"""
    raise NotImplementedError(f"transformation {name} ({kind}) was not converted - implement it by hand")
'''
JOIN_TYPES = {"normal join": "inner", "master outer join": "right", "detail outer join": "left", "full outer join": "full"}


def safe(s):
    return re.sub(r"\W", "_", s)


class Generator:
    def __init__(self, m: Mapping, report: Report, sql_dialect="generic"):
        self.m, self.r, self.dialect = m, report, sql_dialect
        self.ex = InfaExpr(report)
        self.var: dict = {}          # (node, group) -> python variable
        self.lineage: dict = {}      # var -> set of vars whose columns it contains
        self.lines: list = []
        self.seq_noted = False

    # ---------------------------------------------------------------- helpers
    def emit(self, text=""):
        for l in text.split("\n"):
            self.lines.append(("    " + l) if l else "")

    def assign(self, var, code):
        code = format_chain(code)
        self.emit(f"{var} = {code}")

    def expr(self, text, where):
        try:
            return self.ex.code(text, where)
        except Unsupported as e:
            self.r.todo(where, str(e))
            msg = str(e).replace("'", "")[:90]
            return f"F.raise_error({f'sparqix TODO {where}: {msg}'!r})"

    def upstream_key(self, e: Edge):
        src = self.m.nodes.get(e.src)
        group = None
        if src is not None and src.kind == "router":
            p = src.port(e.sport)
            group = p.group if p else None
        return (e.src, group)

    def seq_expr(self, node):
        a = node.attrs
        start, inc = a.get("Start Value", "1") or "1", a.get("Increment By", "1") or "1"
        if not self.seq_noted:
            self.r.warn(node.name, "Sequence Generator -> row_number() over a single global ordering: values are not "
                                   "persisted between runs (Informatica keeps a counter); seed with the current max if needed")
            self.seq_noted = True
        return f"((F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1) * {inc} + {start})"

    def resolve_input(self, node: Node, edges=None):
        """DataFrame expression holding exactly the ports connected into `node`"""
        edges = [e for e in (edges if edges is not None else self.m.edges) if e.dst == node.name]
        seq = [e for e in edges if self.m.nodes[e.src].kind == "sequence"]
        real = [e for e in edges if e not in seq]
        keys = []
        for e in real:
            k = self.upstream_key(e)
            if k not in keys:
                keys.append(k)
        if not keys:
            if not seq:
                raise Unsupported("transformation has no incoming connections")
            raise Unsupported("only a Sequence Generator feeds this transformation")
        base = keys[0]
        if len(keys) > 1:
            cands = [k for k in keys if all(o == k or self.var[o] in self.lineage.get(self.var[k], set()) for o in keys)]
            if not cands:
                raise Unsupported("ports arrive from several unrelated upstream transformations - model it as a Joiner/Union")
            base = cands[0]
        bvar = self.var[base]
        cols, seen = [], set()
        for e in real:
            part = f"F.col({e.sport!r})" + (f".alias({e.dport!r})" if e.sport != e.dport else "")
            if e.dport not in seen:
                cols.append(part)
                seen.add(e.dport)
        code = f"{bvar}.select({', '.join(cols)})" if cols else bvar
        for e in seq:
            code += f".withColumn({e.dport!r}, {self.seq_expr(self.m.nodes[e.src])})"
        return code

    def out_cols(self, node):
        return [p.name for p in node.ports if p.direction in ("out", "inout")]

    # ---------------------------------------------------------------- per node
    def gen_node(self, node: Node):
        v = f"df_{safe(node.name)}"
        if node.kind == "sequence":
            return                       # applied as a column wherever NEXTVAL is connected
        self.var[(node.name, None)] = v
        self.emit(f"# --- {cm(node.name)} ({cm(node.type_name or node.kind)})")
        try:
            getattr(self, "n_" + node.kind, self.n_other)(node, v)
        except Unsupported as e:
            self.r.todo(node.name, str(e))
            self.emit(f"# TODO(sparqix): {cm(e)}")
            self.emit(f"{v} = _unsupported(None, {node.name!r}, {node.type_name or node.kind!r})")
        self.lineage.setdefault(v, set())
        self.emit()

    def n_source(self, node, v):
        a, names = node.attrs, [p.name for p in node.ports]
        if "flat" in (a.get("databasetype") or "").lower() or "flatfile" in a:
            ff = a.get("flatfile", {})
            sep = (ff.get("delimiters") or ",").replace("\\", "") or ","
            skip = int(ff.get("skiprows") or 0)
            self.r.warn(node.name, f"flat file path is a session property in Informatica: set the real path for {a.get('name')}")
            code = f"spark.read.csv('/path/to/{a.get('name')}', sep={sep!r}, header={skip > 0})"
            if skip == 0 and names:
                code += f".toDF({', '.join(map(repr, names))})"
            self.assign(v, code)
        else:
            db = a.get("dbdname") or a.get("ownername")
            tbl = f"{db}.{a.get('name')}" if db else a.get("name")
            self.assign(v, f"spark.table({tbl!r})")
            self.r.note(f"source {tbl} is read with spark.table(): register/ingest the table (JDBC, catalog or view) first")

    def n_sq(self, node, v):
        a = node.attrs
        sql = (a.get("Sql Query") or "").strip()
        if sql:
            from .sql_conv import convert_sql
            conv = convert_sql(sql, self.r, dialect=self.dialect, out="sql").strip().rstrip(";")
            body = conv.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
            self.r.todo(node.name, "Source Qualifier SQL override: converted to spark.sql(...) - register the tables it reads as views")
            self.emit(f'{v} = spark.sql({tq(body)})')
            return
        edges = [e for e in self.m.edges if e.dst == node.name]
        if len({e.src for e in edges}) > 1:
            raise Unsupported("Source Qualifier over several sources (homogeneous join / User Defined Join)")
        code = self.resolve_input(node)
        if (a.get("Select Distinct") or "").upper() == "YES":
            code += ".distinct()"
        flt = (a.get("Source Filter") or "").strip()
        if flt:
            self.r.warn(node.name, "Source Filter is SQL in the source's dialect; passed to filter() as a Spark SQL string")
            code += f".filter({flt!r})"
        if (a.get("User Defined Join") or "").strip():
            raise Unsupported("User Defined Join")
        self.assign(v, code)

    def n_expression(self, node, v):
        code = self.resolve_input(node)
        names = {p.name for p in node.ports}
        for p in node.ports:
            if p.direction == "in" or not p.expr:
                continue
            if p.direction == "var" and re.search(rf"\b{re.escape(p.name)}\b", p.expr):
                raise Unsupported(f"variable port {p.name} refers to itself (row-to-row state, e.g. running totals) - needs a Window")
            code += f".withColumn({p.name!r}, {strip_parens(self.expr(p.expr, node.name + '.' + p.name))})"
        outs = [n for n in self.out_cols(node)]
        if outs:
            code += f".select({', '.join(repr(o) for o in outs)})"
        self.assign(v, code)

    def n_filter(self, node, v):
        code = self.resolve_input(node)
        cond = (node.attrs.get("Filter Condition") or "TRUE").strip()
        if cond.upper() != "TRUE":
            code += f".filter({strip_parens(self.expr(cond, node.name + ' (filter)'))})"
        outs = self.out_cols(node)
        if outs:
            code += f".select({', '.join(repr(o) for o in outs)})"
        self.assign(v, code)

    def n_aggregator(self, node, v):
        code = self.resolve_input(node)
        keys = [p.name for p in node.ports if (p.attrs.get("exprtype") or "").upper() == "GROUPBY"]
        aggs = []
        for p in node.ports:
            if p.name in keys or p.direction in ("in", "var"):
                continue
            if p.direction == "var":
                raise Unsupported("variable ports in an Aggregator")
            if p.expr:
                aggs.append(f"{strip_parens(self.expr(p.expr, node.name + '.' + p.name))}.alias({p.name!r})")
            elif p.direction in ("out", "inout"):
                self.r.warn(node.name, f"port {p.name} is neither grouped nor aggregated: Informatica returns the last row's value -> F.last()")
                aggs.append(f"F.last({p.name!r}).alias({p.name!r})")
        grp = f".groupBy({', '.join(repr(k) for k in keys)})" if keys else ""
        self.assign(v, f"{code}{grp}.agg({', '.join(aggs)})" if keys else f"{code}.agg({', '.join(aggs)})")

    def n_joiner(self, node, v):
        edges = [e for e in self.m.edges if e.dst == node.name]
        master = [e for e in edges if (node.port(e.dport) and node.port(e.dport).attrs.get("master"))]
        detail = [e for e in edges if e not in master]
        if not master or not detail:
            raise Unsupported("could not tell master from detail ports")
        sides = []
        for es in (master, detail):
            keys = {self.upstream_key(e) for e in es}
            if len(keys) != 1:
                raise Unsupported("Joiner input side fed by several upstream transformations")
            k = next(iter(keys))
            cols = ", ".join(f"F.col({e.sport!r})" + (f".alias({e.dport!r})" if e.sport != e.dport else "") for e in es)
            sides.append(f"{self.var[k]}.select({cols})")
        cond = strip_parens(self.expr(node.attrs.get("Join Condition") or "", node.name + " (join condition)"))
        jt = JOIN_TYPES.get((node.attrs.get("Join Type") or "Normal Join").lower(), "inner")
        self.emit(f"{v}_master = {sides[0]}")
        self.emit(f"{v}_detail = {sides[1]}")
        code = f"{v}_master.join({v}_detail, {cond}, {jt!r})"
        outs = self.out_cols(node)
        if outs:
            code += f".select({', '.join(repr(o) for o in outs)})"
        self.assign(v, code)

    def n_lookup(self, node, v):
        a = node.attrs
        table = (a.get("Lookup table name") or "").strip()
        override = (a.get("Lookup Sql Override") or "").strip()
        if not table and not override:
            raise Unsupported("lookup source unknown")
        in_ports = {p.name for p in node.ports if p.direction == "in"}
        lk_ports = {p.name for p in node.ports if p.direction == "lookup"}
        returns = [p.name for p in node.ports if p.direction == "lookup" and p.attrs.get("lookup_return")]
        cond = (a.get("Lookup condition") or "").strip()
        parts = re.split(r"(?i)\s+AND\s+", cond)
        pairs = []
        for part in parts:
            m = re.match(r"\s*(\w+)\s*(=|>=|<=|<>|!=|>|<)\s*(\w+)\s*$", part)
            if not m:
                raise Unsupported(f"lookup condition `{cond[:50]}` not understood")
            l, op, r = m.groups()
            if l in lk_ports and r in in_ports:
                pairs.append((l, op, r))
            elif r in lk_ports and l in in_ports:
                flip = {">": "<", "<": ">", ">=": "<=", "<=": ">="}.get(op, op)
                pairs.append((r, flip, l))
            else:
                raise Unsupported(f"lookup condition `{part}` does not compare a lookup column with an input port")
        # input rows: keep every column of the upstream df, add the IN_ ports as aliases
        edges = [e for e in self.m.edges if e.dst == node.name]
        keys = []
        for e in edges:
            k = self.upstream_key(e)
            if k not in keys:
                keys.append(k)
        if len(keys) != 1:
            raise Unsupported("lookup input ports arrive from several upstream transformations")
        up = self.var[keys[0]]
        code = up
        for e in edges:
            code += f".withColumn({e.dport!r}, F.col({e.sport!r}))"
        self.emit(f"{v}_in = {format_chain(code)}")
        if override:
            from .sql_conv import convert_sql
            sql = convert_sql(override, self.r, dialect=self.dialect, out="sql").strip().rstrip(";")
            self.r.todo(node.name, "Lookup SQL override converted to spark.sql(...) - register the tables it reads")
            self.emit(f'{v}_lkp = spark.sql({tq(sql)})')
        else:
            self.emit(f"{v}_lkp = spark.table({table!r})")
        policy = (a.get("Lookup policy on multiple match") or "Use Any Value").lower()
        eq_cols = [l for l, op, r in pairs if op == "="]
        if "all" not in policy and eq_cols:
            self.r.warn(node.name, "lookup returns one match: lookup table de-duplicated on the key (arbitrary row, Informatica default 'first value')")
            self.emit(f"{v}_lkp = {v}_lkp.dropDuplicates({eq_cols!r})")
        elif "all" in policy:
            self.r.warn(node.name, "'Use All Values' lookup can multiply rows")
        cond_code = " & ".join(f"(F.col('i.{r}') {'==' if op == '=' else ('!=' if op in ('<>', '!=') else op)} F.col('l.{l}'))" for l, op, r in pairs)
        ret = ", ".join(f"F.col('l.{c}').alias({c!r})" for c in returns)
        self.assign(v, f"{v}_in.alias('i').join({v}_lkp.alias('l'), {strip_parens(cond_code) if len(pairs) == 1 else cond_code}, 'left').select('i.*', {ret})" if ret
                    else f"{v}_in.alias('i').join({v}_lkp.alias('l'), {cond_code}, 'left').select('i.*')")
        self.lineage[v] = {up} | self.lineage.get(up, set())

    def n_router(self, node, v):
        base = self.resolve_input(node)
        self.emit(f"{v} = {format_chain(base)}")
        in_names = {p.name for p in node.ports if p.direction == "in"}
        conds = []
        for g in sorted(node.groups, key=lambda g: g["order"]):
            if g["kind"] == "input":
                continue
            gname = g["name"]
            gvar = f"{v}__{safe(gname)}"
            if g["kind"] == "default":
                cond = "~(" + " | ".join(f"F.coalesce({c}, F.lit(False))" for c in conds) + ")" if conds else "F.lit(True)"
            else:
                cond = strip_parens(self.expr(g["expr"] or "TRUE", f"{node.name}.{gname}"))
                conds.append(cond)
            cols = []
            for p in node.ports:
                if p.group == gname and p.direction in ("out", "inout"):
                    src = p.name if p.name in in_names else re.sub(r"\d+$", "", p.name)
                    if src not in in_names:
                        src = p.name
                    cols.append(f"F.col({src!r})" + (f".alias({p.name!r})" if src != p.name else ""))
            code = f"{v}.filter({cond})" + (f".select({', '.join(cols)})" if cols else "")
            self.assign(gvar, code)
            self.var[(node.name, gname)] = gvar
            self.lineage.setdefault(gvar, set())

    def n_sorter(self, node, v):
        code = self.resolve_input(node)
        if (node.attrs.get("Distinct") or "").upper() == "YES":
            code += ".distinct()"
        keys = [p for p in node.ports if p.attrs.get("sortkey")]
        if keys:
            code += ".orderBy(" + ", ".join(
                f"F.col({p.name!r}).{'desc' if (p.attrs.get('sortdir') or '').upper().startswith('DESC') else 'asc'}()" for p in keys) + ")"
        self.assign(v, code)

    def n_union(self, node, v):
        out_ports = [p for p in node.ports if p.direction == "out"]
        in_groups = [g["name"] for g in sorted(node.groups, key=lambda g: g["order"]) if g["kind"] == "input"]
        if not out_ports or len(in_groups) < 2:
            raise Unsupported("union group layout not recognised")
        parts = []
        for g in in_groups:
            gports = [p.name for p in node.ports if p.group == g and p.direction != "out"]
            es = [e for e in self.m.edges if e.dst == node.name and e.dport in gports]
            keys = {self.upstream_key(e) for e in es}
            if len(keys) != 1 or len(gports) != len(out_ports):
                raise Unsupported(f"union input group {g} is not fully/uniformly connected")
            up = self.var[next(iter(keys))]
            by_port = {e.dport: e.sport for e in es}
            cols = [f"F.col({by_port[ip]!r}).alias({op.name!r})" for ip, op in zip(gports, out_ports) if ip in by_port]
            parts.append(f"{up}.select({', '.join(cols)})")
        code = parts[0] + "".join(f".unionByName({p})" for p in parts[1:])
        self.assign(v, code)

    def n_sequence(self, node, v):
        self.emit("# (Sequence Generator: applied as a column wherever NEXTVAL is connected)")
        self.var.pop((node.name, None), None)

    def n_update(self, node, v):
        self.r.todo(node.name, "Update Strategy: row flags (DD_INSERT/DD_UPDATE/DD_DELETE) need a Delta/Iceberg MERGE; rows are passed through unchanged")
        self.emit("# TODO(sparqix): Update Strategy not converted (use MERGE INTO on a Delta/Iceberg target)")
        self.assign(v, self.resolve_input(node))

    def n_other(self, node, v):
        raise Unsupported(f"{node.type_name or node.kind} transformation has no automatic conversion")

    def n_target(self, node, v):
        code = self.resolve_input(node)
        names = [p.name for p in node.ports]
        a = node.attrs
        order = [p for p in names if any(e.dst == node.name and e.dport == p for e in self.m.edges)]
        if order:
            code += f".select({', '.join(map(repr, order))})"
        self.emit(f"{v} = {format_chain(code)}")
        tbl = a.get("name")
        if "flat" in (a.get("databasetype") or "").lower():
            self.r.warn(node.name, f"flat file target path is a session property: set the real path for {tbl}")
            self.emit("if write:")
            self.emit(f"    {v}.write.mode('append').csv('/path/to/{tbl}', header=True)")
        else:
            self.r.warn(node.name, f"target {tbl}: written with mode('append') - Informatica 'Truncate target table' is a session setting (use 'overwrite')")
            self.emit("if write:")
            self.emit(f"    {v}.write.mode('append').saveAsTable({tbl!r})")

    # ---------------------------------------------------------------- driver
    def order(self):
        deps = {n: set() for n in self.m.nodes}
        for e in self.m.edges:
            if e.src in deps and e.dst in deps and e.src != e.dst:
                deps[e.dst].add(e.src)
        out, done = [], set()
        while len(out) < len(deps):
            ready = [n for n in self.m.nodes if n not in done and deps[n] <= done]
            if not ready:
                rest = [n for n in self.m.nodes if n not in done]
                self.r.todo(self.m.name, f"cycle in mapping graph involving {rest}")
                break
            for n in ready:
                out.append(n)
                done.add(n)
        return out

    def generate(self) -> str:
        m = self.m
        self.emit_header = True
        fn = f"m_{safe(m.name)}" if not safe(m.name).startswith("m_") else safe(m.name)
        self.lines.append(f"def {fn}(spark, params=None, write=True):")
        self.emit('"""Converted from Informatica mapping %s"""' % dq(m.name))
        self.emit("params = params or {}")
        if m.params:
            self.emit(f"# mapping parameters/variables: {cm(', '.join(m.params))} (read via params['NAME'])")
        self.emit()
        targets = []
        for name in self.order():
            node = m.nodes[name]
            self.gen_node(node)
            if node.kind == "target":
                targets.append(name)
        self.emit("return {" + ", ".join(f"{t!r}: df_{safe(t)}" for t in targets if (t, None) in self.var) + "}")
        return "\n".join(self.lines)


def generate_mappings(mappings, report: Report, sql_dialect="generic") -> str:
    out = [HEADER]
    for m in mappings:
        out.append("")
        out.append(Generator(m, report, sql_dialect).generate())
        out.append("")
    return "\n".join(out).rstrip() + "\n"
