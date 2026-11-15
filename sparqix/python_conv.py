"""Plain Python (lists / dicts / files / comprehensions) -> PySpark RDD code.

Scope: data-processing idioms over collections:
  sources     [..] / range / open().readlines() / csv.DictReader
  transforms  comprehensions, map/filter/sorted/set/list, nested loops (flatMap)
  actions     sum len min max any all Counter dict(...) {k: v for ...}
  loops       accumulate (`acc += ..`), counting (`d[k] += 1`), `lst.append(..)`,
              anything else falls back to rdd.toLocalIterator() (correct, but not distributed)
"""
from __future__ import annotations

import ast
import re

from .core import Report, Unsupported, cm


def _p(code: str) -> ast.expr:
    return ast.parse(code, mode="eval").body


class PyConv:
    def __init__(self, src: str, report: Report):
        self.src = src
        self.r = report
        self.rdds: set[str] = set()
        self.rdd_nodes: set[int] = set()
        self.empty: set[str] = set()
        self.uses_sc = False
        self.uses_spark = False
        self.line = 0
        self.closure_noted = False

    # ------------------------------------------------------------ helpers
    def is_rdd(self, n) -> bool:
        return (isinstance(n, ast.Name) and n.id in self.rdds) or id(n) in self.rdd_nodes

    def mk(self, code: str, rdd=True) -> ast.expr:
        n = _p(code)
        if rdd:
            self.rdd_nodes.add(id(n))
        return n

    def lam(self, target, body: str) -> str:
        if isinstance(target, ast.Name):
            return f"lambda {target.id}: {body}"
        if isinstance(target, ast.Tuple) and all(isinstance(e, ast.Name) for e in target.elts):
            names = ", ".join(e.id for e in target.elts)
            return f"lambda _t: (lambda {names}: {body})(*_t)"
        raise Unsupported("unsupported loop target")

    def note_closure(self):
        if not self.closure_noted:
            self.r.note("lambdas are shipped to executors: they see a *copy* of captured variables; "
                        "mutating driver-side lists/dicts inside them will not work")
            self.closure_noted = True

    # ------------------------------------------------------------ expression rewriting
    def expr(self, node):
        self.uses_sc |= bool(re.search(r"\bsc\b", ast.unparse(node)))
        t = self
        class X(ast.NodeTransformer):
            def visit_ListComp(s, n):
                s.generic_visit(n)
                return t.comp(n, kind="list")

            def visit_GeneratorExp(s, n):
                s.generic_visit(n)
                return t.comp(n, kind="list")

            def visit_DictComp(s, n):
                s.generic_visit(n)
                return t.dictcomp(n)

            def visit_Call(s, n):
                s.generic_visit(n)
                return t.call(n)

        node = X().visit(node)
        ast.fix_missing_locations(node)
        return node

    def comp(self, n, kind):
        gens = n.generators
        if not self.is_rdd(gens[0].iter):
            return n
        if any(g.is_async for g in gens) or len(gens) > 2:
            raise Unsupported("comprehension with more than two `for` clauses")
        g0 = gens[0]
        base = ast.unparse(g0.iter)
        for cond in g0.ifs:
            base += f".filter({self.lam(g0.target, ast.unparse(cond))})"
        self.note_closure()
        if len(gens) == 2:
            g1 = gens[1]
            inner = f"[{ast.unparse(n.elt)} for {ast.unparse(g1.target)} in {ast.unparse(g1.iter)}" \
                    + "".join(f" if {ast.unparse(c)}" for c in g1.ifs) + "]"
            return self.mk(f"{base}.flatMap({self.lam(g0.target, inner)})")
        elt = ast.unparse(n.elt)
        if isinstance(g0.target, ast.Name) and elt == g0.target.id:
            return self.mk(base)
        return self.mk(f"{base}.map({self.lam(g0.target, elt)})")

    def dictcomp(self, n):
        g = n.generators[0]
        if not self.is_rdd(g.iter) or len(n.generators) > 1:
            return n
        base = ast.unparse(g.iter)
        for cond in g.ifs:
            base += f".filter({self.lam(g.target, ast.unparse(cond))})"
        self.note_closure()
        pair = f"({ast.unparse(n.key)}, {ast.unparse(n.value)})"
        return self.mk(f"{base}.map({self.lam(g.target, pair)}).collectAsMap()", rdd=False)

    def call(self, n):
        f = n.func
        a = n.args
        kw = {k.arg: k.value for k in n.keywords if k.arg}
        if isinstance(f, ast.Name):
            name = f.id
            if name in ("map", "filter") and len(a) == 2 and self.is_rdd(a[1]):
                return self.mk(f"{ast.unparse(a[1])}.{name}({ast.unparse(a[0])})")
            if name in ("list", "tuple") and len(a) == 1 and self.is_rdd(a[0]):
                return a[0]
            if name == "set" and len(a) == 1 and self.is_rdd(a[0]):
                return self.mk(f"{ast.unparse(a[0])}.distinct()")
            if name == "sorted" and a and self.is_rdd(a[0]):
                key = ast.unparse(kw["key"]) if "key" in kw else "lambda x: x"
                rev = ast.unparse(kw["reverse"]) if "reverse" in kw else "False"
                asc = "False" if rev == "True" else ("True" if rev == "False" else f"not {rev}")
                return self.mk(f"{ast.unparse(a[0])}.sortBy({key}, ascending={asc})")
            if name == "enumerate" and a and self.is_rdd(a[0]):
                return self.mk(f"{ast.unparse(a[0])}.zipWithIndex().map(lambda p: (p[1], p[0]))")
            if name in ("sum", "len", "max", "min", "any", "all", "Counter", "dict") and a and self.is_rdd(a[0]):
                x = ast.unparse(a[0])
                code = {
                    "sum": f"{x}.sum()", "len": f"{x}.count()",
                    "max": f"{x}.max({('key=' + ast.unparse(kw['key'])) if 'key' in kw else ''})",
                    "min": f"{x}.min({('key=' + ast.unparse(kw['key'])) if 'key' in kw else ''})",
                    "any": f"(not {x}.filter(bool).isEmpty())", "all": f"{x}.filter(lambda v: not v).isEmpty()",
                    "Counter": f"{x}.countByValue()", "dict": f"{x}.collectAsMap()",
                }[name]
                if name == "sum" and len(a) > 1:
                    raise Unsupported("sum() with a start value")
                return self.mk(code, rdd=False)
            if name in ("zip", "reversed", "next", "iter") and any(self.is_rdd(x) for x in a):
                raise Unsupported(f"{name}() over distributed data has no direct RDD equivalent")
        # any other call: RDD arguments are materialised
        n.args = [self.mk(f"{ast.unparse(x)}.collect()", rdd=False) if self.is_rdd(x) else x for x in a]
        if isinstance(f, ast.Attribute) and self.is_rdd(f.value) and f.attr not in (
                "map", "filter", "flatMap", "collect", "count", "sum", "reduce", "distinct", "take",
                "sortBy", "reduceByKey", "countByValue", "collectAsMap", "max", "min", "mean"):
            n.func = ast.Attribute(value=self.mk(f"{ast.unparse(f.value)}.collect()", rdd=False),
                                   attr=f.attr, ctx=ast.Load())
        return n

    # ------------------------------------------------------------ sources
    def source_of(self, v):
        """Return code for an RDD if `v` is a recognised collection source."""
        s = ast.unparse(v)
        m = re.fullmatch(r"open\((.+?)\)\.(?:readlines\(\)|read\(\)\.splitlines\(\)|read\(\)\.split\('\\n'\))", s)
        if m:
            self.uses_sc = True
            return f"sc.textFile({m.group(1)})"
        if isinstance(v, ast.ListComp) and len(v.generators) == 1:
            it = ast.unparse(v.generators[0].iter)
            m = re.fullmatch(r"open\((.+?)\)", it)
            if m:
                self.uses_sc = True
                tmp = ast.copy_location(ast.ListComp(elt=v.elt, generators=[ast.comprehension(
                    target=v.generators[0].target, iter=ast.Name(id="__src", ctx=ast.Load()),
                    ifs=v.generators[0].ifs, is_async=0)]), v)
                self.rdds.add("__src")
                try:
                    res = self.expr(tmp)
                finally:
                    self.rdds.discard("__src")
                return ast.unparse(res).replace("__src", f"sc.textFile({m.group(1)})")
        m = re.fullmatch(r"list\(csv\.DictReader\(open\((.+?)\)\)\)", s)
        if m:
            self.uses_spark = True
            return f"spark.read.csv({m.group(1)}, header=True).rdd.map(lambda r: r.asDict())"
        if isinstance(v, (ast.List, ast.Tuple)) or (isinstance(v, ast.Call) and isinstance(v.func, ast.Name)
                                                    and v.func.id in ("range", "list") and s.startswith(("range(", "list(range("))):
            self.uses_sc = True
            return f"sc.parallelize({s if s.startswith('list(range') is False else s[5:-1]})"
        return None

    # ------------------------------------------------------------ statements
    def stmt(self, node, indent=0):
        self.line = getattr(node, "lineno", self.line)
        pad = "    " * indent
        try:
            return self.stmt_inner(node, indent, pad)
        except Unsupported as e:
            self.r.todo(self.line, str(e))
            seg = ast.get_source_segment(self.src, node) or ast.unparse(node)
            return [f"{pad}# TODO(sparqix): {cm(e)}"] + [f"{pad}# {l}" for l in seg.splitlines()]

    def block(self, body, indent):
        out = []
        for s in body:
            out += self.stmt(s, indent)
        return out or ["    " * indent + "pass"]

    def stmt_inner(self, node, indent, pad):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            src = self.source_of(node.value)
            if src:
                self.rdds.add(name)
                return [f"{pad}{name} = {src}"]
            v = ast.unparse(node.value)
            if re.fullmatch(r"\{\}|dict\(\)|Counter\(\)|defaultdict\(.*\)|\[\]|list\(\)", v):
                self.empty.add(name)
            new = self.expr(node.value)
            if self.is_rdd(new):
                self.rdds.add(name)
            else:
                self.rdds.discard(name)
            return [f"{pad}{name} = {ast.unparse(new)}"]
        if isinstance(node, ast.With):
            return self.with_stmt(node, indent, pad)
        if isinstance(node, ast.For):
            return self.for_stmt(node, indent, pad)
        if isinstance(node, ast.If):
            test = ast.unparse(self.expr(node.test))
            out = [f"{pad}if {test}:"] + self.block(node.body, indent + 1)
            if node.orelse:
                out += [f"{pad}else:"] + self.block(node.orelse, indent + 1)
            return out
        if isinstance(node, ast.FunctionDef):
            saved = set(self.rdds)
            fn = ast.parse(ast.unparse(node)).body[0]
            fn.body = [ast.Pass()]
            head = ast.unparse(fn).splitlines()[0]
            body = self.block(node.body, indent + 1)
            self.rdds = saved
            return [""] + [pad + head] + body + [""]
        if isinstance(node, ast.Return) and node.value is not None:
            new = self.expr(node.value)
            if self.is_rdd(new) and isinstance(new, (ast.Name,)) or id(new) in self.rdd_nodes:
                return [f"{pad}return {ast.unparse(new)}.collect()"]
            return [f"{pad}return {ast.unparse(new)}"]
        if isinstance(node, (ast.Expr, ast.AugAssign, ast.Assign)):
            new = self.expr(node)
            return [pad + ast.unparse(new)]
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return [pad + ast.unparse(node)]
        return [pad + (ast.get_source_segment(self.src, node) or ast.unparse(node))]

    def with_stmt(self, node, indent, pad):
        if len(node.items) == 1 and len(node.body) == 1 and isinstance(node.body[0], ast.Assign):
            it = node.items[0]
            call = it.context_expr
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "open" \
                    and it.optional_vars is not None:
                f = it.optional_vars.id
                body = node.body[0]
                val = ast.unparse(body.value).replace(f, f"open({ast.unparse(call.args[0])})", 1) \
                    if re.search(rf"\b{f}\b", ast.unparse(body.value)) else None
                if val and isinstance(body.targets[0], ast.Name):
                    inner = ast.Assign(targets=body.targets, value=_p(val), lineno=body.lineno)
                    src = self.source_of(inner.value)
                    if src:
                        self.rdds.add(body.targets[0].id)
                        return [f"{pad}{body.targets[0].id} = {src}"]
        raise Unsupported("`with open(...)` block: only `x = f.readlines()/list(csv.DictReader(f))` bodies are converted")

    def for_stmt(self, node, indent, pad):
        if node.orelse:
            raise Unsupported("for/else")
        if not self.is_rdd(node.iter):
            # try expression rewriting of iter (e.g. for x in sorted(rdd))
            node.iter = self.expr(node.iter)
            if not self.is_rdd(node.iter):
                body = self.block(node.body, indent + 1)
                return [f"{pad}for {ast.unparse(node.target)} in {ast.unparse(node.iter)}:"] + body
        base = ast.unparse(node.iter)
        target, body = node.target, node.body
        # nested single loop -> flatMap
        while len(body) == 1 and isinstance(body[0], ast.For) and not body[0].orelse:
            inner = body[0]
            base = f"{base}.flatMap({self.lam(target, ast.unparse(inner.iter))})"
            target, body = inner.target, inner.body
        cond = None
        stmts = body
        if len(stmts) == 1 and isinstance(stmts[0], ast.If) and not stmts[0].orelse:
            cond = ast.unparse(stmts[0].test)
            stmts = stmts[0].body
        if len(stmts) == 1:
            s = stmts[0]
            src = base + (f".filter({self.lam(target, cond)})" if cond else "")
            self.note_closure()
            # accumulate:  acc += expr   |   acc = acc + expr
            acc = self.accum(s)
            if acc:
                name, expr, op = acc
                if op == "+":
                    return [f"{pad}{name} += {src}.map({self.lam(target, expr)}).sum()"]
            # counting dict: d[k] += expr | d[k] = d.get(k, 0) + expr
            cnt = self.counter(s)
            if cnt:
                d, key, val = cnt
                if d not in self.empty:
                    self.r.warn(self.line, f"`{d}` is rebuilt from the loop alone - it is assumed to be empty before the loop")
                pair = f"({key}, {val})"
                return [f"{pad}{d} = {src}.map({self.lam(target, pair)}).reduceByKey(lambda a, b: a + b).collectAsMap()"]
            # append
            if isinstance(s, ast.Expr) and isinstance(s.value, ast.Call) and isinstance(s.value.func, ast.Attribute) \
                    and s.value.func.attr == "append" and isinstance(s.value.func.value, ast.Name):
                lst = s.value.func.value.id
                elt = ast.unparse(s.value.args[0])
                return [f"{pad}{lst}.extend({src}.map({self.lam(target, elt)}).collect())"]
        self.r.warn(self.line, "loop body is arbitrary Python: running on the driver via toLocalIterator() (correct but not distributed)")
        inner_pad = pad + "    "
        out = [f"{pad}for {ast.unparse(target)} in {base}.toLocalIterator():"]
        return out + self.block(body, indent + 1)

    def accum(self, s):
        if isinstance(s, ast.AugAssign) and isinstance(s.target, ast.Name) and isinstance(s.op, ast.Add):
            return s.target.id, ast.unparse(s.value), "+"
        if isinstance(s, ast.Assign) and len(s.targets) == 1 and isinstance(s.targets[0], ast.Name) \
                and isinstance(s.value, ast.BinOp) and isinstance(s.value.op, ast.Add) \
                and isinstance(s.value.left, ast.Name) and s.value.left.id == s.targets[0].id:
            return s.targets[0].id, ast.unparse(s.value.right), "+"
        return None

    def counter(self, s):
        if isinstance(s, ast.AugAssign) and isinstance(s.target, ast.Subscript) and isinstance(s.op, ast.Add) \
                and isinstance(s.target.value, ast.Name):
            return s.target.value.id, ast.unparse(s.target.slice), ast.unparse(s.value)
        if isinstance(s, ast.Assign) and len(s.targets) == 1 and isinstance(s.targets[0], ast.Subscript):
            t = s.targets[0]
            if isinstance(t.value, ast.Name) and isinstance(s.value, ast.BinOp) and isinstance(s.value.op, ast.Add):
                l = s.value.left
                if isinstance(l, ast.Call) and isinstance(l.func, ast.Attribute) and l.func.attr == "get" \
                        and isinstance(l.func.value, ast.Name) and l.func.value.id == t.value.id \
                        and ast.unparse(l.args[0]) == ast.unparse(t.slice):
                    return t.value.id, ast.unparse(t.slice), ast.unparse(s.value.right)
        return None

    # ------------------------------------------------------------ driver
    def run(self) -> str:
        tree = ast.parse(self.src)
        body = []
        for s in tree.body:
            body += self.stmt(s, 0)
        text = "\n".join(body).strip("\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        head = ["from pyspark.sql import SparkSession", "",
                'spark = SparkSession.builder.appName("converted").getOrCreate()']
        if re.search(r"\bsc\b", text):
            head.append("sc = spark.sparkContext")
        return "\n".join(head) + "\n\n" + text + "\n"


def convert_python(source: str, report: Report) -> str:
    return PyConv(source, report).run()
