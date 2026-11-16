"""pandas -> PySpark DataFrame translator.

Works on the AST. Every expression is evaluated to a typed value `V`:

    df   a Spark DataFrame expression
    col  a Spark Column expression (a pandas Series bound to a DataFrame)
    gb   a pending groupby
    py   ordinary Python (literals, names, lists ...)

Anything without a safe equivalent raises `Unsupported`; the statement is then
emitted as a commented-out TODO, so the output always parses and nothing is
silently converted wrongly.
"""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass, replace

from .core import Report, Unsupported, format_chain, strip_parens, cm, tq

HEADER_SESSION = 'spark = SparkSession.builder.appName("converted").getOrCreate()'

DTYPES = {
    "int": "int", "int8": "tinyint", "int16": "smallint", "int32": "int", "int64": "bigint",
    "Int64": "bigint", "float": "double", "float32": "float", "float64": "double",
    "str": "string", "string": "string", "object": "string", "category": "string",
    "bool": "boolean", "boolean": "boolean", "datetime64[ns]": "timestamp", "datetime": "timestamp",
}
HOW = {"inner": "inner", "left": "left", "right": "right", "outer": "outer", "cross": "cross"}
AGG = {"sum": "sum", "mean": "avg", "avg": "avg", "min": "min", "max": "max", "count": "count",
       "std": "stddev", "var": "variance", "median": "median", "nunique": "countDistinct",
       "first": "first", "last": "last", "size": "count"}
CMP = {ast.Eq: "==", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}
BIN = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/", ast.Mod: "%",
       ast.BitAnd: "&", ast.BitOr: "|"}
NP_FUNCS = {"log": "log", "log10": "log10", "log2": "log2", "log1p": "log1p", "exp": "exp",
            "sqrt": "sqrt", "abs": "abs", "floor": "floor", "ceil": "ceil", "sin": "sin",
            "cos": "cos", "tan": "tan", "sign": "signum"}
STR_SIMPLE = {"upper": "upper", "lower": "lower", "strip": "trim", "lstrip": "ltrim",
              "rstrip": "rtrim", "len": "length", "title": "initcap"}
DT_SIMPLE = {"year": "year", "month": "month", "day": "dayofmonth", "hour": "hour",
             "minute": "minute", "second": "second", "dayofyear": "dayofyear",
             "quarter": "quarter", "week": "weekofyear", "weekofyear": "weekofyear",
             "date": "to_date"}
ORDER_KEEP = {"assign", "query", "fillna", "astype", "dropna", "head", "limit", "copy", "reset_index"}
ROLL_FN = {"sum": "sum", "mean": "avg", "min": "min", "max": "max", "std": "stddev", "var": "variance"}
DF_PROPS = {"columns", "shape", "dtypes"}
FMT = [("%Y", "yyyy"), ("%y", "yy"), ("%m", "MM"), ("%d", "dd"), ("%H", "HH"), ("%M", "mm"),
       ("%S", "ss"), ("%B", "MMMM"), ("%b", "MMM"), ("%A", "EEEE"), ("%a", "EEE"),
       ("%p", "a"), ("%I", "hh"), ("%j", "DDD"), ("%f", "SSSSSS")]


@dataclass
class V:
    kind: str                       # df | col | gb | py | accessor
    code: str
    df: str | None = None           # owning DataFrame (col, gb)
    keys: list | None = None        # groupby keys (python reprs)
    sel: list | None = None         # groupby selected columns (names)
    boolean: bool = False
    isstr: bool = False
    vcol: str | None = None         # name of the value column of a Series-like result
    as_index: bool = True
    attr: str | None = None         # accessor name
    order: str | None = None        # df: Spark sort expressions of the last sort_values (used as the window ordering)
    part: list | None = None        # col: groupby keys (python reprs) the Series is partitioned by (groupby.transform / shift / cumsum ...)
    win: tuple | None = None        # roll: (size or None for expanding, min_periods or None)


def _lit(node):
    try:
        return ast.literal_eval(node)
    except Exception:
        raise Unsupported(f"expected a literal, got `{ast.unparse(node)}`")


def _strfmt(fmt: str) -> str:
    for a, b in FMT:
        fmt = fmt.replace(a, b)
    return fmt


class Translator:
    def __init__(self, report: Report):
        self.r = report
        self.vars: dict[str, V] = {}
        self.line = 0
        self.imports: set[str] = set()
        self.needs_pandas = False
        self.uses_window = False
        self.uses_types = False
        self.bq_clients: set[str] = set()
        self.strings: dict[str, str] = {}
        self.pre_lines: list[str] = []
        self.bq_loaded: set[str] = set()
        self.warned_order = False
        self.warned_part = False

    # ------------------------------------------------------------ helpers
    def todo(self, msg):
        raise Unsupported(msg)

    def warn(self, msg):
        self.r.warn(self.line, msg)

    def col(self, v: V) -> str:
        if v.kind == "col":
            return v.code
        if v.kind == "py":
            return f"F.lit({v.code})"
        raise Unsupported("expected a column expression")

    def args(self, call):
        return call.args, {k.arg: k.value for k in call.keywords if k.arg}

    def names(self, node) -> list:
        """column-name argument -> list of python reprs"""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return [repr(node.value)]
        if isinstance(node, (ast.List, ast.Tuple)):
            return [repr(_lit(e)) for e in node.elts]
        v = self.visit(node)
        if v.kind == "py":
            return [f"*{v.code}"]
        raise Unsupported("column names must be literals")

    def py_list(self, node) -> str:
        return ast.unparse(node)

    def bind_lambda(self, lam, df_code):
        if not (isinstance(lam, ast.Lambda) and len(lam.args.args) == 1):
            raise Unsupported("expected a single-argument lambda")
        name = lam.args.args[0].arg
        saved = self.vars.get(name)
        self.vars[name] = V("df", df_code)
        try:
            return self.visit(lam.body)
        finally:
            if saved is None:
                del self.vars[name]
            else:
                self.vars[name] = saved

    # ------------------------------------------------------------ dispatch
    def visit(self, n) -> V:
        m = getattr(self, "v_" + type(n).__name__, None)
        if m:
            return m(n)
        return self.fallback(n)

    def fallback(self, n) -> V:
        text = ast.unparse(n)
        for name in self.bq_clients:
            if re.search(rf"\b{re.escape(name)}\b", text):
                raise Unsupported(f"BigQuery client call `{text[:50]}` has no automatic conversion (only query/load_table_from_dataframe are supported)")
        for name, v in self.vars.items():
            if v.kind != "py" and re.search(rf"\b{re.escape(name)}\b", text):
                raise Unsupported(f"`{text[:60]}` mixes Spark objects into plain Python")
        return V("py", text)

    def v_Constant(self, n):
        return V("py", repr(n.value), isstr=isinstance(n.value, str))

    def v_Name(self, n):
        if n.id in self.vars:
            return self.vars[n.id]
        return V("py", n.id)

    def v_Tuple(self, n):
        return self.fallback(n)

    v_List = v_Dict = v_JoinedStr = v_Tuple

    def v_UnaryOp(self, n):
        v = self.visit(n.operand)
        if isinstance(n.op, ast.Invert):
            return V("col", f"(~{self.col(v)})", df=v.df, boolean=True)
        if isinstance(n.op, ast.USub):
            return replace(v, code=f"(-{v.code})") if v.kind != "col" else V("col", f"(-{v.code})", df=v.df)
        if isinstance(n.op, ast.Not) and v.kind == "py":
            return V("py", f"(not {v.code})")
        raise Unsupported("unsupported unary operator")

    def v_BinOp(self, n):
        l, r = self.visit(n.left), self.visit(n.right)
        op = type(n.op)
        if l.kind == "py" and r.kind == "py":
            return self.fallback(n)
        df = l.df or r.df
        if op is ast.Add and (l.isstr or r.isstr):
            return V("col", f"F.concat({self.col(l)}, {self.col(r)})", df=df, isstr=True)
        if op is ast.FloorDiv:
            return V("col", f"F.floor({self.col(l)} / {self.col(r)})", df=df)
        if op is ast.Pow:
            return V("col", f"F.pow({self.col(l)}, {self.col(r)})", df=df)
        if op in BIN:
            sym = BIN[op]
            return V("col", f"({l.code} {sym} {r.code})", df=df,
                     boolean=sym in "&|" and (l.boolean or r.boolean))
        raise Unsupported("unsupported operator")

    def v_Compare(self, n):
        if len(n.ops) != 1:
            raise Unsupported("chained comparison")
        l, r = self.visit(n.left), self.visit(n.comparators[0])
        op = type(n.ops[0])
        if l.kind == "py" and r.kind == "py":
            return self.fallback(n)
        if op in CMP:
            return V("col", f"({l.code} {CMP[op]} {r.code})", df=l.df or r.df, boolean=True)
        raise Unsupported("unsupported comparison")

    def v_IfExp(self, n):
        """`a if cond else b` on columns -> F.when(cond, a).otherwise(b) (a NULL / NaN condition takes the else branch in both)"""
        t, x, y = self.visit(n.test), self.visit(n.body), self.visit(n.orelse)
        if t.kind != "col" or not t.boolean or x.kind not in ("col", "py") or y.kind not in ("col", "py") or (x.kind == "py" and y.kind == "py" and not t.df):
            return self.fallback(n)
        return V("col", f"F.when({t.code}, {self.col(x)}).otherwise({self.col(y)})", df=t.df or x.df or y.df,
                 isstr=bool(x.isstr or y.isstr))

    def v_BoolOp(self, n):
        vs = [self.visit(x) for x in n.values]
        if all(v.kind == "py" for v in vs):
            return self.fallback(n)
        if getattr(self, "row_ctx", False) and all(v.kind == "col" and v.boolean for v in vs):      # scalar `and` / `or` in a row lambda
            sym = " & " if isinstance(n.op, ast.And) else " | "
            return V("col", "(" + sym.join(v.code for v in vs) + ")", df=next((v.df for v in vs if v.df), None), boolean=True)
        raise Unsupported("use & / | (not and / or) on columns")

    def v_Attribute(self, n):
        if isinstance(n.value, ast.Name) and n.value.id in ("np", "numpy", "pd", "pandas") \
                and n.value.id not in self.vars:
            if n.attr in ("nan", "NaN", "NA", "NaT"):
                return V("py", "None")
            return V("py", f"{n.value.id}.{n.attr}")
        base = self.visit(n.value)
        if base.kind == "df":
            if n.attr in DF_PROPS:
                code = {"columns": f"{base.code}.columns", "dtypes": f"{base.code}.dtypes",
                        "shape": f"({base.code}.count(), len({base.code}.columns))"}[n.attr]
                return V("py", code)
            if n.attr in ("values", "index", "T", "iloc", "loc"):
                raise Unsupported(f"`.{n.attr}` has no Spark equivalent (Spark DataFrames have no index/positions)")
            return V("col", f"F.col({n.attr!r})", df=base.code)
        if base.kind == "col" and n.attr in ("str", "dt"):
            return V("accessor", base.code, df=base.df, attr=n.attr)
        if base.kind == "gb" and n.attr not in ("agg", "sum", "mean", "count", "size", "min", "max"):
            return replace(base, sel=[n.attr])
        if base.kind == "col" and n.attr == "values":
            raise Unsupported("`.values` -> use .collect() explicitly")
        return V("py", f"{base.code}.{n.attr}")

    def v_Subscript(self, n):
        b = n.value
        if isinstance(b, ast.Attribute) and b.attr in ("loc", "iloc", "at", "iat"):
            recv = self.visit(b.value)
            if recv.kind == "df":
                return self.loc(recv, b.attr, n.slice)
        base = self.visit(b)
        sl = n.slice
        if base.kind == "df":
            if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
                return V("col", f"F.col({sl.value!r})", df=base.code)
            if isinstance(sl, ast.List) and all(isinstance(e, ast.Constant) for e in sl.elts):
                return V("df", f"{base.code}.select({', '.join(map(repr, _lit(sl)))})")
            if isinstance(sl, ast.Slice):
                if sl.lower is None and sl.upper is not None and sl.step is None:
                    return V("df", f"{base.code}.limit({ast.unparse(sl.upper)})")
                raise Unsupported("positional slicing needs an explicit ordering column")
            idx = self.visit(sl)
            if idx.kind == "col" and idx.boolean:
                return V("df", f"{base.code}.filter({strip_parens(idx.code)})", order=base.order)       # a row filter keeps the sort order
            raise Unsupported("cannot tell what this df[...] selects")
        if base.kind == "gb":
            cols = self.names(sl)
            return replace(base, sel=[c.strip("'\"") for c in cols])
        if base.kind == "accessor" and base.attr == "str" and isinstance(sl, ast.Slice):
            lo = _lit(sl.lower) if sl.lower else 0
            hi = _lit(sl.upper) if sl.upper else None
            if lo < 0 or (hi is not None and hi < 0):
                raise Unsupported("negative string slice")
            length = (hi - lo) if hi is not None else 2 ** 31 - 1
            return V("col", f"F.substring({base.code}, {lo + 1}, {length})", df=base.df, isstr=True)
        if base.kind == "py":
            return self.fallback(n)
        raise Unsupported("unsupported subscript")

    def loc(self, recv, which, sl):
        if which != "loc":
            raise Unsupported(f".{which} is positional; Spark rows have no position")
        if isinstance(sl, ast.Tuple) and len(sl.elts) == 2:
            mask = self.visit(sl.elts[0])
            cols = self.names(sl.elts[1])
            code = recv.code
            if not (isinstance(sl.elts[0], ast.Slice)):
                if not (mask.kind == "col" and mask.boolean):
                    raise Unsupported("df.loc row selector must be a boolean condition")
                code += f".filter({strip_parens(mask.code)})"
            if len(cols) == 1 and not isinstance(sl.elts[1], (ast.List, ast.Tuple)):
                return V("col", f"F.col({cols[0]})", df=code)
            return V("df", f"{code}.select({', '.join(cols)})")
        mask = self.visit(sl)
        if mask.kind == "col" and mask.boolean:
            return V("df", f"{recv.code}.filter({strip_parens(mask.code)})", order=recv.order)
        raise Unsupported("unsupported .loc selector")

    # ------------------------------------------------------------ calls
    # ---- BigQuery client code (google-cloud-bigquery / pandas-gbq)
    def bq_sql(self, node) -> str:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name) and node.id in self.strings:
            return self.strings[node.id]
        raise Unsupported("BigQuery SQL must be a plain string literal (f-strings/dynamic SQL: build it, then convert it with --from sql --dialect bigquery)")

    def bq_run(self, sql_node, as_df):
        from .sql_conv import convert_sql_ex
        sql, tables = convert_sql_ex(self.bq_sql(sql_node), self.r, "bigquery", "sql", bq_views=True)
        sql = sql.strip().rstrip(";")
        for t in tables:
            parts = t.split(".")
            view = "_".join(parts[-2:]).replace("-", "_")
            if view not in self.bq_loaded:
                self.bq_loaded.add(view)
                self.pre_lines.append(f"spark.read.format('bigquery').load({t!r}).createOrReplaceTempView({view!r})")
        if tables:
            self.r.note("BigQuery tables are read with the spark-bigquery connector (add the connector jar / --packages) and registered as temp views")
        body = sql.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
        code = f'spark.sql({tq(body)})'
        return V("df", code) if as_df else V("py", code)

    def bq_write(self, df_node, table_node, mode):
        df = self.visit(df_node)
        if df.kind != "df":
            raise Unsupported("writing a non-DataFrame to BigQuery")
        table = _lit(table_node)
        self.warn("BigQuery write uses the indirect method: set the real temporaryGcsBucket (or use writeMethod=direct)")
        return V("py", f"{df.code}.write.format('bigquery').option('table', {table!r}).option('temporaryGcsBucket', 'YOUR_BUCKET').mode({mode!r}).save()")

    def bq_call(self, n):
        f = n.func
        if not isinstance(f, ast.Attribute):
            return None
        unp = ast.unparse(f)
        a, kw = self.args(n)
        if f.attr == "result" and isinstance(f.value, ast.Call) and getattr(f.value.func, "attr", "") == "load_table_from_dataframe":
            return self.bq_call(f.value)
        # pandas_gbq / pandas
        if unp in ("pandas_gbq.read_gbq", "pd.read_gbq", "pandas.read_gbq") and a:
            return self.bq_run(a[0], True)
        if unp in ("pandas_gbq.to_gbq", "pd.to_gbq") and len(a) >= 2:
            ie = _lit(kw["if_exists"]) if "if_exists" in kw else "fail"
            return self.bq_write(a[0], a[1], {"replace": "overwrite", "append": "append", "fail": "errorifexists"}[ie])
        if f.attr == "to_gbq" and a:
            recv = self.visit(f.value)
            if recv.kind == "df":
                ie = _lit(kw["if_exists"]) if "if_exists" in kw else "fail"
                return self.bq_write(f.value, a[0], {"replace": "overwrite", "append": "append", "fail": "errorifexists"}[ie])
        # client.query(...)...
        def query_call(node):
            """the `client.query(sql)` Call inside a chain, or None"""
            while isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "query" and isinstance(node.func.value, ast.Name) and node.func.value.id in self.bq_clients:
                    return node
                node = node.func.value
            return None
        if f.attr in ("to_dataframe", "to_arrow", "result", "query"):
            q = query_call(n)
            if q is not None and q.args:
                if f.attr in ("to_dataframe", "to_arrow"):
                    return self.bq_run(q.args[0], True)
                if f.attr in ("result", "query") and n is q or f.attr == "result":
                    if any(k.arg == "job_config" for k in q.keywords):
                        self.warn("QueryJobConfig/parameters ignored: @param markers became :param - pass values with spark.sql(sql, args={...}) (Spark 3.4+)")
                    return self.bq_run(q.args[0], False)
        if f.attr == "load_table_from_dataframe" and isinstance(f.value, ast.Name) and f.value.id in self.bq_clients and len(a) >= 2:
            mode = "append"
            jc = kw.get("job_config")
            if jc is not None and "WRITE_TRUNCATE" in ast.unparse(jc):
                mode = "overwrite"
            return self.bq_write(a[0], a[1], mode)
        return None

    ROW_STR = {"upper": "F.upper({})", "lower": "F.lower({})", "strip": "F.trim({})"}
    ROW_CAST = {"str": "string", "int": "bigint", "float": "double"}

    def v_Call(self, n):
        f = n.func
        if getattr(self, "row_ctx", False):          # inside df.apply(lambda r: ..., axis=1): r[...] are scalars, so Python's own str()/int()/float()/.upper() apply
            if isinstance(f, ast.Name) and f.id in self.ROW_CAST and len(n.args) == 1 and not n.keywords and f.id not in self.vars:
                v = self.visit(n.args[0])
                if v.kind == "col":
                    return V("col", f"{v.code}.cast({self.ROW_CAST[f.id]!r})", df=v.df, isstr=f.id == "str")
            if isinstance(f, ast.Attribute) and f.attr in self.ROW_STR and not n.args and not n.keywords:
                recv = self.visit(f.value)
                if recv.kind == "col":
                    return V("col", self.ROW_STR[f.attr].format(recv.code), df=recv.df, isstr=True)
        if self.bq_clients or "gbq" in ast.unparse(f):
            r = self.bq_call(n)
            if r is not None:
                return r
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id == "spark" and f.attr in ("sql", "table"):
            return V("df", ast.unparse(n))
        if isinstance(f, ast.Attribute):
            if isinstance(f.value, ast.Name) and f.value.id in ("pd", "np", "numpy", "pandas", "pl", "polars") \
                    and f.value.id not in self.vars:
                return self.module_call(f.attr, n, f.value.id)
            if isinstance(f.value, ast.Attribute) and f.value.attr in ("str", "dt"):
                inner = self.visit(f.value.value)
                if inner.kind == "col":
                    return self.accessor_call(inner, f.value.attr, f.attr, n)
            recv = self.visit(f.value)
            if recv.kind == "df":
                r = self.df_method(recv, f.attr, n)
                if r.kind == "df" and r.order is None and recv.order and f.attr in ORDER_KEEP:
                    r = replace(r, order=recv.order)
                return r
            if recv.kind == "gb":
                return self.gb_method(recv, f.attr, n)
            if recv.kind == "col":
                return self.col_method(recv, f.attr, n)
            if recv.kind == "roll":
                return self.roll_method(recv, f.attr, n)
            return self.fallback(n)
        if isinstance(f, ast.Name) and f.id == "len" and len(n.args) == 1:
            a = self.visit(n.args[0])
            if a.kind == "df":
                return V("py", f"{a.code}.count()")
            if a.kind == "col" and a.df:
                return V("py", f"{a.df}.count()")
        if isinstance(f, ast.Name) and f.id == "print":
            vs = [self.visit(a) for a in n.args]
            if len(vs) == 1 and vs[0].kind == "df":
                return V("py", f"{vs[0].code}.show()")
            if len(vs) == 1 and vs[0].kind == "col" and vs[0].df:
                return V("py", f"{vs[0].df}.select({vs[0].code}).show()")
        if isinstance(f, ast.Name):
            vs = [self.visit(a) for a in n.args]
            kws = [(k.arg, self.visit(k.value)) for k in n.keywords]
            if all(v.kind == "py" for v in vs) and all(v.kind == "py" for _, v in kws):
                parts = [v.code for v in vs] + [f"{k}={v.code}" if k else f"**{v.code}" for k, v in kws]
                return V("py", f"{f.id}({', '.join(parts)})")
            if f.id == "print":
                raise Unsupported("print() of a Spark column/groupby - select it from a DataFrame and call .show()")
        return self.fallback(n)

    # ---- module level pd./np. functions
    def module_call(self, name, n, mod):
        a, kw = self.args(n)
        if mod in ("np", "numpy"):
            if name == "where" and len(a) == 3:
                c, x, y = (self.visit(z) for z in a)
                return V("col", f"F.when({strip_parens(c.code)}, {self.col(x)}).otherwise({self.col(y)})",
                         df=c.df or x.df or y.df)
            if name == "select" and len(a) >= 2:
                conds, choices = a[0], a[1]
                if not (isinstance(conds, ast.List) and isinstance(choices, ast.List)):
                    raise Unsupported("np.select needs literal lists")
                code = None
                df = None
                for c, ch in zip(conds.elts, choices.elts):
                    cv, chv = self.visit(c), self.visit(ch)
                    df = df or cv.df
                    code = f"F.when({strip_parens(cv.code)}, {self.col(chv)})" if code is None \
                        else f"{code}.when({strip_parens(cv.code)}, {self.col(chv)})"
                dflt = self.col(self.visit(kw["default"])) if "default" in kw else "F.lit(None)"
                return V("col", f"{code}.otherwise({dflt})", df=df)
            if name in NP_FUNCS and a:
                v = self.visit(a[0])
                return V("col", f"F.{NP_FUNCS[name]}({self.col(v)})", df=v.df)
            if name == "round" and a:
                v = self.visit(a[0])
                d = ast.unparse(a[1]) if len(a) > 1 else "0"
                return V("col", f"F.bround({self.col(v)}, {d})", df=v.df)      # pandas/Python round ties to even; F.round rounds them up
            if name in ("isnan", "isnull") and a:
                v = self.visit(a[0])
                return V("col", f"{self.col(v)}.isNull()", df=v.df, boolean=True)
            return self.fallback(n)
        # pandas
        if name in ("read_csv", "read_table"):
            return self.read_csv(a, kw)
        if name in ("read_parquet", "read_json", "read_orc"):
            fmt = name.split("_")[1]
            path = self.visit(a[0]).code
            opts = ""
            if fmt == "json" and kw.get("lines") is not None and _lit(kw["lines"]) is False:
                opts = ", multiLine=True"
            v = V("df", f"spark.read.{fmt}({path}{opts})")
            if "columns" in kw:
                v = V("df", f"{v.code}.select({', '.join(self.names(kw['columns']))})")
            return v
        if name == "read_excel":
            raise Unsupported("Excel needs the spark-excel package (com.crealytics) or read with pandas then spark.createDataFrame")
        if name == "read_sql" or name == "read_sql_query":
            raise Unsupported("use spark.read.jdbc(...) or register the tables and call spark.sql(...)")
        if name == "concat":
            if len(a) >= 1 and isinstance(a[0], (ast.List, ast.Tuple)):
                dfs = [self.visit(e) for e in a[0].elts]
            else:
                raise Unsupported("pd.concat needs a literal list of DataFrames")
            if "axis" in kw and _lit(kw["axis"]) in (1, "columns"):
                raise Unsupported("column-wise concat has no direct Spark equivalent (use join on a key)")
            if not all(d.kind == "df" for d in dfs):
                raise Unsupported("pd.concat of non-DataFrames")
            self.warn("pd.concat -> unionByName(allowMissingColumns=True) to match pandas' column alignment")
            code = dfs[0].code
            for d in dfs[1:]:
                code += f".unionByName({d.code}, allowMissingColumns=True)"
            return V("df", code)
        if name == "merge":
            left = self.visit(a[0])
            return self.merge(left, a[1:], kw)
        if name == "to_datetime":
            v = self.visit(a[0])
            fmt = f", {_strfmt(_lit(kw['format']))!r}" if "format" in kw else ""
            return V("col", f"F.to_timestamp({self.col(v)}{fmt})", df=v.df)
        if name == "to_numeric":
            v = self.visit(a[0])
            return V("col", f"{self.col(v)}.cast('double')", df=v.df)
        if name in ("isnull", "isna", "notnull", "notna"):
            v = self.visit(a[0])
            m = "isNull" if name in ("isnull", "isna") else "isNotNull"
            return V("col", f"{self.col(v)}.{m}()", df=v.df, boolean=True)
        if name == "DataFrame":
            self.needs_pandas = True
            return V("df", f"spark.createDataFrame({ast.unparse(n)})")
        if name == "pivot_table":
            return self.pivot(self.visit(a[0]), a[1:], kw)
        raise Unsupported(f"pd.{name}() has no automatic conversion")

    def read_csv(self, a, kw):
        path = self.visit(a[0]).code
        opts = []
        header = True
        if "header" in kw:
            hv = _lit(kw["header"])
            header = hv is not None and hv is not False
        opts.append(f"header={header}")
        infer = "dtype" not in kw
        if infer:
            opts.append("inferSchema=True")
        else:
            self.warn("dtype= ignored: add an explicit schema= if you need exact types")
        if "sep" in kw or "delimiter" in kw:
            opts.append(f"sep={_lit(kw.get('sep') or kw['delimiter'])!r}")
        if "encoding" in kw:
            opts.append(f"encoding={_lit(kw['encoding'])!r}")
        if "na_values" in kw:
            nv = _lit(kw["na_values"])
            nv = nv[0] if isinstance(nv, (list, tuple)) and nv else nv
            opts.append(f"nullValue={nv!r}")
        for ignored in ("index_col", "skiprows", "parse_dates", "low_memory", "thousands", "comment"):
            if ignored in kw:
                self.warn(f"read_csv({ignored}=...) not translated")
        code = f"spark.read.csv({path}, {', '.join(opts)})"
        if "names" in kw:
            code += f".toDF({', '.join(self.names(kw['names']))})"
        if "usecols" in kw:
            code += f".select({', '.join(self.names(kw['usecols']))})"
        if "nrows" in kw:
            code += f".limit({ast.unparse(kw['nrows'])})"
        return V("df", code)

    # ---- DataFrame methods
    def merge(self, left, a, kw):
        if not a:
            if "right" not in kw:
                raise Unsupported("merge needs a right frame")
            right = self.visit(kw["right"])
        else:
            right = self.visit(a[0])
        if right.kind != "df":
            raise Unsupported("merge with a non-DataFrame")
        how = _lit(kw["how"]) if "how" in kw else "inner"
        if how == "cross":
            return V("df", f"{left.code}.crossJoin({right.code})")
        how = HOW[how]
        if "suffixes" in kw:
            self.warn("suffixes= ignored: Spark keeps duplicate column names - rename before joining")
        if "left_on" in kw and "right_on" in kw:
            lo, ro = self.names(kw["left_on"]), self.names(kw["right_on"])
            if not (re.fullmatch(r"\w+", left.code) and re.fullmatch(r"\w+", right.code)):
                raise Unsupported("left_on/right_on join needs both sides assigned to variables first")
            cond = " & ".join(f"({left.code}[{l}] == {right.code}[{r}])" for l, r in zip(lo, ro))
            cond = strip_parens(cond) if len(lo) == 1 else cond
            return V("df", f"{left.code}.join({right.code}, {cond}, {how!r})")
        on = self.names(kw["on"]) if "on" in kw else None
        if on is None:
            self.warn("merge without on= joins on common columns in pandas; specify the key explicitly")
            raise Unsupported("merge without on=")
        on_code = on[0] if len(on) == 1 else f"[{', '.join(on)}]"
        return V("df", f"{left.code}.join({right.code}, on={on_code}, how={how!r})")

    def pivot(self, recv, a, kw):
        idx = self.names(kw["index"]) if "index" in kw else []
        cols = self.names(kw["columns"])
        vals = self.names(kw["values"])
        fn = _lit(kw["aggfunc"]) if "aggfunc" in kw else "mean"
        if not isinstance(fn, str) or fn not in AGG:
            raise Unsupported("pivot_table aggfunc must be a string like 'sum'")
        aggs = ", ".join(f"F.{AGG[fn]}({v})" for v in vals)
        return V("df", f"{recv.code}.groupBy({', '.join(idx)}).pivot({cols[0]}).agg({aggs})")

    def sort(self, recv, a, kw):
        by = a[0] if a else kw.get("by")
        if by is None:
            if recv.vcol is None:
                raise Unsupported("sort_values() without `by`")
            by_names = [repr(recv.vcol)]
        else:
            by_names = self.names(by)
        asc = _lit(kw["ascending"]) if "ascending" in kw else (_lit(a[1]) if len(a) > 1 else True)
        if not isinstance(asc, (list, tuple)):
            asc = [asc] * len(by_names)
        cols = ", ".join(f"F.col({n}).{'asc' if x else 'desc'}()" for n, x in zip(by_names, asc))
        return V("df", f"{recv.code}.orderBy({cols})", vcol=recv.vcol, order=cols)

    def df_method(self, recv: V, name: str, n) -> V:
        a, kw = self.args(n)
        c = recv.code
        if name in ("head", "limit"):
            k = ast.unparse(a[0]) if a else "5"
            return V("df", f"{c}.limit({k})", vcol=recv.vcol)
        if name in ("copy", "reset_index", "rename_axis", "convert_dtypes", "infer_objects", "reindex"):
            if name == "reset_index" and "name" in kw and recv.vcol:
                return V("df", f"{c}.withColumnRenamed({recv.vcol!r}, {_lit(kw['name'])!r})")
            if name == "reset_index" and kw.get("drop") is None and recv.vcol is None:
                self.r.note("reset_index() dropped: Spark DataFrames have no index")
            return recv
        if name == "repartition":
            return V("df", f"{c}.repartition({ast.unparse(a[0])})")
        if name == "tail":
            raise Unsupported("tail() needs an ordering column (use orderBy(...desc()).limit(n))")
        if name == "sort_values":
            return self.sort(recv, a, kw)
        if name in ("nlargest", "nsmallest"):
            by = self.names(kw["columns"] if "columns" in kw else a[1])[0]
            d = "desc" if name == "nlargest" else "asc"
            return V("df", f"{c}.orderBy(F.col({by}).{d}()).limit({ast.unparse(a[0])})")
        if name == "drop":
            tgt = kw.get("columns") or kw.get("labels") or (a[0] if a else None)
            axis = _lit(kw["axis"]) if "axis" in kw else (_lit(a[1]) if len(a) > 1 else 0)
            if tgt is None or ("columns" not in kw and axis not in (1, "columns")):
                raise Unsupported("dropping rows by index label has no Spark equivalent")
            return V("df", f"{c}.drop({', '.join(self.names(tgt))})")
        if name == "drop_duplicates":
            if "keep" in kw and _lit(kw["keep"]) != "first":
                self.warn("keep= ignored: Spark dropDuplicates keeps an arbitrary row (use a window to be deterministic)")
            elif "keep" not in kw:
                self.warn("dropDuplicates keeps an arbitrary row per key, pandas keeps the first")
            sub = kw.get("subset") or (a[0] if a else None)
            return V("df", f"{c}.dropDuplicates([{', '.join(self.names(sub))}])" if sub else f"{c}.distinct()")
        if name == "dropna":
            if "axis" in kw and _lit(kw["axis"]) in (1, "columns"):
                raise Unsupported("dropna(axis=1) drops columns; compute null counts first")
            parts = []
            if "how" in kw:
                parts.append(f"how={_lit(kw['how'])!r}")
            if "thresh" in kw:
                parts.append(f"thresh={ast.unparse(kw['thresh'])}")
            if "subset" in kw:
                parts.append(f"subset=[{', '.join(self.names(kw['subset']))}]")
            return V("df", f"{c}.dropna({', '.join(parts)})")
        if name == "fillna":
            if "method" in kw:
                raise Unsupported("fillna(method=ffill/bfill) needs a window with last(ignorenulls=True)")
            val = a[0] if a else kw.get("value")
            if val is None:
                raise Unsupported("fillna without value")
            sub = f", subset=[{', '.join(self.names(kw['subset']))}]" if "subset" in kw else ""
            return V("df", f"{c}.fillna({self.visit(val).code}{sub})")
        if name == "rename":
            m = kw.get("columns") or (a[0] if a else None)
            if not isinstance(m, ast.Dict):
                raise Unsupported("rename needs a literal dict")
            code = c
            for k, v in zip(m.keys, m.values):
                code += f".withColumnRenamed({_lit(k)!r}, {_lit(v)!r})"
            return V("df", code)
        if name == "astype":
            t = a[0] if a else None
            if isinstance(t, ast.Dict):
                code = c
                for k, v in zip(t.keys, t.values):
                    code += f".withColumn({_lit(k)!r}, F.col({_lit(k)!r}).cast({self.dtype(v)!r}))"
                return V("df", code)
            raise Unsupported("df.astype(single_type) - give a {column: type} dict")
        if name == "assign":
            code = c
            for k, v in kw.items():
                val = self.bind_lambda(v, c) if isinstance(v, ast.Lambda) else self.visit(v)
                code += f".withColumn({k!r}, {strip_parens(self.col(val))})"
            return V("df", code)
        if name == "merge":
            return self.merge(recv, a, kw)
        if name == "join":
            raise Unsupported("DataFrame.join joins on the index; use merge(on=...)")
        if name == "groupby":
            keys = self.names(a[0] if a else kw["by"])
            as_index = _lit(kw["as_index"]) if "as_index" in kw else True
            return V("gb", c, df=c, keys=keys, as_index=as_index)
        if name == "query":
            q = _lit(a[0])
            if "@" in q:
                raise Unsupported("query() with @variables - build the condition with F.col instead")
            q = re.sub(r"(?<![<>!=])==", "=", q)
            q = q.replace("&", " and ").replace("|", " or ")
            q = re.sub(r"\s+", " ", q)
            return V("df", f"{c}.filter({q!r})")
        if name in ("isna", "isnull"):
            return V("df", f"{c}.select([F.col(x).isNull().alias(x) for x in {c}.columns])")
        if name == "describe":
            return V("df", f"{c}.describe()")
        if name == "sample":
            seed = f", seed={ast.unparse(kw['random_state'])}" if "random_state" in kw else ""
            if "frac" in kw:
                return V("df", f"{c}.sample(fraction={ast.unparse(kw['frac'])}{seed})")
            k = ast.unparse(kw["n"] if "n" in kw else a[0])
            return V("df", f"{c}.orderBy(F.rand({seed[7:]})).limit({k})")
        if name == "explode":
            col = self.names(a[0])[0]
            return V("df", f"{c}.withColumn({col}, F.explode({col}))")
        if name == "pivot_table":
            return self.pivot(recv, a, kw)
        if name in ("to_csv", "to_parquet", "to_json"):
            path = self.visit(a[0] if a else kw["path_or_buf"] if "path_or_buf" in kw else kw["path"]).code
            fmt = name[3:]
            self.r.note("Spark writes a *directory* of part files, not a single file (add .coalesce(1) if you need one)")
            opts = ", header=True" if fmt == "csv" else ""
            if fmt == "csv" and "sep" in kw:
                opts += f", sep={_lit(kw['sep'])!r}"
            return V("py", f"{c}.write.mode('overwrite').{fmt}({path}{opts})")
        if name in ("to_numpy", "to_dict", "to_list", "tolist", "to_records"):
            self.warn("collect() pulls all data to the driver - only safe for small results")
            return V("py", f"[r.asDict() for r in {c}.collect()]")
        if name == "apply" and len(a) == 1 and isinstance(a[0], ast.Lambda) and set(kw) == {"axis"} and _lit(kw["axis"]) in (1, "columns"):
            # df.apply(lambda r: <expression of r['x'] ...>, axis=1): a row-wise lambda over plain columns is just a column expression
            self.row_ctx = True
            try:
                body = self.bind_lambda(a[0], c)
            except Unsupported as e:
                raise Unsupported(f"DataFrame.apply(axis=1): the lambda is not a plain column expression ({e}); rewrite it as column expressions, a UDF or mapInPandas")
            finally:
                self.row_ctx = False
            if body.kind != "col":
                raise Unsupported("DataFrame.apply(axis=1): the lambda does not return a column expression; rewrite it as column expressions, a UDF or mapInPandas")
            return replace(body, df=body.df or c)
        if name in ("apply", "applymap", "iterrows", "itertuples", "iteritems", "items", "pipe", "transform"):
            raise Unsupported(f"DataFrame.{name}() is row-wise Python; rewrite as column expressions, a UDF or a pandas_udf / mapInPandas")
        if name in ("set_index", "sort_index", "loc", "iloc", "at", "iat", "stack", "unstack", "melt", "pct_change", "diff", "cumsum"):
            raise Unsupported(f"DataFrame.{name}() has no automatic conversion")
        if name == "info":
            return V("py", f"{c}.printSchema()")
        if name in ("sum", "mean", "min", "max", "count", "std"):
            return V("df", f"{c}.agg(*[F.{AGG[name]}(x).alias(x) for x in {c}.columns])")
        raise Unsupported(f"DataFrame.{name}() has no automatic conversion")

    def dtype(self, node):
        if isinstance(node, ast.Constant):
            t = node.value
        elif isinstance(node, ast.Name):
            t = node.id
        elif isinstance(node, ast.Attribute):
            t = node.attr
        else:
            raise Unsupported("dynamic dtype")
        if t not in DTYPES:
            raise Unsupported(f"unknown dtype {t!r}")
        return DTYPES[t]

    # ---- groupby
    def gb_method(self, gb: V, name: str, n) -> V:
        a, kw = self.args(n)
        keys = ", ".join(gb.keys)
        base = f"{gb.code}.groupBy({keys})"
        key_names = [k.strip("'\"") for k in gb.keys]

        def build(aggs, vcol=None):
            return V("df", f"{base}.agg({', '.join(aggs)})", vcol=vcol)

        if name in AGG and name != "size" or name == "size":
            if name == "size":
                return build(["F.count(F.lit(1)).alias('size')"], vcol="size")
            if gb.sel:
                aggs = [f"F.{AGG[name]}({c!r}).alias({c!r})" for c in gb.sel]
                return build(aggs, vcol=gb.sel[0] if len(gb.sel) == 1 else None)
            self.warn(f"{name}() over all columns: Spark names results `{name}(col)` and only covers numeric columns - alias them if needed")
            m = {"avg": "avg"}.get(AGG[name], AGG[name])
            return V("df", f"{base}.{m}()")
        if name == "agg" or name == "aggregate":
            return self.gb_agg(gb, a, kw, base)
        if name == "transform" and a and isinstance(a[0], ast.Lambda) and gb.sel and len(gb.sel) == 1:
            lam = a[0]
            if len(lam.args.args) != 1:
                raise Unsupported("expected a single-argument lambda")
            pname = lam.args.args[0].arg
            saved = self.vars.get(pname)
            self.vars[pname] = V("col", f"F.col({gb.sel[0]!r})", df=gb.df, part=gb.keys)
            try:
                res = self.visit(lam.body)
            finally:
                if saved is None:
                    del self.vars[pname]
                else:
                    self.vars[pname] = saved
            if res.kind != "col":
                raise Unsupported("transform(lambda) must return a column expression")
            return res
        if name == "transform":
            if not (gb.sel and len(gb.sel) == 1 and a and isinstance(a[0], ast.Constant) and a[0].value in AGG):
                raise Unsupported("transform needs a selected column and a string function")
            self.uses_window = True
            fn = AGG[a[0].value]
            return V("col", f"F.{fn}({gb.sel[0]!r}).over(Window.partitionBy({keys}))", df=gb.df)
        if name == "nunique":
            return build([f"F.countDistinct({c!r}).alias({c!r})" for c in (gb.sel or [])])
        if name in ("cumsum", "cumprod", "cummax", "cummin", "shift", "diff", "pct_change", "cumcount", "rolling", "expanding", "rank"):
            if name != "cumcount" and not (gb.sel and len(gb.sel) == 1):
                raise Unsupported(f"groupby().{name}() needs exactly one selected column, e.g. df.groupby('k')['v'].{name}()")
            col = f"F.col({gb.sel[0]!r})" if gb.sel else "F.lit(1)"
            if name in ("rolling", "expanding"):
                self.warn(f"groupby().{name}() returns a MultiIndex Series in pandas; the converted column is aligned with the original rows")
            return self.seq_method(V("col", col, df=gb.df, part=gb.keys), name, n)
        if name in ("head", "tail", "apply", "filter", "nth"):
            raise Unsupported(f"groupby().{name}() needs a Window spec - convert manually")
        raise Unsupported(f"groupby().{name}() has no automatic conversion")

    def gb_agg(self, gb, a, kw, base):
        aggs, vcol = [], None
        spec = a[0] if a else None

        def one(col, fn, alias):
            if not (isinstance(fn, str) and fn in AGG):
                raise Unsupported(f"aggregation {fn!r} must be one of {sorted(AGG)}")
            if fn == "size":
                return f"F.count(F.lit(1)).alias({alias!r})"
            return f"F.{AGG[fn]}({col!r}).alias({alias!r})"

        if isinstance(spec, ast.Dict):
            for k, v in zip(spec.keys, spec.values):
                col, fn = _lit(k), _lit(v)
                if isinstance(fn, (list, tuple)):
                    self.warn("multiple aggregations per column flattened to `col_fn` names")
                    aggs += [one(col, f, f"{col}_{f}") for f in fn]
                else:
                    aggs.append(one(col, fn, col))
        elif spec is not None:
            fn = _lit(spec)
            if not gb.sel:
                raise Unsupported("agg(func) without a selected column")
            fns = fn if isinstance(fn, (list, tuple)) else [fn]
            for c in gb.sel:
                for f in fns:
                    aggs.append(one(c, f, c if len(fns) == 1 else f"{c}_{f}"))
        for alias, v in kw.items():
            if isinstance(v, ast.Tuple) and len(v.elts) == 2:
                aggs.append(one(_lit(v.elts[0]), _lit(v.elts[1]), alias))
            elif isinstance(v, ast.Call) and getattr(v.func, "attr", "") == "NamedAgg":
                d = {k.arg: _lit(k.value) for k in v.keywords}
                aggs.append(one(d["column"], d["aggfunc"], alias))
            else:
                raise Unsupported("named aggregation must be (column, 'func')")
        if not aggs:
            raise Unsupported("empty aggregation")
        if len(aggs) == 1:
            m = re.search(r"alias\('([^']*)'\)", aggs[0])
            vcol = m.group(1) if m else None
        return V("df", f"{base}.agg({', '.join(aggs)})", vcol=vcol)

    # ---- Series / column methods
    def col_method(self, s: V, name: str, n) -> V:
        a, kw = self.args(n)
        c = s.code
        df = s.df

        def colv(code, **k):
            return V("col", code, df=df, **k)

        def scalar(fn):
            if not df:
                raise Unsupported("aggregation of an unbound column")
            return V("py", f"{df}.agg(F.{fn}({c})).first()[0]")

        if name in ("sum", "mean", "min", "max", "count", "std", "var", "median"):
            if name == "median":
                self.warn("median -> F.median (needs Spark 3.4+); use percentile_approx(col, 0.5) on older versions")
            return scalar(AGG[name])
        if name == "nunique":
            return scalar("countDistinct")
        if name in ("isna", "isnull"):
            return colv(f"{c}.isNull()", boolean=True)
        if name in ("notna", "notnull"):
            return colv(f"{c}.isNotNull()", boolean=True)
        if name == "isin":
            return colv(f"{c}.isin({self.py_list(a[0])})", boolean=True)
        if name == "between":
            return colv(f"{c}.between({self.visit(a[0]).code}, {self.visit(a[1]).code})", boolean=True)
        if name == "astype":
            t = self.dtype(a[0])
            return colv(f"{c}.cast({t!r})", isstr=(t == "string"))
        if name == "fillna":
            return colv(f"F.coalesce({c}, {self.col(self.visit(a[0]))})")
        if name == "round":
            return colv(f"F.bround({c}, {ast.unparse(a[0]) if a else 0})")      # ties to even, like pandas
        if name == "abs":
            return colv(f"F.abs({c})")
        if name == "clip":
            lo = kw.get("lower") or (a[0] if a else None)
            hi = kw.get("upper") or (a[1] if len(a) > 1 else None)
            code = c
            if lo is not None:
                code = f"F.greatest({code}, F.lit({ast.unparse(lo)}))"
            if hi is not None:
                code = f"F.least({code}, F.lit({ast.unparse(hi)}))"
            return colv(code)
        if name == "replace":
            if len(a) == 2:
                return colv(f"F.when({c} == {self.visit(a[0]).code}, {self.col(self.visit(a[1]))}).otherwise({c})")
            raise Unsupported("replace() needs (old, new)")
        if name == "map":
            if isinstance(a[0], ast.Dict):
                return colv(f"F.create_map(*[F.lit(x) for kv in {ast.unparse(a[0])}.items() for x in kv])[{c}]")
            return self.udf(s, a[0])
        if name == "apply":
            return self.udf(s, a[0])
        if name in ("unique", "drop_duplicates"):
            if not df:
                raise Unsupported("unique() of an unbound column")
            return V("df", f"{df}.select({c}).distinct()", vcol=None)
        if name == "value_counts":
            if not df:
                raise Unsupported("value_counts() of an unbound column")
            if "normalize" in kw:
                raise Unsupported("value_counts(normalize=True)")
            m = re.fullmatch(r"F\.col\('([^']*)'\)", c)
            if not m:
                raise Unsupported("value_counts() of a computed column - assign it to a column first")
            return V("df", f"{df}.groupBy({c}).count().orderBy(F.col('count').desc())", vcol="count")
        if name in ("tolist", "to_list"):
            self.warn("collect() pulls all data to the driver - only safe for small results")
            return V("py", f"[r[0] for r in {df}.select({c}).collect()]")
        if name in ("shift", "cumsum", "cumprod", "cummax", "cummin", "diff", "pct_change", "rolling", "expanding", "rank"):
            return self.seq_method(s, name, n)
        if name in ("ewm", "idxmax", "idxmin", "sort_values", "mode", "quantile"):
            raise Unsupported(f"Series.{name}() needs an explicit Window (ordering is not implicit in Spark)")
        raise Unsupported(f"Series.{name}() has no automatic conversion")

    # ---- order dependent Series methods -> Window functions
    def window(self, s: V, rows=None) -> str:
        self.uses_window = True
        owner = self.vars.get(s.df) if s.df else None
        if owner is not None and owner.order:
            order = owner.order
        else:
            order = "F.monotonically_increasing_id()"
            if not self.warned_order:
                self.warned_order = True
                self.warn("pandas relies on row order; the window orders by the current row order (monotonically_increasing_id). "
                          "Sort first (sort_values) or order by an explicit column if the result must be reproducible")
        if s.part:
            w = f"Window.partitionBy({', '.join(s.part)}).orderBy({order})"
        else:
            w = f"Window.orderBy({order})"
            if not self.warned_part:
                self.warned_part = True
                self.warn("window without groupby/partition: Spark moves all rows to a single partition (slow on big data)")
        if rows is not None:
            w += f".rowsBetween({rows[0]}, {rows[1]})"
        return w

    def _int(self, node, what):
        v = _lit(node)
        if isinstance(v, bool) or not isinstance(v, int):
            raise Unsupported(f"{what} must be an integer literal")
        return v

    def seq_method(self, s: V, name: str, n) -> V:
        a, kw = self.args(n)
        c = s.code
        bad = set(kw) - {"shift": {"periods", "fill_value"}, "diff": {"periods"}, "pct_change": {"periods"},
                         "cumsum": {"skipna"}, "cumprod": {"skipna"}, "cummax": {"skipna"}, "cummin": {"skipna"}, "cumcount": set(),
                         "rolling": {"window", "min_periods"}, "expanding": {"min_periods"},
                         "rank": {"method", "ascending", "na_option"}}[name]
        if bad:
            raise Unsupported(f"{name}({', '.join(sorted(bad))}=...) has no automatic conversion")
        if "skipna" in kw and _lit(kw["skipna"]) is not True:
            raise Unsupported(f"{name}(skipna=False) has no automatic conversion")

        def col(code, **k):
            return V("col", code, df=s.df, part=s.part, **k)

        if name in ("shift", "diff", "pct_change"):
            per = self._int(a[0] if a else kw["periods"], "periods") if (a or "periods" in kw) else 1
            if per == 0 and name != "shift":
                raise Unsupported(f"{name}(0)")
            fill = ""
            if "fill_value" in kw or len(a) > 1:
                if name != "shift":
                    raise Unsupported(f"{name}() takes no fill_value")
                fv = self.visit(kw["fill_value"] if "fill_value" in kw else a[1])
                if fv.kind != "py":
                    raise Unsupported("fill_value must be a literal")
                fill = f", {fv.code}"
            w = self.window(s)
            if per == 0:
                return col(c)
            fn = f"F.lag({c}, {per}{fill})" if per > 0 else f"F.lead({c}, {-per}{fill})"
            shifted = f"{fn}.over({w})"
            if name == "shift":
                return col(shifted)
            if name == "diff":
                return col(f"({c} - {shifted})")
            return col(f"({c} / {shifted} - 1)")
        if name in ("cumsum", "cumprod", "cummax", "cummin"):
            w = self.window(s, ("Window.unboundedPreceding", "Window.currentRow"))
            fn = {"cumsum": "sum", "cumprod": "product", "cummax": "max", "cummin": "min"}[name]       # F.product: Spark 3.2+
            return col(f"F.when({c}.isNotNull(), F.{fn}({c}).over({w}))")        # pandas keeps NaN where the input is NaN
        if name == "cumcount":
            w = self.window(s)
            return col(f"(F.row_number().over({w}) - 1)")
        if name == "rank":
            if a:
                raise Unsupported("rank() takes keyword arguments only here")
            method = _lit(kw["method"]) if "method" in kw else "average"
            asc = _lit(kw["ascending"]) if "ascending" in kw else True
            na = _lit(kw["na_option"]) if "na_option" in kw else "keep"
            if method not in ("average", "min", "max", "dense", "first") or not isinstance(asc, bool) or na != "keep":
                raise Unsupported("rank() converts only with method='average', 'min', 'max', 'dense' or 'first', a literal ascending and na_option='keep'")
            self.uses_window = True
            o = f"{c}.{'asc' if asc else 'desc'}_nulls_last()"
            if method == "first":
                o += f", {self.vars[s.df].order}" if (s.df in self.vars and self.vars[s.df].order) else ", F.monotonically_increasing_id()"
                if not (s.df in self.vars and self.vars[s.df].order) and not self.warned_order:
                    self.warned_order = True
                    self.warn("rank(method='first') breaks ties by row order; the converted code uses the current row order (monotonically_increasing_id). Sort first if the result must be reproducible")
            w = f"Window.partitionBy({', '.join(s.part)}).orderBy({o})" if s.part else f"Window.orderBy({o})"
            if not s.part and not self.warned_part:
                self.warned_part = True
                self.warn("window without groupby/partition: Spark moves all rows to a single partition (slow on big data)")
            if method in ("average", "max"):
                # rank() is the lowest rank of a tie group; add the group size: max = min + n - 1, average = min + (n - 1) / 2
                tw = f"Window.partitionBy({', '.join(list(s.part or []) + [c])})"
                n_ties = f"(F.count(F.lit(1)).over({tw}) - 1)"
                expr = f"(F.rank().over({w}) + {n_ties}{' / 2' if method == 'average' else ''})"
            else:
                expr = f"F.{ {'min': 'rank', 'dense': 'dense_rank', 'first': 'row_number'}[method] }().over({w})"
            return col(f"F.when({c}.isNotNull(), {expr}.cast('double'))")      # pandas ranks are floats and keep NaN as NaN
        # rolling / expanding -> intermediate object
        mp = _lit(kw["min_periods"]) if "min_periods" in kw else None
        if mp is not None and (isinstance(mp, bool) or not isinstance(mp, int) or mp < 0):
            raise Unsupported("min_periods must be a non-negative integer literal")
        if name == "rolling":
            node = a[0] if a else kw.get("window")
            if node is None:
                raise Unsupported("rolling() without a window size")
            size = self._int(node, "rolling window")
            if size < 1:
                raise Unsupported("rolling window must be >= 1")
            return V("roll", c, df=s.df, part=s.part, win=(size, mp if mp is not None else size))
        return V("roll", c, df=s.df, part=s.part, win=(None, mp if mp is not None else 1))

    def roll_method(self, r: V, name: str, n) -> V:
        a, kw = self.args(n)
        if name not in ROLL_FN:
            raise Unsupported(f"rolling/expanding().{name}() has no automatic conversion (supported: {', '.join(sorted(ROLL_FN))})")
        if a or kw:
            raise Unsupported(f"rolling/expanding().{name}(...) arguments have no automatic conversion")
        size, mp = r.win
        lo = f"-{size - 1}" if size else "Window.unboundedPreceding"
        s = V("col", r.code, df=r.df, part=r.part)
        w = self.window(s, (lo if size is None or size > 1 else "0", "Window.currentRow"))
        agg = f"F.{ROLL_FN[name]}({r.code}).over({w})"
        if mp > 1:                      # pandas: NaN until `min_periods` non-null observations are in the window
            agg = f"F.when(F.count({r.code}).over({w}) >= {mp}, {agg})"
        return V("col", agg, df=r.df, part=r.part)

    def udf(self, s, fn):
        if not isinstance(fn, ast.Lambda):
            raise Unsupported("apply/map with a named function: wrap it with F.udf(fn, returnType)(col)")
        self.uses_types = True
        self.warn("apply/map -> Python UDF with StringType return (set the right type; prefer built-in functions or pandas_udf for speed)")
        return V("col", f"F.udf({ast.unparse(fn)}, T.StringType())({s.code})", df=s.df)

    def accessor_call(self, s: V, acc: str, name: str, n) -> V:
        a, kw = self.args(n)
        c, df = s.code, s.df

        def colv(code, **k):
            return V("col", code, df=df, **k)

        if acc == "str":
            if name in STR_SIMPLE:
                return colv(f"F.{STR_SIMPLE[name]}({c})", isstr=name != "len")
            if name == "contains":
                p = a[0]
                regex = _lit(kw["regex"]) if "regex" in kw else True
                if "case" in kw and _lit(kw["case"]) is False:
                    raise Unsupported("contains(case=False): use F.lower(col).contains(...)")
                m = "rlike" if regex else "contains"
                return colv(f"{c}.{m}({self.visit(p).code})", boolean=True)
            if name in ("startswith", "endswith"):
                return colv(f"{c}.{name}({self.visit(a[0]).code})", boolean=True)
            if name == "replace":
                old, new = _lit(a[0]), _lit(a[1])
                regex = _lit(kw["regex"]) if "regex" in kw else False
                pat = old if regex else re.escape(old)
                return colv(f"F.regexp_replace({c}, {pat!r}, {new!r})", isstr=True)
            if name == "split":
                return colv(f"F.split({c}, {re.escape(_lit(a[0])) if a else ' '!r})")
            if name == "slice":
                lo = _lit(a[0]) if a else 0
                hi = _lit(a[1]) if len(a) > 1 else None
                return colv(f"F.substring({c}, {lo + 1}, {(hi - lo) if hi is not None else 2 ** 31 - 1})", isstr=True)
            if name == "zfill":
                return colv(f"F.lpad({c}, {ast.unparse(a[0])}, '0')", isstr=True)
            if name == "cat":
                raise Unsupported("str.cat(): use F.concat_ws")
        if acc == "dt":
            if name == "strftime":
                return colv(f"F.date_format({c}, {_strfmt(_lit(a[0]))!r})", isstr=True)
            if name == "normalize":
                return colv(f"F.to_date({c})")
        raise Unsupported(f".{acc}.{name}() has no automatic conversion")

    # accessor properties like .dt.year (no call)
    def accessor_prop(self, s: V, name: str) -> V:
        if name in DT_SIMPLE:
            return V("col", f"F.{DT_SIMPLE[name]}({s.code})", df=s.df)
        if name == "dayofweek" or name == "weekday":
            return V("col", f"((F.dayofweek({s.code}) + 5) % 7)", df=s.df)
        raise Unsupported(f".dt.{name} has no automatic conversion")


# patch: `.dt.year` style attribute access on accessor values
_orig_attr = Translator.v_Attribute


def _attr_with_accessors(self, n):
    if isinstance(n.value, ast.Attribute) and n.value.attr == "dt":
        inner = self.visit(n.value.value)
        if inner.kind == "col":
            return self.accessor_prop(inner, n.attr)
    return _orig_attr(self, n)


Translator.v_Attribute = _attr_with_accessors


class Converter:
    def __init__(self, source: str, report: Report, translator: "Translator | None" = None):
        self.src = source
        self.r = report
        self.t = translator or Translator(report)

    # --- statement level
    def seg(self, node) -> str:
        return ast.get_source_segment(self.src, node) or ast.unparse(node)

    def comment_out(self, node, why, indent):
        self.r.todo(node.lineno, why)
        pad = "    " * indent
        out = [f"{pad}# TODO(sparqix): {cm(why)}"]
        out += [f"{pad}# {cm(l)}" for l in self.seg(node).splitlines()]
        return out

    def emit_value(self, v: V):
        code = v.code
        if v.kind in ("df", "col", "py"):
            return strip_parens(code)
        raise Unsupported("expression is not a value")

    def stmt(self, node, indent=0) -> list:
        pad = "    " * indent
        self.t.line = node.lineno
        self.t.pre_lines.clear()
        try:
            lines = self.stmt_inner(node, indent)
        except Unsupported as e:
            self.t.pre_lines.clear()
            return self.comment_out(node, str(e), indent)
        pre = [pad + l for l in self.t.pre_lines]
        self.t.pre_lines.clear()
        return pre + lines

    def stmt_inner(self, node, indent):
        t = self.t
        pad = "    " * indent
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [a.name.split(".")[0] for a in node.names] if isinstance(node, ast.Import) \
                else [node.module.split(".")[0] if node.module else ""]
            if any(n in ("pandas", "numpy", "polars", "dask", "pandas_gbq") for n in names):
                return []
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("google.cloud"):
                return []
            return [pad + self.seg(node)]
        if isinstance(node, ast.Assign):
            return self.assign(node, pad)
        if isinstance(node, ast.AugAssign):
            tgt = node.target
            if isinstance(tgt, ast.Subscript) and isinstance(tgt.slice, ast.Constant):
                base = t.visit(tgt.value)
                if base.kind == "df":
                    expr = ast.BinOp(left=tgt, op=node.op, right=node.value)
                    ast.copy_location(expr, node)
                    ast.fix_missing_locations(expr)
                    v = t.visit(expr)
                    return [f"{pad}{base.code} = {base.code}.withColumn({tgt.slice.value!r}, {strip_parens(t.col(v))})"]
            if self.mentions_spark_vars(node):
                raise Unsupported("augmented assignment on Spark objects")
            return [pad + self.seg(node)]
        if isinstance(node, ast.Expr):
            return self.expr_stmt(node, pad)
        if isinstance(node, ast.Return):
            if node.value is None:
                return [pad + "return"]
            v = t.visit(node.value)
            return [f"{pad}return {self.emit_value(v)}"]
        if isinstance(node, ast.FunctionDef):
            return self.funcdef(node, indent)
        if isinstance(node, (ast.If, ast.For, ast.While, ast.With)):
            return self.compound(node, indent)
        if isinstance(node, (ast.Pass, ast.Break, ast.Continue)):
            return [pad + ast.unparse(node)]
        if self.mentions_spark_vars(node):
            raise Unsupported(f"{type(node).__name__} statement over Spark objects")
        return [pad + self.seg(node)]

    def mentions_spark_vars(self, node):
        names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        return any(self.t.vars.get(x) and self.t.vars[x].kind != "py" for x in names)

    def assign(self, node, pad):
        t = self.t
        if len(node.targets) != 1:
            raise Unsupported("chained assignment")
        tgt = node.targets[0]
        # df['c'] = expr
        if isinstance(tgt, ast.Subscript):
            base_node = tgt.value
            if isinstance(base_node, ast.Attribute) and base_node.attr == "loc":
                return self.loc_assign(node, tgt, pad)
            base = t.visit(base_node)
            if base.kind == "df" and isinstance(base_node, ast.Name):
                v = t.visit(node.value)
                if isinstance(tgt.slice, ast.Constant) and isinstance(tgt.slice.value, str):
                    if base.order and f"F.col({tgt.slice.value!r})" in base.order:
                        t.vars[base_node.id] = replace(base, order=None)       # the sort column is overwritten: keep the current row order instead
                    return [f"{pad}{base.code} = {base.code}.withColumn({tgt.slice.value!r}, {strip_parens(t.col(v))})"]
                raise Unsupported("assigning to several columns at once")
            raise Unsupported("item assignment on a Spark object")
        if isinstance(tgt, ast.Attribute) and isinstance(tgt.value, ast.Name):
            base = t.visit(tgt.value)
            if base.kind == "df" and tgt.attr == "columns":
                names = t.names(node.value)
                return [f"{pad}{base.code} = {base.code}.toDF({', '.join(names)})"]
            raise Unsupported("attribute assignment on a Spark object")
        if isinstance(tgt, ast.Name) and isinstance(node.value, ast.Call) and "bigquery" in ast.unparse(node.value.func) \
                and ast.unparse(node.value.func).endswith("Client"):
            t.bq_clients.add(tgt.id)
            self.r.note("bigquery.Client(...) removed: authentication is handled by the spark-bigquery connector / cluster credentials")
            return []
        if isinstance(tgt, ast.Name) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            t.strings[tgt.id] = node.value.value
        if isinstance(tgt, ast.Name):
            v = t.visit(node.value)
            if v.kind == "gb":
                raise Unsupported("a groupby object stored in a variable - chain it directly")
            if v.kind in ("df", "col"):
                t.vars[tgt.id] = replace(v, code=tgt.id, df=tgt.id) if v.kind == "df" else v
                code = format_chain(self.emit_value(v))
                if v.kind == "col":
                    # keep a name for the expression but remember its definition
                    t.vars[tgt.id] = replace(v, code=tgt.id)
                return [f"{pad}{tgt.id} = {code}"]
            # plain python (may call .count() etc.)
            t.vars.pop(tgt.id, None)
            return [f"{pad}{tgt.id} = {v.code}"]
        raise Unsupported("unsupported assignment target")

    def loc_assign(self, node, tgt, pad):
        t = self.t
        df = t.visit(tgt.value.value)
        sl = tgt.slice
        if df.kind != "df" or not (isinstance(sl, ast.Tuple) and len(sl.elts) == 2):
            raise Unsupported("unsupported .loc assignment")
        mask = t.visit(sl.elts[0])
        colname = _lit(sl.elts[1])
        val = t.visit(node.value)
        if not (mask.kind == "col" and mask.boolean):
            raise Unsupported(".loc row selector must be a boolean condition")
        return [f"{pad}{df.code} = {df.code}.withColumn({colname!r}, "
                f"F.when({strip_parens(mask.code)}, {t.col(val)}).otherwise(F.col({colname!r})))"]

    def expr_stmt(self, node, pad):
        t = self.t
        call = node.value
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute):
            inplace = [k for k in call.keywords if k.arg == "inplace"]
            if inplace and isinstance(call.func.value, ast.Name) and t.vars.get(call.func.value.id, V("py", "")).kind == "df":
                if _lit(inplace[0].value) is True:
                    call = ast.Call(func=call.func, args=call.args,
                                    keywords=[k for k in call.keywords if k.arg != "inplace"])
                    ast.fix_missing_locations(call)
                    v = t.visit(call)
                    name = call.func.value.id
                    t.vars[name] = V("df", name, df=name)
                    return [f"{pad}{name} = {format_chain(strip_parens(v.code))}"]
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "head":
            v = t.visit(call)
            if v.kind == "df":
                k = ast.unparse(call.args[0]) if call.args else "5"
                return [f"{pad}{v.code.rsplit('.limit', 1)[0]}.show({k})"]
        v = t.visit(call)
        if v.kind == "df":
            return [f"{pad}{v.code}.show()"]
        return [pad + v.code]

    def block(self, body, indent):
        lines = []
        for s in body:
            lines += self.stmt(s, indent)
        return lines or ["    " * indent + "pass"]

    def header_of(self, node):
        c = ast.parse(ast.unparse(node)).body[0]
        c.body = [ast.Pass()]
        if hasattr(c, "orelse"):
            c.orelse = []
        text = ast.unparse(c).splitlines()
        return text[0]

    def funcdef(self, node, indent):
        t = self.t
        pad = "    " * indent
        saved = dict(t.vars)
        for a in node.args.args:
            ann = ast.unparse(a.annotation) if a.annotation else ""
            if "DataFrame" in ann or re.fullmatch(r"(df|data)\w*|\w+_df", a.arg):
                t.vars[a.arg] = V("df", a.arg, df=a.arg)
        fn = ast.parse(ast.unparse(node)).body[0]
        for a in fn.args.args:
            if a.annotation and "DataFrame" in ast.unparse(a.annotation):
                a.annotation = ast.Name(id="DataFrame")
                t.imports.add("DataFrame")
        if fn.returns and "DataFrame" in ast.unparse(fn.returns):
            fn.returns = ast.Name(id="DataFrame")
            t.imports.add("DataFrame")
        fn.body = [ast.Pass()]
        head = ast.unparse(fn).splitlines()[0]
        body = self.block(node.body, indent + 1)
        t.vars = saved
        return [pad + head] + body

    def compound(self, node, indent):
        pad = "    " * indent
        if isinstance(node, ast.If):
            test = self.t.visit(node.test)
            if test.kind != "py":
                raise Unsupported("condition on a Spark object")
            lines = [f"{pad}if {test.code}:"] + self.block(node.body, indent + 1)
            if node.orelse:
                lines += [f"{pad}else:"] + self.block(node.orelse, indent + 1)
            return lines
        header_nodes = {ast.For: lambda x: [x.iter], ast.While: lambda x: [x.test],
                        ast.With: lambda x: [i.context_expr for i in x.items]}[type(node)](node)
        if isinstance(node, ast.For):
            loop = self.row_loop(node, indent)
            if loop is not None:
                return loop
        for h in header_nodes:
            if self.mentions_spark_vars(h):
                raise Unsupported(f"{type(node).__name__.lower()} loop over a Spark DataFrame/column: "
                                  "rewrite as column expressions, groupBy/agg, or a UDF")
        head = self.header_of(node)
        return [pad + head] + self.block(node.body, indent + 1)

    def row_loop(self, node, indent):
        """`for i, row in df.iterrows()` / `for row in df.itertuples()` -> a driver-side loop over toLocalIterator().
        Only when the body reads rows and never touches a Spark object (no writes back into the DataFrame)."""
        it = node.iter
        if not (isinstance(it, ast.Call) and isinstance(it.func, ast.Attribute) and it.func.attr in ("iterrows", "itertuples") and not it.args):
            return None
        if any(k.arg not in ("name",) for k in it.keywords) or it.func.attr == "iterrows" and it.keywords:
            raise Unsupported(f"{it.func.attr}() arguments have no automatic conversion")
        recv = self.t.visit(it.func.value)
        if recv.kind != "df":
            return None
        if node.orelse:
            raise Unsupported("for/else over rows has no automatic conversion")
        tgt = node.target
        if it.func.attr == "iterrows":
            if not (isinstance(tgt, ast.Tuple) and len(tgt.elts) == 2 and all(isinstance(e, ast.Name) for e in tgt.elts)):
                raise Unsupported("iterrows() loop target must be `for idx, row in ...`")
            idx, row = tgt.elts[0].id, tgt.elts[1].id
            head = f"for {idx}, {row} in enumerate({recv.code}.toLocalIterator()):"
        else:
            if not isinstance(tgt, ast.Name):
                raise Unsupported("itertuples() loop target must be a single name")
            row = tgt.id
            if any(isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == row and n.attr == "Index" for n in ast.walk(node)):
                raise Unsupported("row.Index has no automatic conversion (Spark rows have no index)")
            head = f"for {row} in {recv.code}.toLocalIterator():"
        saved = {k: self.t.vars.pop(k) for k in [x.id for x in ast.walk(tgt) if isinstance(x, ast.Name)] if k in self.t.vars}
        try:
            for b in node.body:
                if self.mentions_spark_vars(b):
                    raise Unsupported("the loop body uses the DataFrame (e.g. writes back with df.loc[...]); "
                                      "rewrite it as column expressions, a UDF or a pandas_udf / mapInPandas")
            self.t.warn("iterrows/itertuples -> driver-side loop over toLocalIterator(): rows are fetched one partition at a time and processed in plain Python "
                        "(slow, not distributed); the index is the row number, which only matches pandas for a default RangeIndex")
            pad = "    " * indent
            return [pad + head] + self.block(node.body, indent + 1)
        finally:
            self.t.vars.update(saved)

    def body(self) -> str:
        tree = ast.parse(self.src)
        out = []
        for s in tree.body:
            if isinstance(s, ast.FunctionDef) and out:
                out.append("")
            out += self.stmt(s, 0)
            if isinstance(s, ast.FunctionDef):
                out.append("")
        return "\n".join(out).strip("\n")

    def run(self) -> str:
        body = self.body()
        return header_for(self.t, body) + "\n\n" + body + "\n"


def header_for(t: "Translator", body: str) -> str:
    head = ["from pyspark.sql import SparkSession, functions as F" + (", Window" if t.uses_window else "")]
    if t.uses_types:
        head.append("from pyspark.sql import types as T")
    if "DataFrame" in t.imports:
        head.append("from pyspark.sql import DataFrame")
    if t.needs_pandas:
        head.append("import pandas as pd")
    if re.search(r"\bspark\b", body):
        head += ["", HEADER_SESSION]
    return "\n".join(head)


def convert_pandas(source: str, report: Report) -> str:
    return Converter(source, report).run()
