"""Polars (eager + lazy) -> PySpark. Reuses the pandas translator's statement/value machinery,
overriding the Polars-specific API surface."""
from __future__ import annotations

import ast
import re
from dataclasses import replace

from .core import Report, Unsupported, strip_parens
from .pandas_conv import Converter, Translator, V, _lit, _strfmt, header_for

PL_DTYPES = {"Int8": "tinyint", "Int16": "smallint", "Int32": "int", "Int64": "bigint", "UInt8": "smallint",
             "UInt16": "int", "UInt32": "bigint", "UInt64": "decimal(20,0)", "Float32": "float",
             "Float64": "double", "Utf8": "string", "String": "string", "Boolean": "boolean",
             "Date": "date", "Datetime": "timestamp"}
AGG = {"sum": "sum", "mean": "avg", "min": "min", "max": "max", "count": "count", "std": "stddev",
       "var": "variance", "median": "median", "first": "first", "last": "last", "n_unique": "countDistinct"}
MATH = {"abs": "abs", "sqrt": "sqrt", "log10": "log10", "exp": "exp", "floor": "floor", "ceil": "ceil",
        "sin": "sin", "cos": "cos", "tan": "tan", "sign": "signum", "log1p": "log1p"}
HOW = {"inner": "inner", "left": "left", "right": "right", "outer": "full", "full": "full",
       "semi": "left_semi", "anti": "left_anti", "cross": "cross"}
ALIAS = re.compile(r"\.alias\('([^']*)'\)$")
ROOT = re.compile(r"F\.col\('([^']*)'\)")
AGG_START = re.compile(r"^F\.(sum|avg|min|max|count|stddev|variance|median|first|last|countDistinct)\(")


class PolarsTranslator(Translator):
    def dtype(self, node):
        name = node.attr if isinstance(node, ast.Attribute) else getattr(node, "id", None)
        if name not in PL_DTYPES:
            raise Unsupported("unknown polars dtype")
        return PL_DTYPES[name]

    # ---- attribute properties
    def v_Attribute(self, n):
        if isinstance(n.value, ast.Name) and n.value.id in ("pl", "polars") and n.value.id not in self.vars:
            return V("py", f"pl.{n.attr}")
        if isinstance(n.value, ast.Attribute) and n.value.attr == "dt":
            inner = self.visit(n.value.value)
            if inner.kind == "col":
                raise Unsupported(f".dt.{n.attr} is a method in Polars - call it")
        base = self.visit(n.value)
        if base.kind == "df" and n.attr in ("height",):
            return V("py", f"{base.code}.count()")
        if base.kind == "df" and n.attr == "width":
            return V("py", f"len({base.code}.columns)")
        if base.kind == "df" and n.attr in ("schema",):
            return V("py", f"{base.code}.schema")
        if base.kind == "df" and n.attr not in ("columns", "shape", "dtypes"):
            raise Unsupported(f"DataFrame.{n.attr} has no automatic conversion")
        if base.kind == "col" and n.attr in ("str", "dt"):
            return V("accessor", base.code, df=base.df, attr=n.attr)
        if base.kind == "df":
            return super().v_Attribute(n)
        return V("py", f"{base.code}.{n.attr}")

    def v_Call(self, n):
        f = n.func
        if isinstance(f, ast.Attribute) and f.attr in ("then", "otherwise", "when") \
                and not (isinstance(f.value, ast.Name) and f.value.id in ("pl", "polars")):
            recv = self.visit(f.value)
            if recv.kind in ("when", "col"):
                return self.when_call(recv, f.attr, n)
        return super().v_Call(n)

    def when_call(self, recv, name, n):
        a = [self.visit(x) for x in n.args]
        if name == "when":
            return V("when", recv.code, attr=strip_parens(a[0].code))
        if name == "then":
            val = self.col(a[0])
            code = f"F.when({recv.attr}, {val})" if not recv.code else f"{recv.code}.when({recv.attr}, {val})"
            return V("col", code)
        if name == "otherwise":
            return V("col", f"{recv.code}.otherwise({self.col(a[0])})")
        raise Unsupported(name)

    # ---- pl.* functions
    def module_call(self, name, n, mod):
        a, kw = self.args(n)
        if name == "col":
            if len(a) != 1 or not (isinstance(a[0], ast.Constant) and isinstance(a[0].value, str)):
                raise Unsupported("pl.col with several names / dtypes / regex - expand manually")
            if a[0].value == "*":
                raise Unsupported("pl.col('*')")
            return V("col", f"F.col({a[0].value!r})")
        if name == "len":
            return V("col", "F.count(F.lit(1))")
        if name == "lit":
            return V("col", f"F.lit({self.visit(a[0]).code})")
        if name == "when":
            return V("when", "", attr=strip_parens(self.visit(a[0]).code))
        if name in ("read_csv", "scan_csv"):
            opts = ["header=True", "inferSchema=True"]
            sep = kw.get("separator")
            if sep is not None:
                opts.append(f"sep={_lit(sep)!r}")
            if "has_header" in kw and _lit(kw["has_header"]) is False:
                opts[0] = "header=False"
            code = f"spark.read.csv({self.visit(a[0]).code}, {', '.join(opts)})"
            if "columns" in kw:
                code += f".select({', '.join(self.names(kw['columns']))})"
            if "n_rows" in kw:
                code += f".limit({ast.unparse(kw['n_rows'])})"
            return V("df", code)
        if name in ("read_parquet", "scan_parquet"):
            return V("df", f"spark.read.parquet({self.visit(a[0]).code})")
        if name in ("read_ndjson", "scan_ndjson"):
            return V("df", f"spark.read.json({self.visit(a[0]).code})")
        if name == "read_json":
            return V("df", f"spark.read.json({self.visit(a[0]).code}, multiLine=True)")
        if name == "concat":
            dfs = [self.visit(e) for e in a[0].elts]
            how = _lit(kw["how"]) if "how" in kw else "vertical"
            if how in ("vertical", "diagonal", "vertical_relaxed", "diagonal_relaxed"):
                allow = ", allowMissingColumns=True" if how.startswith("diagonal") else ""
                code = dfs[0].code + "".join(f".unionByName({d.code}{allow})" for d in dfs[1:])
                return V("df", code)
            raise Unsupported(f"pl.concat(how={how!r})")
        if name == "DataFrame":
            self.needs_pandas = True
            return V("df", f"spark.createDataFrame(pd.DataFrame({ast.unparse(a[0])}))")
        if name in ("sum", "mean", "min", "max", "count", "first", "last", "n_unique", "median", "std") and a:
            v = self.visit(a[0])
            return V("col", f"F.{AGG[name]}({self.col(v)})")
        raise Unsupported(f"pl.{name}() has no automatic conversion")

    # ---- helpers for select / with_columns / agg
    def named(self, v: V, name=None):
        code = v.code
        m = ALIAS.search(code)
        if m:
            return code[:m.start()], m.group(1)
        if name:
            return code, name
        r = ROOT.search(code)
        if not r:
            raise Unsupported("expression needs an .alias(...)")
        return code, r.group(1)

    def exprs(self, a, kw):
        out = []
        for x in a:
            if isinstance(x, ast.Constant) and isinstance(x.value, str):
                out.append((f"F.col({x.value!r})", x.value))
            elif isinstance(x, (ast.List, ast.Tuple)):
                out += self.exprs(x.elts, {})
            else:
                v = self.visit(x)
                if v.kind != "col":
                    raise Unsupported("expected a column expression")
                out.append(self.named(v))
        for k, x in kw.items():
            out.append(self.named(self.visit(x), k))
        return out

    def window_fix(self, code):
        if AGG_START.match(code) and ".over(" not in code:
            self.uses_window = True
            self.warn("aggregate inside with_columns/select: added .over(Window.partitionBy()) - this moves all rows to one partition")
            return code + ".over(Window.partitionBy())"
        return code

    # ---- DataFrame methods
    def df_method(self, recv, name, n):
        a, kw = self.args(n)
        c = recv.code
        if name in ("lazy", "collect", "fetch", "clone", "rechunk", "cache"):
            if name in ("collect", "fetch"):
                self.r.note("LazyFrame.collect() removed: Spark is lazy until an action (show/count/write)")
            return recv
        if name == "select":
            items = self.exprs(a, kw)
            parts = []
            for code, nm in items:
                if code == f"F.col({nm!r})":
                    parts.append(code)
                else:
                    parts.append(f"{self.window_fix(code) if False else code}.alias({nm!r})")
            return V("df", f"{c}.select({', '.join(parts)})")
        if name == "with_columns":
            code = c
            for ex, nm in self.exprs(a, kw):
                code += f".withColumn({nm!r}, {strip_parens(self.window_fix(ex))})"
            return V("df", code)
        if name == "filter":
            conds = [self.visit(x) for x in a]
            cond = " & ".join(strip_parens(x.code) if len(conds) == 1 else x.code for x in conds)
            return V("df", f"{c}.filter({cond})")
        if name in ("group_by", "groupby", "group_by_dynamic"):
            if name == "group_by_dynamic":
                raise Unsupported("group_by_dynamic -> use F.window(...)")
            return V("gb", c, df=c, keys=self.names(ast.Tuple(elts=a)) if len(a) > 1 else self.names(a[0]))
        if name == "sort":
            by = self.names(a[0] if a else kw["by"])
            desc = _lit(kw["descending"]) if "descending" in kw else False
            desc = desc if isinstance(desc, (list, tuple)) else [desc] * len(by)
            cols = ", ".join(f"F.col({b}).{'desc' if d else 'asc'}()" for b, d in zip(by, desc))
            return V("df", f"{c}.orderBy({cols})")
        if name == "join":
            right = self.visit(a[0])
            how = HOW[_lit(kw["how"])] if "how" in kw else "inner"
            if "on" in kw:
                on = self.names(kw["on"])
                return V("df", f"{c}.join({right.code}, on={on[0] if len(on) == 1 else '[' + ', '.join(on) + ']'}, how={how!r})")
            if "left_on" in kw and "right_on" in kw:
                lo, ro = self.names(kw["left_on"]), self.names(kw["right_on"])
                if not (re.fullmatch(r"\w+", c) and re.fullmatch(r"\w+", right.code)):
                    raise Unsupported("left_on/right_on join needs both sides assigned to variables first")
                cond = " & ".join(f"({c}[{l}] == {right.code}[{r}])" for l, r in zip(lo, ro))
                return V("df", f"{c}.join({right.code}, {strip_parens(cond) if len(lo) == 1 else cond}, {how!r})")
            raise Unsupported("join needs on= or left_on/right_on")
        if name in ("head", "limit"):
            return V("df", f"{c}.limit({ast.unparse(a[0]) if a else kw.get('n') and ast.unparse(kw['n']) or 5})")
        if name == "unique":
            sub = kw.get("subset") or (a[0] if a else None)
            self.warn("unique() keeps an arbitrary row per key (Polars keeps first only with keep='first', maintain_order)")
            return V("df", f"{c}.dropDuplicates([{', '.join(self.names(sub))}])" if sub else f"{c}.distinct()")
        if name == "drop_nulls":
            sub = kw.get("subset") or (a[0] if a else None)
            return V("df", f"{c}.dropna(subset=[{', '.join(self.names(sub))}])" if sub else f"{c}.dropna()")
        if name == "fill_null":
            val = a[0] if a else kw.get("value")
            if val is None or "strategy" in kw:
                raise Unsupported("fill_null(strategy=...) needs a Window with last(ignorenulls=True)")
            return V("df", f"{c}.fillna({self.visit(val).code})")
        if name == "drop":
            return V("df", f"{c}.drop({', '.join(self.names(ast.Tuple(elts=a)) if len(a) > 1 else self.names(a[0]))})")
        if name == "rename":
            m = a[0]
            if not isinstance(m, ast.Dict):
                raise Unsupported("rename needs a literal dict")
            return V("df", c + "".join(f".withColumnRenamed({_lit(k)!r}, {_lit(v)!r})" for k, v in zip(m.keys, m.values)))
        if name == "explode":
            col = self.names(a[0])[0]
            return V("df", f"{c}.withColumn({col}, F.explode({col}))")
        if name == "sample":
            seed = f", seed={ast.unparse(kw['seed'])}" if "seed" in kw else ""
            if "fraction" in kw:
                return V("df", f"{c}.sample(fraction={ast.unparse(kw['fraction'])}{seed})")
            return V("df", f"{c}.orderBy(F.rand({seed[7:]})).limit({ast.unparse(kw.get('n') or a[0])})")
        if name == "to_pandas":
            return V("py", f"{c}.toPandas()")
        if name in ("write_csv", "write_parquet", "write_json", "write_ndjson"):
            fmt = {"write_csv": "csv", "write_parquet": "parquet", "write_json": "json", "write_ndjson": "json"}[name]
            self.r.note("Spark writes a *directory* of part files, not a single file (add .coalesce(1) if you need one)")
            path = self.visit(a[0] if a else kw["file"]).code
            return V("py", f"{c}.write.mode('overwrite').{fmt}({path}{', header=True' if fmt == 'csv' else ''})")
        if name == "describe":
            return V("df", f"{c}.describe()")
        if name == "pivot":
            on, idx, vals = (self.names(kw[k])[0] for k in ("on", "index", "values"))
            fn = _lit(kw["aggregate_function"]) if "aggregate_function" in kw else "first"
            return V("df", f"{c}.groupBy({idx}).pivot({on}).agg(F.{AGG.get(fn, fn)}({vals}))")
        if name in ("with_row_index", "with_row_count", "tail", "melt", "unpivot", "transpose", "map_rows", "apply", "iter_rows", "rolling", "upsample"):
            raise Unsupported(f"DataFrame.{name}() has no automatic conversion (Spark rows have no position/order)")
        raise Unsupported(f"DataFrame.{name}() has no automatic conversion")

    def gb_method(self, gb, name, n):
        a, kw = self.args(n)
        if name == "agg":
            items = self.exprs(a, kw)
            aggs = [f"{code}.alias({nm!r})" for code, nm in items]
            return V("df", f"{gb.code}.groupBy({', '.join(gb.keys)}).agg({', '.join(aggs)})")
        if name in ("len", "count"):
            return V("df", f"{gb.code}.groupBy({', '.join(gb.keys)}).count()")
        raise Unsupported(f"group_by().{name}() has no automatic conversion")

    # ---- column expression methods
    def col_method(self, s, name, n):
        a, kw = self.args(n)
        c, df = s.code, s.df

        def colv(code, **k):
            return V("col", code, df=df, **k)

        def agg(fn):
            code = f"F.{fn}({c})"
            if df:   # Series scalar (df['a'].sum())
                return V("py", f"{df}.agg({code}).first()[0]")
            return V("col", code)

        if name in AGG:
            return agg(AGG[name])
        if name == "len":
            return agg("count") if df else V("col", "F.count(F.lit(1))")
        if name == "alias":
            return V("col", f"{c}.alias({_lit(a[0])!r})", df=df)
        if name in MATH:
            return colv(f"F.{MATH[name]}({c})")
        if name == "log":
            return colv(f"F.log({c})" if not a else f"F.log({ast.unparse(a[0])}, {c})")
        if name == "round":
            return colv(f"F.round({c}, {ast.unparse(a[0]) if a else 0})")
        if name == "pow":
            return colv(f"F.pow({c}, {ast.unparse(a[0])})")
        if name == "is_null":
            return colv(f"{c}.isNull()", boolean=True)
        if name == "is_not_null":
            return colv(f"{c}.isNotNull()", boolean=True)
        if name == "is_in":
            return colv(f"{c}.isin({ast.unparse(a[0])})", boolean=True)
        if name == "is_between":
            return colv(f"{c}.between({self.visit(a[0]).code}, {self.visit(a[1]).code})", boolean=True)
        if name == "not_":
            return colv(f"(~{c})", boolean=True)
        if name == "cast":
            t = self.dtype(a[0])
            return colv(f"{c}.cast({t!r})", isstr=t == "string")
        if name == "fill_null":
            if "strategy" in kw:
                raise Unsupported("fill_null(strategy=...)")
            return colv(f"F.coalesce({c}, {self.col(self.visit(a[0]))})")
        if name == "over":
            self.uses_window = True
            keys = ", ".join(self.names(ast.Tuple(elts=a)) if len(a) > 1 else self.names(a[0]))
            return colv(f"{c}.over(Window.partitionBy({keys}))")
        if name in ("unique", "n_unique"):
            return agg("countDistinct")
        if name in ("shift", "cum_sum", "cum_max", "cum_min", "rank", "diff", "rolling_mean", "rolling_sum", "sort", "arg_max", "arg_min", "map_elements", "map_batches", "sort_by", "head", "tail"):
            raise Unsupported(f"Expr.{name}() needs an explicit Window or UDF")
        raise Unsupported(f"Expr.{name}() has no automatic conversion")

    def accessor_call(self, s, acc, name, n):
        a, kw = self.args(n)
        c, df = s.code, s.df

        def colv(code, **k):
            return V("col", code, df=df, **k)

        if acc == "str":
            m = {"to_uppercase": "upper", "to_lowercase": "lower", "strip_chars": "trim",
                 "len_chars": "length", "to_titlecase": "initcap"}
            if name in m:
                return colv(f"F.{m[name]}({c})", isstr=name != "len_chars")
            if name == "contains":
                lit = _lit(kw["literal"]) if "literal" in kw else False
                return colv(f"{c}.{'contains' if lit else 'rlike'}({self.visit(a[0]).code})", boolean=True)
            if name in ("starts_with", "ends_with"):
                return colv(f"{c}.{name.replace('_with', 'with')}({self.visit(a[0]).code})", boolean=True)
            if name in ("replace", "replace_all"):
                lit = _lit(kw["literal"]) if "literal" in kw else False
                old, new = _lit(a[0]), _lit(a[1])
                if name == "replace":
                    self.warn("str.replace replaces the first match in Polars; regexp_replace replaces all")
                pat = re.escape(old) if lit else old
                return colv(f"F.regexp_replace({c}, {pat!r}, {new!r})", isstr=True)
            if name == "slice":
                off = _lit(a[0])
                ln = _lit(a[1]) if len(a) > 1 else 2 ** 31 - 1
                return colv(f"F.substring({c}, {off + 1}, {ln})", isstr=True)
            if name == "split":
                return colv(f"F.split({c}, {re.escape(_lit(a[0]))!r})")
            if name == "strptime":
                fmt = f", {_strfmt(_lit(a[1]))!r}" if len(a) > 1 else ""
                fn = "to_date" if getattr(a[0], "attr", "") == "Date" else "to_timestamp"
                return colv(f"F.{fn}({c}{fmt})")
        if acc == "dt":
            simple = {"year": "year", "month": "month", "day": "dayofmonth", "hour": "hour", "minute": "minute",
                      "second": "second", "quarter": "quarter", "week": "weekofyear", "ordinal_day": "dayofyear",
                      "date": "to_date"}
            if name in simple:
                return colv(f"F.{simple[name]}({c})")
            if name == "weekday":
                return colv(f"(((F.dayofweek({c}) + 5) % 7) + 1)")
            if name == "strftime":
                return colv(f"F.date_format({c}, {_strfmt(_lit(a[0]))!r})", isstr=True)
        raise Unsupported(f".{acc}.{name}() has no automatic conversion")


def convert_polars(source: str, report: Report) -> str:
    t = PolarsTranslator(report)
    conv = Converter(source, report, translator=t)
    conv.t = t
    body = conv.body()
    return header_for(t, body) + "\n\n" + body + "\n"
