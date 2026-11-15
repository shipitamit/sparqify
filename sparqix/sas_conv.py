"""SAS -> PySpark: DATA step, PROC SORT / MEANS / SUMMARY / FREQ / PRINT / IMPORT / EXPORT / SQL.

SAS is statement based (`...;`), so the file is split on `;` outside quotes and consumed block by block.
Row-order dependent features (RETAIN, FIRST./LAST., LAG, implicit loops, MERGE flags) are flagged as TODO.
"""
from __future__ import annotations

import re

from .core import Report, Unsupported, format_chain, strip_parens, cm
from .pratt import Pratt, tokenize

TOK = re.compile(r"""(?P<ws>\s+)|(?P<num>\d+\.?\d*(?:[eE][+-]?\d+)?)|(?P<str>"[^"]*"|'[^']*')
 |(?P<word>[A-Za-z_][\w.&]*)|(?P<op>\*\*|\|\||<=|>=|\^=|~=|\^|<>|[-+*/=<>(),!|&.])""", re.X)
BP = {"or": 10, "|": 10, "and": 12, "&": 12, "=": 20, "eq": 20, "ne": 20, "^=": 20, "~=": 20, "<>": 20, "<": 20, ">": 20,
      "<=": 20, ">=": 20, "lt": 20, "gt": 20, "le": 20, "ge": 20, "in": 20, "||": 25, "+": 30, "-": 30, "*": 40,
      "/": 40, "**": 60}
UN = {"-": 50, "+": 50, "not": 15, "^": 15, "~": 15}
CMP = {"=": "==", "eq": "==", "ne": "!=", "^=": "!=", "~=": "!=", "<>": "!=", "<": "<", ">": ">", "<=": "<=",
       ">=": ">=", "lt": "<", "gt": ">", "le": "<=", "ge": ">="}
SIMPLE = {"upcase": "upper", "lowcase": "lower", "strip": "trim", "trim": "rtrim", "left": "ltrim", "abs": "abs",
          "sqrt": "sqrt", "exp": "exp", "log": "log", "log10": "log10", "int": "floor", "floor": "floor",
          "ceil": "ceil", "year": "year", "month": "month", "day": "dayofmonth", "length": "length",
          "lengthn": "length", "propcase": "initcap", "hour": "hour", "minute": "minute", "second": "second"}
AGG_STATS = {"mean": "avg", "sum": "sum", "min": "min", "max": "max", "n": "count", "std": "stddev",
             "median": "median", "nmiss": None, "var": "variance"}


class SasExpr:
    def __init__(self, report):
        self.r = report
        self.inputs = []          # dataset tokens read by the block being converted (SET / DATA=), for the table-loading pass
        self.by = []              # BY variables of the DATA step being converted
        self.cond = False         # True while converting an IF/ELSE statement (LAG() is not allowed there)
        self.helpers = set()      # helper functions the generated code needs (e.g. "merge")
        self.datalines = {}       # placeholder -> raw DATALINES text (extracted before the source is split on ';')
        self.once = set()         # warnings already given

    def parse(self, text):
        p = Pratt(tokenize(text, TOK), BP, UN, "SAS expression")
        n = p.expr()
        if not p.done():
            raise Unsupported(f"cannot parse SAS expression `{text}`")
        return n

    def code(self, text):
        return self.ex(self.parse(text))

    def ex(self, n):
        t = n[0]
        if t == "num":
            return n[1]
        if t == "str":
            return repr(n[1][1:-1])
        if t == "paren":
            return f"({self.ex(n[1])})"
        if t == "id" or t == "ref":
            if n[1] == ".":
                return "F.lit(None)"
            fl = re.fullmatch(r"(?i)(first|last)\.(\w+)", n[1])
            if fl:
                return self.byflag(fl.group(1).lower(), fl.group(2))
            if "&" in n[1]:
                raise Unsupported("macro variable reference (&var) - substitute a Python variable")
            return f"F.col({n[1]!r})"
        if t == "un":
            return {"-": f"(-{self.ex(n[2])})", "+": self.ex(n[2])}.get(n[1], f"(~{self.ex(n[2])})")
        if t == "list":
            return "[" + ", ".join(self.lit(x) for x in n[1]) + "]"
        if t == "bin":
            op = n[1]
            if op in CMP:
                if n[2] == ("op", ".") or n[3] == ("id", "."):
                    pass
                if n[3] in (("id", "."),):
                    return f"{self.ex(n[2])}.{'isNull' if CMP[op] == '==' else 'isNotNull'}()"
                return f"({self.ex(n[2])} {CMP[op]} {self.ex(n[3])})"
            if op == "in":
                return f"{self.ex(n[2])}.isin({self.ex(n[3])})"
            if op in ("and", "&"):
                return f"({self.ex(n[2])} & {self.ex(n[3])})"
            if op in ("or", "|"):
                return f"({self.ex(n[2])} | {self.ex(n[3])})"
            if op == "||":
                return f"F.concat({self.s(n[2])}, {self.s(n[3])})"
            if op == "**":
                return f"F.pow({self.ex(n[2])}, {self.ex(n[3])})"
            return f"({self.ex(n[2])} {op} {self.ex(n[3])})"
        if t == "call":
            return self.call(n[1].lower(), n[2])
        raise Unsupported("unsupported SAS expression")

    def warn_once(self, key, msg):
        if key not in self.once:
            self.once.add(key)
            self.r.warn("sas", msg)

    def byflag(self, kind, var):
        by = [b.lower() for b in self.by]
        if var.lower() not in by:
            raise Unsupported(f"{kind.upper()}.{var} needs a BY statement that names {var}")
        parts = self.by[:by.index(var.lower()) + 1]
        self.warn_once("order", "BY-group, RETAIN and LAG logic follow the current row order of the DataFrame (SAS: dataset order): sort first, "
                                "or the result depends on how Spark happens to order rows")
        return f"(F.row_number().over({window(parts, frame=False, desc=kind == 'last')}) == 1)"

    def s(self, n):
        c = self.ex(n)
        return f"F.lit({c})" if n[0] in ("str", "num") else c

    def lit(self, n):
        if n[0] in ("num", "str"):
            return self.ex(n)
        if n[0] == "un" and n[1] == "-":
            return "-" + self.lit(n[2])
        raise Unsupported("IN (...) list must contain literals")

    def rowwise(self, a, op):
        self.r.warn("sas", "row-wise SUM()/MEAN() ignore missing values: translated with coalesce(...,0)")
        return a

    def call(self, f, a):
        e = self.ex
        if f in SIMPLE:
            return f"F.{SIMPLE[f]}({', '.join(e(x) for x in a)})"
        if f == "sum":
            return "(" + " + ".join(f"F.coalesce({e(x)}, F.lit(0))" for x in self.rowwise(a, "sum")) + ")"
        if f == "mean":
            parts = " + ".join(f"F.coalesce({e(x)}, F.lit(0))" for x in a)
            cnt = " + ".join(f"{e(x)}.isNotNull().cast('int')" for x in a)
            self.rowwise(a, "mean")
            return f"(({parts}) / ({cnt}))"
        if f in ("min", "max"):
            return f"F.{'least' if f == 'min' else 'greatest'}({', '.join(e(x) for x in a)})"
        if f == "round":
            unit = a[1][1] if len(a) > 1 else "1"
            if float(unit) in (1.0,):
                return f"F.round({e(a[0])}, 0)"
            if float(unit) < 1:
                return f"F.round({e(a[0])}, {len(unit.split('.')[1])})"
            raise Unsupported("ROUND with unit > 1")
        if f == "substr":
            return f"F.substring({e(a[0])}, {e(a[1])}, {e(a[2]) if len(a) > 2 else 2 ** 31 - 1})"
        if f == "missing":
            return f"{e(a[0])}.isNull()"
        if f in ("cats", "cat"):
            return f"F.concat({', '.join(self.s(x) for x in a)})"
        if f == "catx":
            return f"F.concat_ws({e(a[0])}, {', '.join(self.s(x) for x in a[1:])})"
        if f == "today":
            return "F.current_date()"
        if f == "mdy":
            return f"F.make_date({e(a[2])}, {e(a[0])}, {e(a[1])})"
        if f in ("put", "input") and len(a) == 2 and a[1][0] in ("num", "id"):
            fmt = a[1][1].lower()
            if f == "put":
                if re.fullmatch(r"(best)?\d*\.\d*", fmt):
                    return f"{e(a[0])}.cast('string')"
                m = re.fullmatch(r"z(\d+)\.", fmt)
                if m:
                    return f"F.lpad({e(a[0])}.cast('string'), {m.group(1)}, '0')"
            elif re.fullmatch(r"(best)?\d*\.\d*", fmt):
                return f"{e(a[0])}.cast('double')"
            elif re.fullmatch(r"(yymmdd|date|mmddyy|ddmmyy)\d*\.", fmt):
                self.r.warn("sas", f"INPUT(..., {fmt}) -> to_date with default format; set the pattern explicitly")
                return f"F.to_date({e(a[0])})"
        if f in ("put", "input"):
            raise Unsupported(f"{f.upper()}() with this informat/format: use cast() / date_format() / to_date() explicitly")
        lm = re.fullmatch(r"(lag|dif)(\d*)", f)
        if lm and a:
            if self.cond:
                raise Unsupported(f"{f.upper()}() inside IF/ELSE: SAS updates its queue only when the call runs, so the result differs: compute it in its own assignment first")
            self.warn_once("lag", "LAG()/DIF() look at the previous row of the whole dataset (also across BY groups, as in SAS); the row order is Spark's current order: sort first")
            self.warn_once("order", "BY-group, RETAIN and LAG logic follow the current row order of the DataFrame (SAS: dataset order): sort first, "
                                    "or the result depends on how Spark happens to order rows")
            lagc = f"F.lag({self.s(a[0])}, {int(lm.group(2) or 1)}).over(Window.orderBy('_row'))"
            return lagc if lm.group(1) == "lag" else f"({self.s(a[0])} - {lagc})"
        if f in ("intnx", "intck", "retain"):
            raise Unsupported(f"{f.upper()}() depends on SAS date intervals - convert manually")
        raise Unsupported(f"SAS function {f.upper()}() has no automatic conversion")


def window(parts, frame=True, desc=False):
    """source text of a Window ordered by the hidden row-number column `_row`, partitioned by `parts`; frame=True: running (unbounded preceding to current row)"""
    base = f"Window.partitionBy({', '.join(repr(x) for x in parts)})" if parts else "Window"
    w = f"{base}.orderBy({'F.col(' + repr('_row') + ').desc()' if desc else repr('_row')})"
    return w + (".rowsBetween(Window.unboundedPreceding, Window.currentRow)" if frame else "")


def split_statements(src: str):
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    src = re.sub(r"(?m)^[ \t]*\*[^;]*;", "", src)          # * comment ;
    out, cur, q = [], [], None
    for ch in src:
        if q:
            cur.append(ch)
            if ch == q:
                q = None
        elif ch in "'\"":
            q = ch
            cur.append(ch)
        elif ch == ";":
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    tail = "".join(cur).strip()
    if tail.startswith("%"):                                # a macro call at end of file may omit its ';'
        out.append(tail)
    return [s for s in out if s]


def split_ds(text: str):
    out, cur, depth = [], "", 0
    for ch in text:
        depth += ch == "("
        depth -= ch == ")"
        if ch.isspace() and depth == 0:
            if cur:
                out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur:
        out.append(cur)
    return out


def dsname(tok: str) -> str:
    tok = tok.strip()
    tok = re.sub(r"\(.*\)$", "", tok).strip()
    return re.sub(r"(?i)^(work|temp)\.", "", tok).replace(".", "_")


def ds_options(tok: str, em: SasExpr, code: str, report) -> str:
    """apply (where= keep= drop= rename=) dataset options"""
    m = re.search(r"^[^(]*\((.*)\)\s*$", tok.strip())
    if not m:
        return code
    opts = m.group(1)
    w = re.search(r"(?i)where\s*=\s*\((.*?)\)(?=\s+\w+\s*=|\s*$)", opts)
    if w:
        code += f".filter({strip_parens(em.code(w.group(1)))})"
    k = re.search(r"(?i)keep\s*=\s*([\w\s]+?)(?=\s+\w+\s*=|\s*$)", opts)
    if k:
        code += ".select(" + ", ".join(repr(x) for x in k.group(1).split()) + ")"
    d = re.search(r"(?i)drop\s*=\s*([\w\s]+?)(?=\s+\w+\s*=|\s*$)", opts)
    if d:
        code += ".drop(" + ", ".join(repr(x) for x in d.group(1).split()) + ")"
    r = re.search(r"(?i)rename\s*=\s*\((.*?)\)", opts)
    if r:
        for a, b in re.findall(r"(\w+)\s*=\s*(\w+)", r.group(1)):
            code += f".withColumnRenamed({a!r}, {b!r})"
    return code


_IDENT = re.compile(r"[A-Za-z_]\w*")


def _bind_tables(lines, inputs, out_so_far):
    """DataFrames only exist as Python variables, SAS datasets are named tables. So: (1) a dataset read but never produced earlier in the script is a
    catalog table: load it with spark.table(); (2) a PROC SQL that reads a DataFrame needs it registered as a temp view first."""
    from .sql_conv import tables_in
    defined = {m.group(1) for l in out_so_far for m in [re.match(r"([A-Za-z_]\w*) = ", l)] if m}
    pre = []
    for tok in inputs:
        name = dsname(tok)
        if _IDENT.fullmatch(name) and name not in defined and name not in {p.split(" = ")[0] for p in pre}:
            table = re.sub(r"\(.*\)$", "", tok.strip()).strip()
            table = re.sub(r"(?i)^(work|temp)\.", "", table)
            pre.append(f"{name} = spark.table({table!r})")
    text = "\n".join(lines)
    produced = {m.group(1) for l in lines for m in [re.match(r"([A-Za-z_]\w*) = ", l)] if m}
    for sql in re.findall(r'spark\.sql\("""\n(.*?)\n"""\)', text, re.S):
        for t in tables_in(sql):
            if _IDENT.fullmatch(t) and (t in defined or any(p.startswith(t + " = ") for p in pre)) and t not in produced:
                pre.append(f"{t}.createOrReplaceTempView({t!r})")
    return pre + lines


_DL_START = re.compile(r"(?i)(.*;)?([ \t]*)(datalines4?|cards4?)[ \t]*;[ \t]*\r?")
_DL_END = re.compile(r";{1,4}[ \t]*\r?")


def extract_datalines(source: str, em) -> str:
    """Move DATALINES/CARDS data out of the text before it is split on ';' (it may hold quotes, '/*' or anything else). Linear: a block with no
    closing ';' line ends the search, so a hostile file cannot make this quadratic."""
    lines, out, i = source.split("\n"), [], 0
    while i < len(lines):
        m = _DL_START.fullmatch(lines[i])
        if m:
            j = i + 1
            while j < len(lines) and not _DL_END.fullmatch(lines[j].strip()):
                j += 1
            if j < len(lines):
                key = f"__SPQX_DATA_{len(em.datalines)}__"
                em.datalines[key] = "\n".join(lines[i + 1:j])
                out.append(f"{m.group(1) or ''}{m.group(2)}{m.group(3)};")
                out.append(f"{key};")
                i = j + 1
                continue
            out.extend(lines[i:])                 # unterminated: leave the rest alone
            break
        out.append(lines[i])
        i += 1
    return "\n".join(out)


_MERGE_HELPER = [
    "def _sas_merge(frames, flags, by):",
    '    """SAS MERGE ... BY: full outer match-merge. Same-named columns: the later dataset wins where it has the row. The IN= flags are booleans."""',
    "    out = frames[0].withColumn(flags[0], F.lit(True))",
    "    for df, flag in zip(frames[1:], flags[1:]):",
    "        df = df.withColumn(flag, F.lit(True))",
    "        ov = [c for c in out.columns if c in df.columns and c not in by and c != flag]",
    "        df = df.select(*[F.col(c).alias(c + '__r') if c in ov else F.col(c) for c in df.columns])",
    "        out = out.join(df, on=by, how='outer')",
    "        for c in ov:",
    "            out = out.withColumn(c, F.when(F.col(flag), F.col(c + '__r')).otherwise(F.col(c))).drop(c + '__r')",
    "    for flag in flags:",
    "        out = out.withColumn(flag, F.coalesce(F.col(flag), F.lit(False)))",
    "    return out",
    "",
    "",
]


def convert_sas(source: str, report: Report) -> str:
    from .sas_macro import expand_macros
    em = SasExpr(report)

    source = extract_datalines(source, em)
    stmts = expand_macros(split_statements(source), report)
    out = ["from pyspark.sql import SparkSession, functions as F, Window", "",
           'spark = SparkSession.builder.appName("converted").getOrCreate()', ""]
    helper_at = len(out)
    i = 0
    while i < len(stmts):
        s = stmts[i]
        head = s.split(None, 1)[0].lower() if s else ""
        low = s.lower()
        start = i
        try:
            if head in ("libname", "options", "title", "footnote", "ods", "filename", "quit", "run", "%put", "%include"):
                if head == "libname":
                    report.note("LIBNAME ignored: point spark.read/write or catalog databases at the same storage")
                i += 1
                continue
            if head == "%let":
                m = re.match(r"(?is)%let\s+(\w+)\s*=\s*(.*)", s)
                v = m.group(2).strip().strip("'\"")
                out.append(f"{m.group(1)} = {v!r}  # SAS macro variable (&{cm(m.group(1))})")
                report.warn("sas", f"macro variable &{m.group(1)} became a Python variable - replace &{m.group(1)} usages yourself")
                i += 1
                continue
            if head.startswith("%"):
                raise Unsupported(f"{head} macro construct has no automatic conversion")
            if head in ("data", "proc"):
                em.inputs = []
                i, lines = (data_step if head == "data" else proc)(stmts, i, em, report)
                out += _bind_tables(lines, em.inputs, out)
            else:
                raise Unsupported(f"statement `{s.split()[0]}` not understood")
        except Unsupported as e:
            # skip to the end of this block (run; / quit;)
            j = start + 1
            while j < len(stmts) and stmts[j].lower().split()[0] not in ("run", "quit", "data", "proc"):
                j += 1
            if j < len(stmts) and stmts[j].lower().split()[0] in ("run", "quit"):
                j += 1
            report.todo(f"statement {start + 1}", str(e))
            out.append(f"# TODO(sparqix): {cm(e)}")
            out += ["# " + x.replace("\n", "\n# ") + ";" for x in stmts[start:j]]
            i = j
    if "merge" in em.helpers:
        out[helper_at:helper_at] = _MERGE_HELPER
    return "\n".join(out) + "\n"


def block(stmts, i):
    """statements until run;/quit; (exclusive). returns (body, next_index)"""
    body = []
    i += 1
    while i < len(stmts) and stmts[i].lower().split()[0] not in ("run", "quit"):
        body.append(stmts[i])
        i += 1
    return body, i + 1


_NUMTXT = re.compile(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?")
_DATE_INFORMATS = {"date": "ddMMMyyyy", "mmddyy": "MM/dd/yyyy", "ddmmyy": "dd/MM/yyyy"}
MAX_DATALINES = 5000


def _date_pattern(fmt):
    m = re.fullmatch(r"(date|yymmdd|mmddyy|ddmmyy)(\d*)\.", fmt.lower())
    if not m:
        return None
    if m.group(1) == "yymmdd":
        return "yyyyMMdd" if m.group(2) == "8" else "yyyy-MM-dd"
    if m.group(1) in ("mmddyy", "ddmmyy") and m.group(2) == "8":
        return _DATE_INFORMATS[m.group(1)].replace("yyyy", "yy")
    return _DATE_INFORMATS[m.group(1)]


def parse_input(st):
    """INPUT statement -> [(name, kind, date pattern)]; kind is num | char | date. Column pointers and other advanced forms are not supported."""
    toks = st.strip()[5:].replace(":", " ").split()
    spec, i = [], 0
    while i < len(toks):
        name = toks[i]
        if not re.fullmatch(r"[A-Za-z_]\w*", name):
            raise Unsupported(f"INPUT item `{name[:20]}`: column/line pointers and lists are not supported, read the file with spark.read.csv and a schema")
        kind, pat = "num", None
        if i + 1 < len(toks) and (toks[i + 1].startswith("$") or re.fullmatch(r"[A-Za-z]+\d*\.\d*|\d+\.\d*", toks[i + 1])):
            fmt = toks[i + 1]
            i += 1
            if fmt.startswith("$"):
                kind = "char"
            else:
                pat = _date_pattern(fmt)
                if pat:
                    kind = "date"
                elif not re.fullmatch(r"(best)?\d*\.\d*", fmt.lower()):
                    raise Unsupported(f"informat {fmt} is not supported: read the column as text and convert it")
        spec.append((name, kind, pat))
        i += 1
    if not spec:
        raise Unsupported("INPUT statement without variables")
    return spec


def _schema(spec):
    return ", ".join(f"{n} {'double' if k == 'num' else 'string'}" for n, k, _ in spec)


def _date_cols(spec):
    return "".join(f".withColumn({n!r}, F.to_date(F.col({n!r}), {p!r}))" for n, k, p in spec if k == "date")


def datalines_frame(text, spec, infile, report):
    rows, bad = [], 0
    dlm = infile.get("dlm")
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = [x.strip() for x in (line.split(dlm) if dlm else line.split())]
        if infile.get("dsd"):
            parts = [x[1:-1] if len(x) >= 2 and x[0] == x[-1] == '"' else x for x in parts]
        vals = []
        for k, (n, kind, _) in enumerate(spec):
            v = parts[k] if k < len(parts) else ""
            if kind == "num":
                if v in ("", "."):
                    vals.append(None)
                elif _NUMTXT.fullmatch(v):
                    vals.append(float(v))
                else:
                    vals.append(None)
                    bad += 1
            else:
                vals.append(v if v != "" else None)
        rows.append(tuple(vals))
    if len(rows) > MAX_DATALINES:
        raise Unsupported(f"DATALINES with {len(rows)} rows: write it to a file and read it with spark.read.csv")
    if bad:
        report.warn("sas", f"{bad} DATALINES value(s) were not numbers and became missing")
    report.note("DATALINES became an in-memory DataFrame (spark.createDataFrame); numeric variables are double like in SAS")
    return f"spark.createDataFrame({rows!r}, {_schema(spec)!r}){_date_cols(spec)}"


def _parse_infile(st, report):
    m = re.match(r"(?is)infile\s+(\"[^\"]*\"|'[^']*'|datalines|cards)\s*(.*)$", st.strip())
    if not m:
        raise Unsupported("INFILE with a fileref: use spark.read.csv with the real path")
    d = {"path": None if m.group(1).lower() in ("datalines", "cards") else m.group(1)[1:-1], "dsd": bool(re.search(r"(?i)\bdsd\b", m.group(2)))}
    dm = re.search(r"(?i)\b(?:dlm|delimiter)\s*=\s*('[^']*'|\"[^\"]*\")", m.group(2))
    if dm:
        d["dlm"] = dm.group(1)[1:-1]
    elif d["dsd"]:
        d["dlm"] = ","
    fo = re.search(r"(?i)\bfirstobs\s*=\s*(\d+)", m.group(2))
    d["firstobs"] = int(fo.group(1)) if fo else 1
    if d["firstobs"] > 2:
        raise Unsupported("INFILE FIRSTOBS > 2: skip the leading lines yourself")
    if re.search(r"(?i)\b(obs|lrecl|recfm|column|start)\s*=", m.group(2)):
        report.warn("sas", "INFILE options OBS=/LRECL=/RECFM= are ignored")
    if "&" in (d["path"] or ""):
        raise Unsupported("macro variable in an INFILE path")
    return d


_SUM = re.compile(r"(?is)([A-Za-z_]\w*)\s*\+\s*(.+)")
_SUM_ASSIGN = re.compile(r"(?is)([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\s*\+\s*(.+)")
_RESET = re.compile(r"(?is)if\s+first\.(\w+)\s+then\s+([A-Za-z_]\w*)\s*=\s*(.+)")
_ELSE_SUM = re.compile(r"(?is)else\s+([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\s*\+\s*(.+)")
_CARRY = re.compile(r"(?is)if\s+(?:not\s+missing\(\s*(\w+)\s*\)|(\w+)\s*(?:\^=|~=|ne|<>)\s*\.)\s+then\s+([A-Za-z_]\w*)\s*=\s*(\w+)")
_OUTPUT_IF = re.compile(r"(?is)if\s+(.+?)\s+then\s+output")


def plan_state(body, em, report):
    """Pre-pass over a DATA step body for statements whose meaning depends on row order. Returns (actions, skip, needs_row, out_filter):
    actions {statement index: '.withColumn(...)' text}, statements to skip (consumed by a plan), whether the hidden `_row` column is needed,
    and the condition of a final `IF cond THEN OUTPUT`. Only recognised patterns are converted: running sums and counters, resets at the start of a
    BY group, carry-forward of the last non-missing value, FIRST./LAST. flags, LAG/DIF, `IF cond THEN OUTPUT`. Anything else on a RETAIN variable is flagged."""
    actions, skip, retained, sums, resets, carries = {}, set(), {}, [], [], []
    out_filter = None
    text = "\n".join(body)
    for idx, st in enumerate(body):
        s = st.strip()
        w = s.split()[0].lower() if s else ""
        if w == "by":
            em.by = [t for t in s.split()[1:] if t.lower() not in ("descending", "notsorted")]
            skip.add(idx)
        elif w == "retain":
            names = []
            for t in s.split()[1:]:
                if re.fullmatch(r"[-+]?\d+(?:\.\d+)?|'[^']*'|\"[^\"]*\"|\.", t):
                    for n in names:
                        retained[n.lower()] = t
                    names = []
                else:
                    names.append(t)
                    retained.setdefault(t.lower(), None)
            skip.add(idx)
    for idx, st in enumerate(body):
        s = st.strip()
        low = s.lower()
        if idx in skip:
            continue
        m = _RESET.fullmatch(s)
        if m:
            nxt = body[idx + 1].strip() if idx + 1 < len(body) else ""
            m2 = _ELSE_SUM.fullmatch(nxt)
            if m2 and m2.group(1).lower() == m.group(2).lower() == m2.group(2).lower():
                sums.append((idx + 1, m.group(2).lower(), m2.group(3), "ifelse"))
                resets.append((idx, m.group(1), m.group(2).lower(), m.group(3), "after"))
                skip.update((idx, idx + 1))
            else:
                resets.append((idx, m.group(1), m.group(2).lower(), m.group(3), None))
            continue
        m = _CARRY.fullmatch(s)
        if m and m.group(3).lower() in retained and (m.group(1) or m.group(2)).lower() == m.group(4).lower():
            carries.append((idx, m.group(3).lower(), m.group(4)))
            skip.add(idx)
            continue
        m = _SUM_ASSIGN.fullmatch(s)
        if m and m.group(1).lower() == m.group(2).lower() and m.group(1).lower() in retained:
            sums.append((idx, m.group(1).lower(), m.group(3), "assign"))
            skip.add(idx)
            continue
        m = _SUM.fullmatch(s)
        if m and not low.startswith(("if ", "else ", "where ", "keep ", "drop ")):
            sums.append((idx, m.group(1).lower(), m.group(2), "sum"))
            skip.add(idx)
            continue
        if low == "output":
            skip.add(idx)
            continue
        m = _OUTPUT_IF.fullmatch(s)
        if m:
            if out_filter is not None or idx != max(i for i in range(len(body)) if body[i].strip()):
                raise Unsupported("OUTPUT inside the step: only a final `IF cond THEN OUTPUT;` is converted (it becomes a filter)")
            out_filter = m.group(1)
            skip.add(idx)
    used_resets = set()
    for idx, var, term, kind in sums:
        parts, rv, order = [], None, None
        for r in resets:
            if r[2] == var:
                if r[1].lower() not in [b.lower() for b in em.by]:
                    raise Unsupported(f"IF FIRST.{r[1]} needs a BY statement that names {r[1]}")
                parts = em.by[:[b.lower() for b in em.by].index(r[1].lower()) + 1]
                rv = r[3].strip()
                order = r[4] or ("before" if r[0] < idx else "after")
                used_resets.add(r[0])
                break
        t_node = em.parse(term)
        tcode = f"F.coalesce({em.s(t_node)}, F.lit(0))"
        total = f"F.sum({tcode}).over({window(parts)})"
        init = retained.get(var)
        if kind == "assign":
            em.warn_once("assign", "x = x + y on a RETAIN variable: a missing y makes SAS's result missing from then on; here missing values count as 0 (like the sum statement)")
        if parts and rv is not None:
            zero = rv in (".", "0")
            rcode = "F.lit(0)" if zero else em.s(em.parse(rv))
            if order == "before":
                expr = total if zero else f"({rcode} + {total})"
            elif _norm(rv) == _norm(term):
                expr = total
            else:
                first = f"F.first({tcode}).over({window(parts, frame=False)})"
                expr = f"({total} - {first})" if zero else f"({rcode} + {total} - {first})"
        else:
            expr = total if init in (None, "0", ".") else f"(F.lit({init}) + {total})"
        if not parts:
            em.warn_once("single", "running total without BY-group reset uses one Spark partition for the whole dataset (slow on large data)")
        actions[idx] = f".withColumn({var!r}, {expr})"
    for idx, var, src in carries:
        parts, order = [], None
        for r in resets:
            if r[2] == var and r[3].strip() == ".":
                if r[1].lower() not in [b.lower() for b in em.by]:
                    raise Unsupported(f"IF FIRST.{r[1]} needs a BY statement that names {r[1]}")
                parts = em.by[:[b.lower() for b in em.by].index(r[1].lower()) + 1]
                used_resets.add(r[0])
                skip.add(r[0])
        expr = f"F.last(F.col({src!r}), ignorenulls=True).over({window(parts)})"
        init = retained.get(var)
        if init not in (None, "."):
            expr = f"F.coalesce({expr}, F.lit({init}))"
        actions[idx] = f".withColumn({var!r}, {expr})"
    skip.update(used_resets)
    handled = {v for _, v, _, _ in sums} | {v for _, v, _ in carries}
    for var in retained:
        if var not in handled:
            raise Unsupported(f"RETAIN {var}: only running sums (x + y, x = x + y), counters and carrying forward the last non-missing value are converted; rewrite with Window functions")
    needs_row = bool(sums or carries or re.search(r"(?i)\b(first|last)\.\w|\b(lag|dif)\d*\s*\(", text))
    return actions, skip, needs_row, out_filter


def _norm(t):
    return re.sub(r"\s+", "", t).lower()


def data_step(stmts, i, em, report):
    header = stmts[i]
    outs = re.findall(r"\S+", header[4:].strip())
    outname = dsname(outs[0]) if outs else "_data_"
    body, nxt = block(stmts, i)
    code = None
    lines = []
    keep = drop = None
    em.by, em.cond = [], False
    actions, skip, needs_row, out_filter = plan_state(body, em, report)
    infile, spec = {}, None
    drop_cols = []
    skip_next = False

    def flush(var, branches, els):
        c = None
        for cond, val in branches:
            c = f"F.when({strip_parens(cond)}, {val})" if c is None else f"{c}.when({strip_parens(cond)}, {val})"
        c += f".otherwise({els if els is not None else f'F.col({var!r})'})"
        return f".withColumn({var!r}, {c})"

    def started(c):
        """first statement that defines the rows: add the hidden row-number column when BY/RETAIN/LAG logic needs it"""
        if needs_row:
            if em.by and ("," in c or c.count(".unionByName(") or c.startswith("_sas_merge(")):
                c += ".orderBy(" + ", ".join(repr(b) for b in em.by) + ")"
            c += ".withColumn('_row', F.monotonically_increasing_id())"
            drop_cols.append("_row")
        return c

    if_state = None   # (var, branches, else)
    for idx, st in enumerate(body):
        if skip_next:
            skip_next = False
            continue
        low = st.strip().lower()
        w = low.split()[0] if low else ""
        if w == "set":
            names = split_ds(st[3:].strip())
            em.inputs += names
            codes = [ds_options(n, em, dsname(n), report) for n in names]
            code = started(codes[0] + "".join(f".unionByName({c}, allowMissingColumns=True)" for c in codes[1:]))
            continue
        if w == "merge":
            if not em.by:
                raise Unsupported("MERGE without BY pairs rows by position (one-to-one merge): use zip-like logic with row numbers")
            names = split_ds(st[5:].strip())
            frames, flags = [], []
            for k, n in enumerate(names):
                em.inputs.append(re.sub(r"(?i)\bin\s*=\s*\w+", "", n))
                fm = re.search(r"(?i)\bin\s*=\s*(\w+)", n)
                clean = re.sub(r"(?i)\bin\s*=\s*\w+", "", n)
                frames.append(ds_options(clean, em, dsname(clean), report))
                flags.append(fm.group(1) if fm else f"_in{k + 1}")
            em.helpers.add("merge")
            report.warn("sas", "MERGE became a full outer join on the BY variables; for same-named columns the later dataset wins where it has the row. "
                               "Many-to-many matches differ: SAS pairs rows in order, Spark joins every combination")
            code = started(f"_sas_merge([{', '.join(frames)}], {flags!r}, {em.by!r})")
            drop_cols += flags
            continue
        if w == "infile":
            infile = _parse_infile(st, report)
            continue
        if w == "input" and code is None:
            spec = parse_input(st)
            if infile.get("path"):
                code = started(f"spark.read.csv({infile['path']!r}, schema={_schema(spec)!r}, sep={infile.get('dlm', ' ')!r}, header={infile['firstobs'] == 2!r}){_date_cols(spec)}")
                if not infile.get("dlm"):
                    report.warn("sas", "INFILE without DLM= splits on single spaces in Spark; SAS splits on any run of blanks")
            continue
        if w in ("datalines", "cards", "datalines4") and code is None:
            if spec is None:
                raise Unsupported("DATALINES without an INPUT statement")
            data = em.datalines.get(body[idx + 1].strip()) if idx + 1 < len(body) else None
            if data is None:
                raise Unsupported("DATALINES block not found")
            code = started(datalines_frame(data, spec, infile, report))
            skip_next = True
            continue
        if code is None:
            if w in ("datalines", "cards", "infile", "input"):
                raise Unsupported("DATALINES/INFILE/INPUT form not supported: read the file with spark.read.csv and set a schema")
            if idx in skip:
                continue
            raise Unsupported("DATA step without a SET, MERGE, INFILE or DATALINES statement")
        if idx in actions:
            if if_state:
                code += flush(if_state[0], if_state[1], None)
                if_state = None
            code += actions[idx]
            continue
        if idx in skip:
            continue
        if w in ("array", "do", "end", "return", "call", "output"):
            raise Unsupported(f"{w.upper()} depends on row-by-row processing; rewrite with Window functions or groupBy")
        em.cond = w in ("if", "else")
        m_if = re.match(r"(?is)if\s+(.*?)\s+then\s+(.*)$", st.strip())
        m_else_if = re.match(r"(?is)else\s+if\s+(.*?)\s+then\s+(.*)$", st.strip())
        m_else = re.match(r"(?is)else\s+(.*)$", st.strip())
        def assign(txt):
            m = re.match(r"(?is)(\w+)\s*=\s*(.*)$", txt.strip())
            if not m:
                if re.match(r"(?is)delete$", txt.strip()):
                    return ("delete", None)
                raise Unsupported(f"unsupported THEN action `{txt}`")
            return m.group(1), em.code(m.group(2))
        if m_else_if and if_state:
            v, val = assign(m_else_if.group(2))
            if v != if_state[0]:
                raise Unsupported("IF/ELSE IF chain assigning different variables")
            if_state[1].append((em.code(m_else_if.group(1)), val))
            continue
        if m_else and if_state:
            v, val = assign(m_else.group(1))
            if v != if_state[0]:
                raise Unsupported("IF/ELSE assigning different variables")
            chain_part = flush(if_state[0], if_state[1], val)
            code += chain_part
            if_state = None
            continue
        if if_state:                       # chain ended with no ELSE
            code += flush(if_state[0], if_state[1], None)
            if_state = None
        if m_if:
            cond = em.code(m_if.group(1))
            v, val = assign(m_if.group(2))
            if v == "delete":
                code += f".filter(~{cond})"
            else:
                if_state = (v, [(cond, val)], None)
            continue
        if w == "if":                      # subsetting IF
            code += f".filter({strip_parens(em.code(st.strip()[2:]))})"
        elif w == "where":
            code += f".filter({strip_parens(em.code(st.strip()[5:]))})"
        elif w == "keep":
            keep = st.split()[1:]
        elif w == "drop":
            drop = st.split()[1:]
        elif w == "rename":
            for a, b in re.findall(r"(\w+)\s*=\s*(\w+)", st):
                code += f".withColumnRenamed({a!r}, {b!r})"
        elif w in ("length", "format", "informat", "label", "attrib"):
            report.note(f"{w.upper()} statement ignored in DATA step")
        elif re.match(r"(?is)\w+\s*=", st.strip()):
            m = re.match(r"(?is)(\w+)\s*=\s*(.*)$", st.strip())
            code += f".withColumn({m.group(1)!r}, {strip_parens(em.code(m.group(2)))})"
        else:
            raise Unsupported(f"DATA step statement `{st[:40]}` not understood")
    em.cond = False
    if if_state:
        code += flush(if_state[0], if_state[1], None)
    if code is None:
        raise Unsupported("empty DATA step")
    if out_filter:
        em.cond = True
        code += f".filter({strip_parens(em.code(out_filter))})"
        em.cond = False
    if drop:
        code += ".drop(" + ", ".join(repr(x) for x in drop) + ")"
    if keep:
        code += ".select(" + ", ".join(repr(x) for x in keep) + ")"
    elif drop_cols:
        code += ".drop(" + ", ".join(repr(x) for x in drop_cols) + ")"
    if keep is None and drop_cols and False:
        pass
    return nxt, [f"{outname} = {format_chain(code)}"]


def proc(stmts, i, em, report):
    s = stmts[i]
    kind = s.split()[1].lower()
    opts = s
    data = re.search(r"(?i)\bdata\s*=\s*(\S+)", opts)
    din = dsname(data.group(1)) if data else "_last_"
    if data:
        em.inputs.append(data.group(1))
    outm = re.search(r"(?i)\bout\s*=\s*(\S+)", opts)
    if kind == "sql":
        body, nxt = block(stmts, i)
        return nxt, proc_sql(body, report)
    body, nxt = block(stmts, i)
    stm = {b.split()[0].lower(): b for b in body if b}
    if kind == "sort":
        by = stm.get("by", "")
        toks = by.split()[1:]
        cols, k = [], 0
        while k < len(toks):
            if toks[k].lower() == "descending":
                cols.append(f"F.col({toks[k + 1]!r}).desc()")
                k += 2
            else:
                cols.append(f"F.col({toks[k]!r}).asc()")
                k += 1
        name = dsname(outm.group(1)) if outm else din
        code = f"{din}"
        keys = [t for t in toks if t.lower() != "descending"]
        if re.search(r"(?i)\bnodupkey\b", opts):
            report.warn("sas", "NODUPKEY keeps the first row per key in SAS; dropDuplicates keeps an arbitrary one")
            code += f".dropDuplicates({keys!r})"
        elif re.search(r"(?i)\bnodup(?:licates)?\b", opts):
            code += ".distinct()"
        code += f".orderBy({', '.join(cols)})"
        return nxt, [f"{name} = {code}"]
    if kind in ("means", "summary"):
        cls = stm.get("class", stm.get("by", "")).split()[1:]
        var = stm.get("var", "").split()[1:]
        outst = stm.get("output")
        if not outst:
            return nxt, [f"{din}.select({', '.join(repr(v) for v in var)}).describe().show()" if var else f"{din}.describe().show()"]
        oname = re.search(r"(?i)\bout\s*=\s*(\S+)", outst)
        pairs = re.findall(r"(?i)\b(mean|sum|min|max|n|std|median|var|nmiss)\s*=\s*([\w\s]+?)(?=\s+(?:mean|sum|min|max|n|std|median|var|nmiss|out)\s*=|$)", outst)
        if not oname or not pairs:
            raise Unsupported("PROC MEANS OUTPUT needs `out=` and `stat=name` pairs (AUTONAME not supported)")
        aggs = []
        for stat, names in pairs:
            fn = AGG_STATS[stat.lower()]
            if fn is None:
                raise Unsupported("NMISS statistic")
            nm = names.split()
            if stat.lower() == "n" and len(nm) == 1 and len(var) != 1:
                aggs.append(f"F.count(F.lit(1)).alias({nm[0]!r})")
                continue
            if len(nm) != len(var):
                raise Unsupported(f"{stat}= names do not line up with VAR list")
            aggs += [f"F.{fn}({v!r}).alias({n!r})" for v, n in zip(var, nm)]
        base = f"{din}.groupBy({', '.join(repr(c) for c in cls)})" if cls else f"{din}"
        report.note("PROC MEANS with CLASS also outputs overall (_TYPE_=0) rows in SAS; only per-group rows are produced here")
        return nxt, [f"{dsname(oname.group(1))} = {base}.agg({', '.join(aggs)})"]
    if kind == "freq":
        tbl = stm.get("tables", stm.get("table", ""))
        if not tbl:
            raise Unsupported("PROC FREQ without TABLES")
        expr = re.sub(r"(?i)^tables?\s+", "", tbl).split("/")[0].strip()
        cols = [c for c in re.split(r"\*|\s+", expr) if c]
        oo = re.search(r"(?i)\bout\s*=\s*(\S+)", tbl)
        code = f"{din}.groupBy({', '.join(repr(c) for c in cols)}).count()"
        return nxt, [f"{dsname(oo.group(1))} = {code}" if oo else f"{code}.show()"]
    if kind == "print":
        ob = re.search(r"(?i)obs\s*=\s*(\d+)", opts)
        return nxt, [f"{din}.show({ob.group(1)})" if ob else f"{din}.show()"]
    if kind == "import":
        f = re.search(r"(?i)(?:datafile|file)\s*=\s*(\"[^\"]*\"|'[^']*')", opts)
        name = dsname(outm.group(1)) if outm else "imported"
        if not f:
            raise Unsupported("PROC IMPORT without DATAFILE=")
        return nxt, [f"{name} = spark.read.csv({f.group(1)[1:-1]!r}, header=True, inferSchema=True)"]
    if kind == "export":
        f = re.search(r"(?i)outfile\s*=\s*(\"[^\"]*\"|'[^']*')", opts)
        if not f:
            raise Unsupported("PROC EXPORT without OUTFILE=")
        report.note("Spark writes a *directory* of part files, not a single file")
        return nxt, [f"{din}.write.mode('overwrite').csv({f.group(1)}, header=True)"]
    raise Unsupported(f"PROC {kind.upper()} has no automatic conversion")


def proc_sql(body, report):
    from .sql_conv import convert_sql
    lines = []
    for st in body:
        st = re.sub(r"(?i)\b(work|temp)\.", "", st)
        if re.search(r"(?i)\bcalculated\b", st):
            raise Unsupported("CALCULATED references: repeat the expression or use a subquery")
        m = re.match(r"(?is)create\s+table\s+([\w.]+)\s+as\s+(select.*)$", st.strip())
        if m:
            name = m.group(1).replace(".", "_")
            sql = convert_sql(m.group(2), report, dialect="generic", out="sql").rstrip().rstrip(";")
            lines += [f'{name} = spark.sql("""', sql, '""")', f"{name}.createOrReplaceTempView({name!r})"]
        elif re.match(r"(?is)select\b", st.strip()):
            sql = convert_sql(st, report, dialect="generic", out="sql").rstrip().rstrip(";")
            lines += ['spark.sql("""', sql, '""").show()']
        else:
            raise Unsupported(f"PROC SQL statement `{st.split()[0].upper()}` not supported")
    return lines
