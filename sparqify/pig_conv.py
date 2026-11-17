"""Apache Pig Latin (.pig) -> PySpark.

LOAD, FILTER, FOREACH ... GENERATE (incl. after GROUP), GROUP, JOIN (inner/left/right/full), ORDER, DISTINCT, LIMIT, UNION, STORE, DUMP.
Expressions go through expr_xlate (lang "pig"). UDFs, nested FOREACH blocks, COGROUP, FLATTEN, SPLIT and macros become TODOs.
"""
from __future__ import annotations

import re

from .core import Report, Unsupported, cm
from .expr_xlate import translate

_AGG = {"SUM": "F.sum", "AVG": "F.avg", "MIN": "F.min", "MAX": "F.max", "COUNT": "F.count", "COUNT_STAR": "F.count"}


def _statements(src: str):
    src = re.sub(r"/\*.*?\*/", " ", src, flags=re.S)
    src = re.sub(r"--[^\n]*", " ", src)
    out, buf, q = [], [], None
    for ch in src:
        if q:
            buf.append(ch)
            if ch == q:
                q = None
        elif ch in "'\"":
            q = ch
            buf.append(ch)
        elif ch == ";":
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if "".join(buf).strip():
        out.append("".join(buf).strip())
    return [s for s in out if s]


def _split_top(s: str):
    parts, depth, cur, q = [], 0, [], None
    for ch in s:
        if q:
            cur.append(ch)
            if ch == q:
                q = None
            continue
        if ch in "'\"":
            q = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur).strip())
    return parts


_TYPES = {"int": "int", "long": "long", "float": "float", "double": "double", "chararray": "string", "boolean": "boolean",
          "datetime": "timestamp", "bytearray": "string", "biginteger": "decimal(38,0)", "bigdecimal": "decimal(38,10)"}


def convert_pig(source: str, report: Report) -> str:
    lines = ["from pyspark.sql import SparkSession, functions as F", "", 'spark = SparkSession.builder.appName("pig_script").getOrCreate()', ""]
    known = set()
    groups = {}     # alias -> (source alias, [group cols] or None)
    stmts = _statements(source)
    if not stmts:
        report.todo("pig", "no statements found")
    for n, st in enumerate(stmts, 1):
        where = f"stmt {n}"
        low = st.lower()
        m = re.match(r"(?is)^(\w+)\s*=\s*(.*)$", st)
        if re.match(r"(?i)^(set|register|define|import|%declare|%default)\b", st):
            report.note(f"{st.split()[0].upper()} statement ignored: {st[:60]}")
            continue
        if re.match(r"(?is)^dump\s+(\w+)$", st):
            a = re.match(r"(?is)^dump\s+(\w+)$", st).group(1)
            lines.append(f"{a}.show(truncate=False)")
            continue
        m2 = re.match(r"(?is)^store\s+(\w+)\s+into\s+'([^']*)'(?:\s+using\s+(\w+)\s*\(([^)]*)\))?$", st)
        if m2:
            a, path, fn, args = m2.groups()
            sep = ","
            if args and args.strip():
                sep = args.strip().strip("'\"")
            elif fn in (None, "PigStorage"):
                sep = "\t"
            if fn and fn not in ("PigStorage",):
                report.todo(where, f"STORE ... USING {fn} not converted")
                lines.append(f"# TODO(sparqify): STORE {cm(a)} USING {cm(fn)}")
            else:
                lines.append(f"{a}.write.mode('overwrite').csv({path!r}, sep={sep!r}, header=False)")
                report.note(f"{where}: Spark writes a directory of part files, not a single file")
            continue
        if not m:
            report.todo(where, f"statement not understood: {st[:70]}")
            lines.append(f"# TODO(sparqify): {cm(' '.join(st.split())[:100])}")
            continue
        alias, rhs = m.group(1), m.group(2).strip()
        verb = rhs.split(None, 1)[0].upper() if rhs else ""
        try:
            if verb == "LOAD":
                lm = re.match(r"(?is)^load\s+'([^']*)'(?:\s+using\s+(\w+)\s*\(([^)]*)\))?(?:\s+as\s*\((.*)\))?$", rhs)
                if not lm:
                    raise Unsupported("LOAD form")
                path, fn, args, schema = lm.groups()
                if fn and fn != "PigStorage":
                    report.todo(where, f"LOAD USING {fn} not converted")
                    lines.append(f"# TODO(sparqify): {cm(alias)} = LOAD {path!r} USING {cm(fn)}")
                    continue
                sep = (args or "").strip().strip("'\"") or "\t"
                lines.append(f"{alias} = spark.read.csv({path!r}, sep={sep!r}, header=False, inferSchema=True)")
                if schema:
                    cols, casts = [], []
                    for f in _split_top(schema):
                        nm, _, ty = f.partition(":")
                        cols.append(nm.strip())
                        casts.append(_TYPES.get(ty.strip().lower()))
                    lines.append(f"{alias} = {alias}.toDF({', '.join(repr(c) for c in cols)})")
                    cc = [f"F.col({c!r}).cast({t!r})" for c, t in zip(cols, casts) if t]
                    if cc:
                        lines.append(f"{alias} = {alias}.select(" + ", ".join(f"F.col({c!r}).cast({t!r}).alias({c!r})" if t else f"F.col({c!r})" for c, t in zip(cols, casts)) + ")")
                else:
                    report.warn(where, f"{alias}: LOAD without a schema - columns are _c0, _c1, ...")
            elif verb == "FILTER":
                fm = re.match(r"(?is)^filter\s+(\w+)\s+by\s+(.*)$", rhs)
                lines.append(f"{alias} = {fm.group(1)}.filter({translate(fm.group(2), 'pig', None, lambda w: report.warn(where, w))})")
            elif verb == "DISTINCT":
                lines.append(f"{alias} = {rhs.split()[1]}.distinct()")
            elif verb == "LIMIT":
                parts = rhs.split()
                lines.append(f"{alias} = {parts[1]}.limit({int(parts[2])})")
            elif verb == "UNION":
                names = [x.strip() for x in rhs[5:].split(",")]
                if names and names[0].upper().startswith("ONSCHEMA"):
                    names[0] = names[0].split(None, 1)[1]
                lines.append(f"{alias} = {names[0]}" + "".join(f".unionByName({x})" for x in names[1:]))
            elif verb == "ORDER":
                om = re.match(r"(?is)^order\s+(\w+)\s+by\s+(.*)$", rhs)
                ks = []
                for k in _split_top(om.group(2)):
                    kp = k.split()
                    ks.append(f"F.col({kp[0].split('::')[-1]!r}).{'desc' if len(kp) > 1 and kp[1].upper() == 'DESC' else 'asc'}()")
                lines.append(f"{alias} = {om.group(1)}.orderBy({', '.join(ks)})")
            elif verb == "GROUP":
                gm = re.match(r"(?is)^group\s+(\w+)\s+(all|by\s+(.*?))(?:\s+parallel\s+\d+)?$", rhs)
                if not gm:
                    raise Unsupported("GROUP form")
                src_a = gm.group(1)
                if gm.group(2).lower() == "all":
                    groups[alias] = (src_a, [])
                else:
                    key = gm.group(3).strip()
                    if key.startswith("(") and key.endswith(")"):
                        key = key[1:-1]
                    cols = [k.strip().split("::")[-1] for k in _split_top(key)]
                    if not all(re.fullmatch(r"\w+", c) for c in cols):
                        raise Unsupported("GROUP BY expression (only plain columns)")
                    groups[alias] = (src_a, cols)
                lines.append(f"{alias} = {src_a}  # grouped by {cm(groups[alias][1] or 'ALL')}: the next FOREACH aggregates it")
            elif verb == "FOREACH":
                fm = re.match(r"(?is)^foreach\s+(\w+)\s+generate\s+(.*)$", rhs)
                if not fm:
                    raise Unsupported("nested FOREACH block")
                srcn, gen = fm.groups()
                items = _split_top(gen)
                if srcn in groups:
                    base, gcols = groups[srcn]
                    aggs, sel = [], []
                    for it in items:
                        im = re.match(r"(?is)^(.*?)(?:\s+as\s+(\w+))?$", it)
                        ex, nm = im.group(1).strip(), im.group(2)
                        if ex.lower() == "group":
                            sel.append(gcols)
                            continue
                        am = re.match(r"(?is)^(\w+)\s*\(\s*(?:(\w+)\.(\w+)|(\w+))?\s*\)$", ex)
                        if not am or am.group(1).upper() not in _AGG:
                            raise Unsupported(f"expression after GROUP: {ex}")
                        f_, arg = am.group(1).upper(), am.group(3)
                        label = nm or f"{f_.lower()}_{arg or 'all'}"
                        if f_ in ("COUNT_STAR",) or (f_ == "COUNT" and not arg):
                            aggs.append(f"F.count(F.lit(1)).alias({label!r})")
                        else:
                            aggs.append(f"{_AGG[f_]}(F.col({arg!r})).alias({label!r})")
                    gb = ", ".join(repr(c) for c in gcols)
                    lines.append(f"{alias} = {base}.groupBy({gb}).agg({', '.join(aggs)})" if gcols else f"{alias} = {base}.agg({', '.join(aggs)})")
                else:
                    cols = []
                    for it in items:
                        im = re.match(r"(?is)^(.*?)(?:\s+as\s+(\w+))?$", it)
                        ex, nm = im.group(1).strip(), im.group(2)
                        if re.search(r"(?i)\bflatten\s*\(", ex):
                            raise Unsupported("FLATTEN")
                        if ex == "*":
                            cols.append("'*'")
                            continue
                        code = translate(ex, "pig", None, lambda w: report.warn(where, w))
                        if code.startswith("F.col(") and code.count("(") == 1 and not nm:
                            cols.append(code)
                        else:
                            cols.append(f"{code}.alias({(nm or 'expr' + str(len(cols))) !r})")
                    lines.append(f"{alias} = {srcn}.select({', '.join(cols)})")
            elif verb == "JOIN":
                jm = re.match(r"(?is)^join\s+(.*)$", rhs)
                body = re.sub(r"(?i)\s+using\s+'\w+'|\s+parallel\s+\d+", "", jm.group(1))
                sides = _split_top(body)
                parsed = []
                for sd in sides:
                    sm = re.match(r"(?is)^(\w+)\s+by\s+\(?(.*?)\)?(?:\s+(left|right|full)(?:\s+outer)?)?$", sd.strip())
                    if not sm:
                        raise Unsupported("JOIN form")
                    parsed.append((sm.group(1), [c.strip().split("::")[-1] for c in sm.group(2).split(",")], sm.group(3)))
                if len(parsed) != 2:
                    raise Unsupported("JOIN of more than two relations")
                (a, ka, ha), (b, kb, hb) = parsed
                how = {"left": "left", "right": "right", "full": "outer"}.get((ha or hb or "").lower(), "inner")
                if ka == kb:
                    lines.append(f"{alias} = {a}.join({b}, on={ka!r}, how={how!r})")
                else:
                    cond = " & ".join(f"({a}[{x!r}] == {b}[{y!r}])" for x, y in zip(ka, kb))
                    lines.append(f"{alias} = {a}.join({b}, {cond}, {how!r})")
                report.warn(where, f"{alias}: Pig prefixes joined columns as rel::col; same-named columns are ambiguous here")
            else:
                raise Unsupported(f"{verb or 'statement'} not supported")
        except Unsupported as ex:
            report.todo(where, f"{alias}: {ex}: {' '.join(st.split())[:80]}")
            lines.append(f"# TODO(sparqify): {cm(alias)} = {cm(' '.join(rhs.split())[:100])}")
            if verb in ("FILTER", "ORDER", "DISTINCT", "LIMIT", "FOREACH"):
                parts = rhs.split()
                if len(parts) > 1 and re.fullmatch(r"\w+", parts[1]):
                    lines.append(f"{alias} = {parts[1]}")
        known.add(alias)
    return "\n".join(lines) + "\n"
