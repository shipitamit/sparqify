"""Scala collections / Java streams -> Spark RDD code (Scala RDD API / Java JavaRDD API).

Pattern based: collection variables defined from literals / files become RDDs, and method chains rooted at
them are rewritten method by method. Everything else in the file is left untouched.
"""
from __future__ import annotations

import bisect
import re

from .core import Report

OPEN, CLOSE = {"(": ")", "{": "}", "[": "]"}, {")", "}", "]"}


def match(text, i):
    """index of the bracket closing text[i]"""
    depth, q = 0, None
    for j in range(i, len(text)):
        ch = text[j]
        if q:
            if ch == "\\":
                continue
            if ch == q and text[j - 1] != "\\":
                q = None
            continue
        if ch in "\"'":
            q = ch
        elif ch in OPEN:
            depth += 1
        elif ch in CLOSE:
            depth -= 1
            if depth == 0:
                return j
    return len(text) - 1


def parse_chain(text, i, scala):
    """parse `.name(args)(args)...` calls starting at i -> ([(name, [groups])], end)"""
    calls = []
    pat = re.compile(r"\s*\.\s*([A-Za-z_]\w*)")
    while True:
        m = pat.match(text, i)
        if not m:
            break
        name, i = m.group(1), m.end()
        groups = []
        while True:
            k = i
            while scala and k < len(text) and text[k] in " \t":
                k += 1
            if k < len(text) and (text[k] == "(" or (scala and text[k] == "{")):
                j = match(text, k)
                groups.append(text[k + 1:j].strip())
                i = j + 1
                if not scala:
                    break
            else:
                break
        calls.append((name, groups))
    return calls, i


_STATIC = {"Integer": {"parseInt", "valueOf", "toString", "signum", "bitCount"}, "Long": {"parseLong", "valueOf", "toString", "signum"},
           "Double": {"parseDouble", "valueOf", "toString", "isNaN"}, "Boolean": {"parseBoolean", "valueOf"}, "Character": None,
           "Math": None, "Objects": None, "String": {"valueOf", "format"}, "Arrays": None, "Collections": None}


def first_arg(text):
    """first top-level argument of an argument list"""
    depth, q = 0, None
    for i, ch in enumerate(text):
        if q:
            if ch == q and text[i - 1] != "\\":
                q = None
        elif ch in "\"'":
            q = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            return text[:i].strip(), True
    return text.strip(), False


def lam(g, scala=True):
    """(param, body) of a lambda / placeholder expression / function name"""
    g = g.strip()
    if g == "identity":
        return "x_", "x_"
    if "=>" in g and scala:
        p, b = g.split("=>", 1)
        return p.strip().strip("()").split(":")[0].strip(), b.strip()
    if "->" in g and not scala:
        p, b = g.split("->", 1)
        return p.strip().strip("()"), b.strip()
    if "::" in g and not scala:
        left, meth = (x.strip() for x in g.split("::", 1))
        if not re.fullmatch(r"[A-Za-z_][\w.]*", left) or not re.fullmatch(r"[A-Za-z_]\w*", meth):
            return "x", f"({g}).apply(x)"
        if left[0].islower() and "." not in left:               # obj::method / this::method
            return "x", f"{left}.{meth}(x)"
        if left in _STATIC and (_STATIC[left] is None or meth in _STATIC[left]):
            return "x", f"{left}.{meth}(x)"
        return "x", f"x.{meth}()"
    if scala and "_" in g:
        return "x_", re.sub(r"(?<![\w])_(?![\w])", "x_", g)
    return ("x_" if scala else "x"), f"{g}({'x_' if scala else 'x'})"


class Scala:
    RDD = {"map", "filter", "flatMap", "distinct", "union", "zipWithIndex", "sortBy", "reduceByKey", "sample",
           "filterNot", "sorted", "groupBy", "keys", "values", "mapValues", "join", "zip", "cache"}

    def __init__(self, report):
        self.r = report

    def rewrite(self, root, calls, line):
        cur, rdd, i = root, True, 0
        while i < len(calls):
            name, g = calls[i]
            a = g[0] if g else ""
            if not rdd:
                cur += f".{name}" + "".join(f"({x})" for x in g)
                i += 1
                continue
            nxt = calls[i + 1] if i + 1 < len(calls) else None
            if name == "groupBy" and nxt and nxt[0] in ("mapValues", "map") and nxt[1] and re.search(r"(_\.|v\.)(size|length)\b", nxt[1][0]):
                p, b = lam(a)
                cur += f".map({p} => ({b}, 1)).reduceByKey(_ + _)"
                i += 2
                continue
            if name in ("map", "filter", "flatMap", "distinct", "union", "keys", "values", "cache", "mapValues", "reduceByKey", "join"):
                cur += f".{name}" + (f"({a})" if g else ("" if name in ("keys", "values") else "()"))
            elif name == "zipWithIndex":
                self.r.warn(line, "zipWithIndex on an RDD pairs with a Long index (a Scala collection gives an Int)")
                cur += ".zipWithIndex()"
            elif name == "zip":
                self.r.warn(line, "RDD.zip needs both RDDs to have the same number of partitions and the same number of elements in each")
                cur += f".zip({a})"
            elif name == "collect" and g:
                cur += f".collect {{ {a} }}"
            elif name == "collect":
                cur += ".collect()"; rdd = False
            elif name == "filterNot":
                p, b = lam(a)
                cur += f".filter({p} => !({b}))"
            elif name in ("sortBy", "sorted"):
                asc = True
                if nxt and nxt[0] == "reverse" and not nxt[1]:
                    asc = False
                    i += 1
                key = a if (name == "sortBy" and g) else "identity"
                cur += f".sortBy({key}" + ("" if asc else ", ascending = false") + ")"
            elif name == "reverse":
                self.r.todo(line, "reverse on an RDD: sort descending instead (sortBy(f, ascending = false))")
                cur += ".reverse /* TODO(sparqify) */"
            elif name == "groupBy":
                self.r.warn(line, "RDD.groupBy returns RDD[(K, Iterable[T])] (not a Map); prefer reduceByKey/aggregateByKey")
                cur += f".groupBy({a})"
            elif name in ("size", "length"):
                self.r.warn(line, "count() returns a Long where size/length returned an Int")
                cur += ".count()"; rdd = False
            elif name == "count":
                cur += f".filter({a}).count()" if g else ".count()"; rdd = False
            elif name == "sum":
                self.r.warn(line, "RDD.sum() returns a Double (a collection's sum keeps its Int/Long type): convert with .toInt / .toLong if needed")
                cur += ".sum()"; rdd = False
            elif name == "head":
                cur += ".first()"; rdd = False
            elif name == "headOption":
                cur += ".take(1).headOption"; rdd = False
            elif name == "find":
                cur += f".filter({a}).take(1).headOption"; rdd = False
            elif name == "drop" and g:
                self.r.warn(line, "drop(n) -> zipWithIndex filter: relies on the RDD's current element order")
                cur += f".zipWithIndex().filter(_._2 >= {a}).map(_._1)"
            elif name in ("max", "min"):
                cur += f".{name}()"; rdd = False
            elif name in ("toList", "toSeq", "toVector", "toSet"):
                cur += f".collect().{name}"; rdd = False
            elif name == "toArray":
                cur += ".collect()"; rdd = False
            elif name == "toMap":
                cur += ".collectAsMap()"; rdd = False
            elif name == "countByKey":
                cur += ".countByKey()"; rdd = False
            elif name == "mkString":
                cur += (".collect().mkString(" + a + ")") if g else ".collect().mkString"; rdd = False
            elif name == "take":
                cur += f".take({a})"; rdd = False
            elif name == "reduce":
                cur += f".reduce({a})"; rdd = False
            elif name in ("foldLeft", "fold"):
                self.r.warn(line, "foldLeft -> fold: the zero element must be neutral and the function associative/commutative")
                cur += f".fold({a})({g[1]})" if len(g) > 1 else f".fold({a})"; rdd = False
            elif name == "exists":
                cur += f".filter({a}).isEmpty() == false"; rdd = False
            elif name == "forall":
                p, b = lam(a)
                cur += f".filter({p} => !({b})).isEmpty()"; rdd = False
            elif name == "isEmpty":
                cur += ".isEmpty()"; rdd = False
            elif name == "nonEmpty":
                cur += ".isEmpty() == false"; rdd = False
            elif name == "foreach":
                self.r.warn(line, "foreach on an RDD runs on the executors - println output / mutations will not appear on the driver")
                cur += f".foreach({a})"; rdd = False
            elif name == "countByValue":
                cur += ".countByValue()"; rdd = False
            else:
                self.r.todo(line, f"`.{name}` on a Scala collection has no automatic RDD equivalent")
                cur += f".{name}" + "".join(f"({x})" for x in g) + " /* TODO(sparqify) */"
            i += 1
        return cur, rdd


class Java:
    def __init__(self, report):
        self.r = report

    def rewrite(self, root, calls, line):
        cur, rdd = root, True
        i = 0
        while i < len(calls):
            name, g = calls[i]
            a = g[0] if g else ""
            if not rdd:
                cur += f".{name}({', '.join(g)})"
                i += 1
                continue
            if name in ("filter", "distinct"):
                cur += f".{name}({a})"
            elif name == "map":
                cur += f".map({a})"
            elif name == "flatMap":
                p, b = lam(a, scala=False)
                b = re.sub(r"\.stream\(\)\s*$", ".iterator()", b)
                if not b.endswith(".iterator()"):
                    b = f"({b}).iterator()"
                cur += f".flatMap({p} -> {b})"
            elif name == "sorted" and not a:
                self.r.warn(line, "sorted() -> sortBy(x -> x, true, numPartitions): set the partition count")
                cur += ".sortBy(x -> x, true, 1)"
            elif name in ("mapToInt", "mapToDouble", "mapToLong"):
                nxt = calls[i + 1][0] if i + 1 < len(calls) else None
                if nxt == "sum":
                    self.r.warn(line, f"{name}(...).sum() -> mapToDouble(...).sum() converted back with intValue()/longValue() (Spark sums as double: large longs lose precision)")
                    conv = {"mapToInt": ".intValue()", "mapToLong": ".longValue()", "mapToDouble": ""}[name]
                    cur += f".mapToDouble({a}).sum(){conv}"
                    rdd = False
                    i += 2
                    continue
                cur += f".map({a})"
            elif name == "boxed" and not a:
                pass
            elif name == "mapToObj":
                cur += f".map({a})"
            elif name == "count":
                cur += ".count()"; rdd = False
            elif name == "limit":
                nxt = calls[i + 1] if i + 1 < len(calls) else None
                if nxt and nxt[0] == "toList" or (nxt and nxt[0] == "collect" and re.fullmatch(r"Collectors\.toList\(\)", re.sub(r"\s+", "", nxt[1][0] if nxt[1] else ""))):
                    cur += f".take({a})"; rdd = False
                    i += 2
                    continue
                self.r.warn(line, "limit(n) followed by more operations: the first n rows are brought to the driver and re-parallelised")
                cur = f"jsc.parallelize({cur}.take({a}))"
            elif name == "skip":
                self.r.warn(line, "skip(n) -> zipWithIndex filter: relies on the RDD's current element order")
                cur += f".zipWithIndex().filter(t -> t._2() >= {a}).map(t -> t._1())"
            elif name == "reduce":
                parts = _split_top(a)
                if len(parts) == 1:
                    self.r.warn(line, "reduce(f) -> Optional.of(rdd.reduce(f)): an empty RDD throws (a stream returns an empty Optional)")
                    cur = f"Optional.of({cur}.reduce({a}))"
                else:
                    cur += f".fold({parts[0]}, {parts[1]})"
                rdd = False
            elif name == "anyMatch":
                cur += f".filter({a}).isEmpty() == false"; rdd = False
            elif name == "noneMatch":
                cur += f".filter({a}).isEmpty()"; rdd = False
            elif name == "allMatch":
                p, b = lam(a, scala=False)
                cur += f".filter({p} -> !({b})).isEmpty()"; rdd = False
            elif name == "findFirst" and not a:
                cur += ".take(1).stream().findFirst()"; rdd = False
            elif name in ("max", "min") and a:
                self.r.warn(line, f"{name}(comparator): Spark serialises the comparator, so it must be Serializable (Comparator.naturalOrder() is; a plain lambda is not)")
                cur = f"Optional.of({cur}.{name}({a}))"; rdd = False
                self.r.warn(line, f"{name}(...) on an empty RDD throws (a stream returns an empty Optional)")
            elif name == "sorted" and a:
                sk = self.sort_key(a)
                if sk is None:
                    self.r.todo(line, f"sorted({a[:40]}) has no automatic conversion: use sortBy(key, ascending, numPartitions)")
                    cur += f".sorted({a}) /* TODO(sparqify) */"
                else:
                    self.r.warn(line, "sorted(...) -> sortBy(key, ascending, 1): set the partition count")
                    cur += f".sortBy({sk[0]}, {sk[1]}, 1)"
            elif name == "forEach":
                self.r.warn(line, "forEach -> collect().forEach on the driver (keeps the output order and avoids shipping System.out to executors): use foreach(...) for big data")
                cur += f".collect().forEach({a})"; rdd = False
            elif name == "toList":
                cur += ".collect()"; rdd = False
            elif name == "collect":
                cur, rdd = self.collector(cur, a, line)
            else:
                self.r.todo(line, f"`.{name}()` on a Java stream has no automatic RDD equivalent")
                cur += f".{name}({', '.join(g)}) /* TODO(sparqify) */"
            i += 1
        return cur, rdd

    def sort_key(self, a):
        """Comparator expression -> (key lambda, ascending) when it is one of the simple forms"""
        a = re.sub(r"\s+", " ", a.strip())
        if a == "Comparator.naturalOrder()":
            return "x -> x", "true"
        if a == "Comparator.reverseOrder()" or a == "Collections.reverseOrder()":
            return "x -> x", "false"
        m = re.fullmatch(r"Comparator\.comparing\((.+?)\)(\.reversed\(\))?", a)
        if m and not _split_top(m.group(1))[1:]:
            p, b = lam(m.group(1), scala=False)
            return f"{p} -> {b}", "false" if m.group(2) else "true"
        return None

    def collector(self, cur, a, line):
        a = re.sub(r"\s+", " ", a)
        if re.fullmatch(r"Collectors\.toList\(\)", a):
            return cur + ".collect()", False
        if re.fullmatch(r"Collectors\.toSet\(\)", a):
            return f"new java.util.HashSet<>({cur}.collect())", False
        if re.fullmatch(r"Collectors\.counting\(\)", a):
            return cur + ".count()", False
        m = re.fullmatch(r"Collectors\.joining\((.*)\)", a)
        if m:
            return f"String.join({m.group(1) or chr(34) * 2}, {cur}.collect())", False
        m = re.fullmatch(r"Collectors\.summing(Int|Long|Double)\((.+)\)", a)
        if m:
            conv = {"Int": ".intValue()", "Long": ".longValue()", "Double": ""}[m.group(1)]
            self.r.warn(line, "summing -> mapToDouble(...).sum(): Spark sums as double (large longs lose precision)")
            return f"{cur}.mapToDouble({m.group(2)}).sum(){conv}", False
        m = re.fullmatch(r"Collectors\.groupingBy\((.+), Collectors\.counting\(\)\)", a)
        if m:
            p, b = lam(m.group(1), scala=False)
            return f"{cur}.mapToPair({p} -> new Tuple2<>({b}, 1L)).reduceByKey(Long::sum).collectAsMap()", False
        m = re.fullmatch(r"Collectors\.groupingBy\((.+), Collectors\.summing(Int|Long|Double)\((.+)\)\)", a)
        if m:
            p, b = lam(m.group(1), scala=False)
            p2, b2 = lam(m.group(3), scala=False)
            ref = {"Int": "Integer", "Long": "Long", "Double": "Double"}[m.group(2)]
            if p != p2:
                b2 = re.sub(rf"\b{re.escape(p2)}\b", p, b2)
            return f"{cur}.mapToPair({p} -> new Tuple2<>({b}, {b2})).reduceByKey({ref}::sum).collectAsMap()", False
        m = re.fullmatch(r"Collectors\.groupingBy\(([^,]+)\)", a)
        if m:
            self.r.warn(line, "groupingBy(f) -> groupBy(f).collectAsMap(): the values are Iterable<T> (not List<T>)")
            return f"{cur}.groupBy({m.group(1)}).collectAsMap()", False
        m = re.fullmatch(r"Collectors\.toMap\((.+)\)", a)
        if m:
            parts = _split_top(m.group(1))
            if len(parts) == 2:
                kp, kb = lam(parts[0], scala=False)
                vp, vb = lam(parts[1], scala=False)
                if vp != kp:
                    vb = re.sub(rf"\b{re.escape(vp)}\b", kp, vb)
                self.r.warn(line, "toMap -> mapToPair(...).collectAsMap(): a duplicate key is kept as the last value (Java throws IllegalStateException)")
                return f"{cur}.mapToPair({kp} -> new Tuple2<>({kb}, {vb})).collectAsMap()", False
        self.r.todo(line, f"collector `{a[:50]}` has no automatic conversion")
        return cur + f".collect({a}) /* TODO(sparqify) */", False


def _split_top(text):
    out, depth, q, cur = [], 0, None, []
    for i, ch in enumerate(text):
        if q:
            if ch == q and text[i - 1] != "\\":
                q = None
        elif ch in "\"'":
            q = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
            continue
        cur.append(ch)
    out.append("".join(cur).strip())
    return out


def convert_scala(source: str, report: Report) -> str:
    return _convert(source, report, scala=True)


def convert_java(source: str, report: Report) -> str:
    return _convert(source, report, scala=False)


def _convert(source: str, report: Report, scala: bool) -> str:
    rw = Scala(report) if scala else Java(report)
    text = source
    rdds: set[str] = set()
    out, pos = [], 0
    newlines = [k for k, ch in enumerate(source) if ch == "\n"]          # rewrites never add or remove line breaks
    lines_before = lambda p: bisect.bisect_left(newlines, p) + 1
    assign = re.compile(
        r"(?:(?:val|var)\s+(?P<n>\w+)\s*(?::[^=\n]+)?=\s*)$" if scala else
        r"(?:[\w<>,\s\[\]?]+?\s+(?P<n>\w+)\s*=\s*)$")
    # --- roots
    if scala:
        lit = re.compile(r"(?P<pre>(?:val|var)\s+(?P<n>\w+)\s*(?::[^=\n]+)?=\s*)(?P<root>List|Seq|Array|Vector|Set)\(")
        rng = re.compile(r"(?P<pre>(?:val|var)\s+(?P<n>\w+)\s*(?::[^=\n]+)?=\s*)(?P<root>\(\s*\d+\s+(?:to|until)\s+\w+\s*\))(?:\.toList|\.toSeq)?")
        src_file = re.compile(r"(?P<pre>(?:val|var)\s+(?P<n>\w+)\s*(?::[^=\n]+)?=\s*)(?:scala\.io\.)?Source\.fromFile\((?P<p>[^)\n]{0,300})\)\.getLines(?:\(\))?(?:\.toList|\.toSeq)?(?![\w.(])")
    else:
        lit = re.compile(r"(?P<pre>(?<![\w<>,\[\]?])[\w<>,\[\]?]{1,200}\s+(?P<n>\w+)\s*=\s*)(?P<root>Arrays\.asList|List\.of|new ArrayList<>)\(")

    def wrap_all(t):
        """wrap every list/Seq/... literal assigned to a variable; one left-to-right pass with a linear bracket table"""
        closers, stack, q = {}, [], None
        for k, ch in enumerate(t):
            if q:
                if ch == q and t[k - 1] != "\\":
                    q = None
            elif ch in "\"'":
                q = ch
            elif ch in OPEN:
                stack.append(k)
            elif ch in CLOSE and stack:
                closers[stack.pop()] = k
        out, last = [], 0
        for m in lit.finditer(t):
            j = closers.get(m.end() - 1)
            if j is None or m.start("root") < last:
                continue
            inner = t[m.start("root"):j + 1]
            rdds.add(m.group("n"))
            if scala:
                if m.group("root") == "Set":
                    inner += ".toSeq"
                inner = f"sc.parallelize({inner})"
            out.append(t[last:m.start("root")] + inner)      # java: keep the list; stream() is rewritten later
            last = j + 1
        return "".join(out) + t[last:]

    if scala:
        text = wrap_all(text)
        for m in reversed(list(rng.finditer(text))):
            rdds.add(m.group("n"))
            text = text[:m.start("root")] + f"sc.parallelize({m.group('root').strip()[1:-1].replace(' until ', ' until ')} )".replace("( ", "(").replace(" )", ")") + text[m.end():]
        for m in reversed(list(src_file.finditer(text))):
            rdds.add(m.group("n"))
            path, extra = first_arg(m.group("p"))
            if extra:
                report.warn(lines_before(m.start()), "Source.fromFile(path, encoding): the encoding argument was dropped (sc.textFile reads UTF-8)")
            text = text[:m.start("pre") + len(m.group("pre"))] + f"sc.textFile({path})" + text[m.end():]
        if not rdds:
            report.note("no list/Seq/Array/range/Source.fromFile definitions found - nothing to convert")
    else:
        text = wrap_all(text)
    # --- file roots used directly in a chain:  Source.fromFile(p).getLines().map(..)   /   Files.lines(Paths.get(p)).filter(..)
    exprs = {}

    def expr_root(m):
        name = f"_sparqify_src{len(exprs)}"
        path, extra = first_arg(m.group("p"))
        if extra:
            report.warn(lines_before(m.start()), "file open with extra arguments (encoding): they were dropped (textFile reads UTF-8)")
        exprs[name] = f"sc.textFile({path})" if scala else f"jsc.textFile({path})"
        rdds.add(name)
        return name + m.group("tail")
    if scala:
        text = re.sub(r"(?:scala\.io\.)?Source\.fromFile\((?P<p>[^)\n]{0,300})\)\.getLines(?:\(\))?(?:\.toList|\.toSeq)?(?P<tail>(?=\s*\.))", expr_root, text)
    else:
        text = re.sub(r"Files\.lines\(\s*Paths\.get\((?P<p>[^)\n]{0,300})\)\s*\)(?P<tail>(?=\s*\.))", expr_root, text)
    # --- chains
    root_pat = re.compile(r"(?<![\w.])(\w+)(?![\w])" + ("" if scala else r"(?:\s*\.\s*stream\(\))?"))
    stream_pat = re.compile(r"\s*\.\s*stream\(\)")
    res, pos = "", 0
    for m in root_pat.finditer(text):
        if m.start() < pos or m.group(1) not in rdds:
            continue
        if not scala and m.group(1) not in exprs and not stream_pat.fullmatch(m.group(0)[len(m.group(1)):] or "x"):
            continue
        calls, end = parse_chain(text, m.end(), scala)
        if not calls:
            continue
        line = lines_before(m.start())
        root = exprs.get(m.group(1)) or (m.group(1) if scala else f"jsc.parallelize({m.group(1)})")
        new, is_rdd = rw.rewrite(root, calls, line)
        res += text[pos:m.start()] + new
        pos = end
        prefix = text[max(0, text.rfind("\n", 0, m.start())):m.start()]
        am = assign.search(prefix.lstrip("\n"))
        if is_rdd and am and am.group("n"):
            rdds.add(am.group("n"))
    res += text[pos:]
    for k, v in exprs.items():                                   # a file root that no chain used
        res = res.replace(k, v)
    if scala:
        res = re.sub(r"println\(\s*(\w+)\s*\)", lambda m: f"println({m.group(1)}.collect().toList)" if m.group(1) in rdds else m.group(0), res)
    note = ("// --- sparqify: Scala RDD conversion. Needs `val sc = spark.sparkContext` in scope "
            "(import org.apache.spark.sql.SparkSession).\n" if scala else
            "// --- sparqify: Java RDD conversion. Needs `JavaSparkContext jsc` and imports "
            "org.apache.spark.api.java.*, scala.Tuple2.\n")
    report.note("lambdas are shipped to executors: captured variables are copies, driver-side mutation inside them does not work")
    return note + res
