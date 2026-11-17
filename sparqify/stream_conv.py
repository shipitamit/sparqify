"""Streaming code -> Spark Structured Streaming (PySpark).

Recognised patterns
 1. Kafka consumer loops (kafka-python `for msg in consumer`, confluent-kafka `consumer.poll`) with per-record logic:
    JSON parsing, field access, derived values, if-filters, running counters/sums in dicts, producer.send / print sinks
 2. directory-polling loops (`while True: listdir/glob -> read file with pandas -> process -> to_csv/to_parquet -> sleep`)
 3. legacy Spark DStream pipelines (StreamingContext: socketTextStream, textFileStream, Kafka direct streams; map/flatMap/
    filter/reduceByKey/countByValue/window ops/foreachRDD/pprint)
Per-record Python that cannot be expressed as column logic is kept as a foreachBatch stub that raises until you fill it in.
Schemas are inferred from the fields the code touches (types guessed: numeric if used in arithmetic): review them.
Not run against real Spark."""
from __future__ import annotations

import ast
import re

from .core import Report, Unsupported, cm

_CMP = {ast.Eq: "==", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}
_BIN = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/", ast.Mod: "%"}

HEAD = ("from pyspark.sql import SparkSession, functions as F, types as T\n\n"
        "spark = SparkSession.builder.appName('converted-stream').getOrCreate()\n")


def _rx(sep):
    return sep if re.fullmatch(r"[\w ,;:\-]+", sep) else re.escape(sep)


def _src(n):
    return ast.unparse(n)


def _const(n):
    return n.value if isinstance(n, ast.Constant) else None


# ---------------------------------------------------------------- record-expression translator
class RExpr:
    """Python expression over a parsed record -> Column expression string."""

    def __init__(self, recs, derived, msg=None, numeric=None):
        self.recs, self.derived, self.msg = recs, set(derived), msg
        self.keys, self.numeric = [], numeric if numeric is not None else set()

    def key(self, k, numeric=False):
        if k not in self.keys:
            self.keys.append(k)
        if numeric:
            self.numeric.add(k)
        return f"F.col({k!r})"

    def t(self, n, num=False):
        if isinstance(n, ast.Constant) and isinstance(n.value, (int, float, str, bool)):
            return f"F.lit({n.value!r})"
        if isinstance(n, ast.Subscript) and isinstance(n.value, ast.Name) and n.value.id in self.recs and isinstance(_const(n.slice), str):
            return self.key(_const(n.slice), num)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "get" and isinstance(n.func.value, ast.Name) \
                and n.func.value.id in self.recs and n.args and isinstance(_const(n.args[0]), str):
            c = self.key(_const(n.args[0]), num)
            if len(n.args) > 1:
                return f"F.coalesce({c}, {self.t(n.args[1])})"
            return c
        if isinstance(n, ast.Name):
            if n.id in self.derived:
                return f"F.col({n.id!r})"
            raise Unsupported(f"name {n.id}")
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == self.msg:
            m = {"key": "F.col('key').cast('string')", "topic": "F.col('topic')", "partition": "F.col('partition')",
                 "offset": "F.col('offset')", "timestamp": "F.col('timestamp')"}
            if n.attr in m:
                return m[n.attr]
        if isinstance(n, ast.BinOp) and type(n.op) in _BIN:
            isnum = not isinstance(n.op, ast.Add) or not (isinstance(_const(n.left), str) or isinstance(_const(n.right), str))
            if isinstance(n.op, ast.Add) and not isnum:
                return f"F.concat({self.t(n.left)}, {self.t(n.right)})"
            return f"({self.t(n.left, True)} {_BIN[type(n.op)]} {self.t(n.right, True)})"
        if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.USub):
            return f"(-{self.t(n.operand, True)})"
        if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.Not):
            return f"(~{self.t(n.operand)})"
        if isinstance(n, ast.Compare) and len(n.ops) == 1:
            op = n.ops[0]
            numeric_ctx = isinstance(_const(n.comparators[0]), (int, float)) and not isinstance(_const(n.comparators[0]), bool)
            if type(op) in _CMP:
                return f"({self.t(n.left, numeric_ctx)} {_CMP[type(op)]} {self.t(n.comparators[0])})"
            if isinstance(op, (ast.In, ast.NotIn)) and isinstance(n.comparators[0], (ast.Tuple, ast.List, ast.Set)):
                vals = ", ".join(repr(ast.literal_eval(e)) for e in n.comparators[0].elts)
                r = f"{self.t(n.left)}.isin({vals})"
                return r if isinstance(op, ast.In) else f"(~{r})"
        if isinstance(n, ast.BoolOp):
            return "(" + (" & " if isinstance(n.op, ast.And) else " | ").join(self.t(v) for v in n.values) + ")"
        if isinstance(n, ast.IfExp):
            return f"F.when({self.t(n.test)}, {self.t(n.body)}).otherwise({self.t(n.orelse)})"
        if isinstance(n, ast.JoinedStr):
            parts = []
            for v in n.values:
                if isinstance(v, ast.Constant):
                    parts.append(f"F.lit({v.value!r})")
                elif isinstance(v, ast.FormattedValue):
                    parts.append(f"{self.t(v.value)}.cast('string')")
            return f"F.concat({', '.join(parts)})"
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and not n.keywords:
            base, m = self.t(n.func.value), n.func.attr
            simple = {"upper": "F.upper", "lower": "F.lower", "strip": "F.trim"}
            if m in simple and not n.args:
                return f"{simple[m]}({base})"
            if m in ("startswith", "endswith") and len(n.args) == 1 and isinstance(_const(n.args[0]), str):
                return f"{base}.{m}({_const(n.args[0])!r})"
            if m == "split" and len(n.args) == 1 and isinstance(_const(n.args[0]), str):
                return f"F.split({base}, {_rx(_const(n.args[0]))!r})"
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and len(n.args) == 1 and not n.keywords:
            f = n.func.id
            m = {"len": "F.length", "abs": "F.abs"}
            if f in m:
                return f"{m[f]}({self.t(n.args[0])})"
            if f in ("int", "float", "str"):
                typ = {"int": "int", "float": "double", "str": "string"}[f]
                return f"{self.t(n.args[0], f != 'str')}.cast({typ!r})"
        raise Unsupported(_src(n))


def _schema_code(keys, numeric):
    fields = ", ".join(f"T.StructField({k!r}, T.{'DoubleType' if k in numeric else 'StringType'}())" for k in keys)
    return f"schema = T.StructType([{fields}])  # types guessed from usage: review"


# ---------------------------------------------------------------- 1. Kafka consumer loops
def _kw(call, name):
    for k in call.keywords:
        if k.arg == name:
            return k.value
    return None


def _servers(call):
    v = _kw(call, "bootstrap_servers") or _kw(call, "bootstrap.servers")
    if v is not None:
        c = _const(v)
        if isinstance(c, str):
            return c
        if isinstance(v, (ast.List, ast.Tuple)) and all(isinstance(_const(e), str) for e in v.elts):
            return ",".join(_const(e) for e in v.elts)
        return f"<{_src(v)}>"
    return "localhost:9092"


def kafka_loops(tree, report):
    """-> list of generated code blocks"""
    consumers, producers, topics = {}, {}, {}
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name) and isinstance(n.value, ast.Call):
            f = _src(n.value.func).split(".")[-1]
            if f == "KafkaConsumer":
                consumers[n.targets[0].id] = n.value
            elif f == "Consumer":                                    # confluent
                consumers[n.targets[0].id] = n.value
            elif f in ("KafkaProducer", "Producer"):
                producers[n.targets[0].id] = n.value
    for n in ast.walk(tree):                                          # confluent: consumer.subscribe([...])
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "subscribe" and isinstance(n.func.value, ast.Name) and n.args:
            topics[n.func.value.id] = [_const(e) for e in getattr(n.args[0], "elts", [])]
    out = []
    for loop in [n for n in ast.walk(tree) if isinstance(n, (ast.For, ast.While))]:
        cname = msg = None
        body = loop.body
        if isinstance(loop, ast.For) and isinstance(loop.iter, ast.Name) and loop.iter.id in consumers and isinstance(loop.target, ast.Name):
            cname, msg = loop.iter.id, loop.target.id
        elif isinstance(loop, ast.While):
            for st in loop.body:
                if isinstance(st, ast.Assign) and isinstance(st.value, ast.Call) and isinstance(st.value.func, ast.Attribute) \
                        and st.value.func.attr in ("poll", "consume") and isinstance(st.value.func.value, ast.Name) and st.value.func.value.id in consumers:
                    cname, msg = st.value.func.value.id, st.targets[0].id
                    body = [b for b in loop.body if b is not st]
        if not cname:
            continue
        out.append(_kafka_pipeline(consumers[cname], topics.get(cname), msg, body, producers, report))
    return out


class _Unsup(Exception):
    pass


def _kafka_pipeline(cons, ctopics, msg, body, producers, report):
    # --- source options
    tp = [_const(a) for a in cons.args if isinstance(_const(a), str)] or (ctopics or [])
    if not tp:
        report.todo("kafka", "topic name is not a literal: set the 'subscribe' option")
    sub = ",".join(t for t in tp if t) or "FILL_ME"
    servers = _servers(cons)
    cfg = None
    if cons.args and isinstance(cons.args[0], ast.Dict):                # confluent config dict
        cfg = {_const(k): _const(v) for k, v in zip(cons.args[0].keys, cons.args[0].values)}
        servers = cfg.get("bootstrap.servers", servers)
    off = "latest"
    r = _const(_kw(cons, "auto_offset_reset")) or (cfg or {}).get("auto.offset.reset")
    if r == "earliest":
        off = "earliest"
    if _kw(cons, "group_id") is not None or (cfg and "group.id" in cfg):
        report.note("kafka group_id ignored: Structured Streaming tracks offsets in the checkpoint, not in a consumer group")
    deser = _kw(cons, "value_deserializer")
    json_deser = bool(deser and "json.loads" in _src(deser))
    # --- scan body
    recs, derived, ops, notes = set(), [], [], []
    rx = RExpr(recs, [], msg)
    raw_value = False
    state, sink, todo_stmts, drop = None, None, [], 0
    dicts = {}
    counters = set()
    for st in ast.walk(ast.Module(body=[], type_ignores=[])):
        pass
    # counters declared outside: find names used as counters in body
    def is_counter_update(st):
        if isinstance(st, ast.AugAssign) and isinstance(st.target, ast.Subscript) and isinstance(st.target.value, ast.Name):
            return st.target.value.id, st.target.slice, st.value, isinstance(st.op, ast.Add)
        return None

    def handle(stmts, filters):
        nonlocal state, sink, raw_value
        for st in stmts:
            if isinstance(st, ast.Expr) and isinstance(st.value, ast.Constant):
                continue
            if isinstance(st, (ast.Pass, ast.Continue)):
                continue
            if isinstance(st, ast.If):
                t = st.test
                if isinstance(t, ast.Compare) and isinstance(t.ops[0], ast.Is) and _const(t.comparators[0]) is None:
                    continue                                      # `if msg is None: continue` (confluent)
                if isinstance(t, ast.Call) and isinstance(t.func, ast.Attribute) and t.func.attr == "error":
                    continue
                if st.orelse:
                    todo_stmts.append(st)
                    continue
                try:
                    tc = rx.t(t)
                except Unsupported:
                    todo_stmts.append(st)
                    continue
                handle(st.body, filters + [tc])
                continue
            # value parsing
            if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name):
                v, tgt = st.value, st.targets[0].id
                s = _src(v)
                if re.search(r"json\.loads\(", s) and re.search(rf"\b{msg}\.value", s):
                    recs.add(tgt)
                    continue
                if json_deser and re.fullmatch(rf"{msg}\.value(\(\))?", s):
                    recs.add(tgt)
                    continue
                if re.fullmatch(rf"{msg}\.value(\(\))?(\.decode\(.*\))?", s):
                    raw_value = True
                    ops.append(("with", tgt, "F.col('value').cast('string')", filters))
                    derived.append(tgt)
                    continue
                if isinstance(v, ast.Dict):
                    try:
                        cols = [(_const(k), rx.t(val)) for k, val in zip(v.keys, v.values) if isinstance(_const(k), str)]
                    except Unsupported:
                        todo_stmts.append(st)
                        continue
                    dicts[tgt] = cols
                    continue
                try:
                    code = rx.t(v)
                except Unsupported:
                    todo_stmts.append(st)
                    continue
                derived.append(tgt)
                rx.derived.add(tgt)
                ops.append(("with", tgt, code, filters))
                continue
            cu = is_counter_update(st)
            if cu:
                name, key, val, isadd = cu
                try:
                    kcode = rx.t(key)
                    if isinstance(_const(val), int):
                        state = ("count", name, kcode, None, filters)
                    else:
                        state = ("sum", name, kcode, rx.t(val, True), filters)
                except Unsupported:
                    todo_stmts.append(st)
                continue
            if isinstance(st, ast.Expr) and isinstance(st.value, ast.Call):
                c = st.value
                f = _src(c.func)
                if isinstance(c.func, ast.Attribute) and c.func.attr == "send" and isinstance(c.func.value, ast.Name) and c.func.value.id in producers:
                    topic = _const(c.args[0]) if c.args else _const(_kw(c, "topic"))
                    val = c.args[1] if len(c.args) > 1 else _kw(c, "value")
                    cols = None
                    while isinstance(val, ast.Call) and isinstance(val.func, ast.Attribute) and val.func.attr == "encode":
                        val = val.func.value
                    if isinstance(val, ast.Call) and "json.dumps" in _src(val.func) and val.args:
                        a = val.args[0]
                        if isinstance(a, ast.Name) and a.id in dicts:
                            cols = dicts[a.id]
                        elif isinstance(a, ast.Dict):
                            try:
                                cols = [(_const(k), rx.t(v)) for k, v in zip(a.keys, a.values)]
                            except Unsupported:
                                cols = None
                    sink = ("kafka", topic or "FILL_ME", _servers(producers[c.func.value.id]), cols, filters)
                    continue
                if f == "print":
                    sink = ("console", None, None, None, filters)
                    continue
                if f.endswith("flush") or f.endswith("commit") or f.endswith("close"):
                    continue
            todo_stmts.append(st)
    handle(body, [])
    # --- generate
    L = [HEAD]
    L.append(f"raw = (spark.readStream.format('kafka')\n       .option('kafka.bootstrap.servers', {servers!r})\n       .option('subscribe', {sub!r})\n       .option('startingOffsets', {off!r})\n       .load())")
    if recs:
        L.append(_schema_code(rx.keys, rx.numeric))
        L.append("events = raw.select(F.from_json(F.col('value').cast('string'), schema).alias('d'), F.col('key').cast('string').alias('key'), 'topic', 'timestamp').select('d.*', 'key', 'topic', 'timestamp')")
    else:
        L.append("events = raw.select(F.col('key').cast('string').alias('key'), F.col('value').cast('string').alias('value'), 'topic', 'timestamp')")
    cur = "events"
    def apply(df, filt):
        c = df
        for f in filt:
            pass
        return c
    allfilters = []
    for kind, name, code, filt in ops:
        for f in filt:
            if f not in allfilters:
                allfilters.append(f)
        if filt:
            w = f"F.when({' & '.join(_safe(rx, f) for f in filt)}, {code}).otherwise(F.lit(None))" if False else code
        L.append(f"events = events.withColumn({name!r}, {code})")
    if state or sink:
        flt = (state or sink)[4]
        if flt:
            L.append("events = events.filter(" + " & ".join(flt) + ")")
    elif allfilters:
        L.append("events = events.filter(" + " & ".join(allfilters) + ")")
    out_df, mode = "events", "append"
    if state:
        kind, name, kcode, vcode, _ = state
        agg = "F.count(F.lit(1)).alias('count')" if kind == "count" else f"F.sum({vcode}).alias('total')"
        L.append(f"agg = events.groupBy({kcode}.alias('key')).agg({agg})")
        out_df, mode = "agg", "update"
        report.note(f"running dict `{name}` became a streaming aggregation (outputMode update): state lives in the checkpoint, not in the process")
    writer = None
    if sink and sink[0] == "kafka" and not state:
        _, topic, bs, cols, _ = sink
        if cols:
            L.append(f"result = events.select(F.to_json(F.struct({', '.join(c + '.alias(' + repr(k) + ')' for k, c in cols)})).alias('value'))")
        else:
            L.append("result = events.select(F.col('value').cast('string').alias('value'))" if raw_value or not recs else "result = events.select(F.to_json(F.struct(*[c for c in events.columns if c not in ('key', 'topic', 'timestamp')])).alias('value'))")
        out_df = "result"
        writer = f"(result.writeStream.format('kafka')\n         .option('kafka.bootstrap.servers', {bs!r})\n         .option('topic', {topic!r})"
    elif sink and sink[0] == "kafka":
        report.todo("kafka", "producer.send combined with a running aggregate: write the aggregate to Kafka with to_json(struct(...)) yourself")
    if writer is None:
        writer = f"({out_df}.writeStream.format('console')"
        if not sink and not state:
            report.warn("kafka", "no sink recognised in the loop body: writing to the console")
    L.append(f"query = {writer}\n         .option('checkpointLocation', '/tmp/checkpoints/{re.sub(r'[^a-z0-9]+', '_', sub.lower())}')\n         .outputMode({mode!r})\n         .start())")
    if todo_stmts:
        txt = "\n".join("#   " + l for st in todo_stmts for l in _src(st).splitlines())
        for st in todo_stmts:
            report.todo(f"line {st.lineno}", f"per-record logic has no column equivalent: `{_src(st).splitlines()[0][:70]}`")
        L.insert(-1, "# TODO(sparqify): statements below need a foreachBatch function (or a UDF):\n" + txt +
                 "\n# def process_batch(batch_df, batch_id):\n#     for row in batch_df.collect():\n#         ...\n# and use .writeStream.foreachBatch(process_batch) instead of the sink below\n"
                 "raise NotImplementedError('TODO(sparqify): per-record logic above is not converted: finish it before running this job')")
    L.append("query.awaitTermination()")
    report.note("kafka: needs the spark-sql-kafka connector package (org.apache.spark:spark-sql-kafka-0-10_2.12)")
    report.note("per-record Python (dict state, side effects) is replaced by column logic: ordering across partitions and exactly-once semantics differ")
    return "\n".join(L[1:]) if False else "\n".join(L)


def _safe(rx, f):
    try:
        return rx.t(f)
    except Unsupported:
        return "F.lit(True)  # TODO(sparqify): condition not translated: " + _src(f)


# ---------------------------------------------------------------- 2. directory polling
def poll_loops(tree, source, report):
    out = []
    for loop in [n for n in ast.walk(tree) if isinstance(n, ast.While)]:
        s = _src(loop)
        if not (re.search(r"os\.listdir|glob\.glob|\.glob\(|os\.scandir", s) and "sleep" in s):
            continue
        m = re.search(r"(os\.listdir|glob\.glob|\.glob|os\.scandir)\(([^)]*)\)", s)
        d = None
        for n in ast.walk(loop):
            if isinstance(n, ast.Call) and _src(n.func) in ("os.listdir", "glob.glob", "os.scandir") and n.args:
                d = n.args[0]
        if d is None or not isinstance(_const(d), str):
            for n in ast.walk(tree):                                # variable holding the folder
                if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name) and d is not None and isinstance(d, ast.Name) and n.targets[0].id == d.id and isinstance(_const(n.value), str):
                    d = n.value
        path = _const(d) if d is not None and isinstance(_const(d), str) else "FILL_ME"
        if path.endswith(("*", ".csv", ".json", ".parquet")) or "*" in path:
            path = path.rsplit("/", 1)[0] if "/" in path else "."
        fmt, rd, readvar = None, None, None
        for n in ast.walk(loop):
            if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Attribute) and n.value.func.attr in ("read_csv", "read_json", "read_parquet"):
                fmt = {"read_csv": "csv", "read_json": "json", "read_parquet": "parquet"}[n.value.func.attr]
                rd, readvar = n, n.targets[0].id
        if not rd:
            report.todo(f"line {loop.lineno}", "polling loop does not read files with pandas read_csv/read_json/read_parquet: not converted")
            continue
        sinkfmt = sinkpath = None
        keep = []
        removed = []
        def collect(stmts):
            nonlocal sinkfmt, sinkpath
            for st in stmts:
                if isinstance(st, (ast.Continue, ast.Pass)):
                    continue
                if isinstance(st, ast.Expr) and isinstance(st.value, ast.Call) and isinstance(st.value.func, ast.Attribute) \
                        and st.value.func.attr in ("to_csv", "to_parquet", "to_json") and st.value.args:
                    sinkfmt = {"to_csv": "csv", "to_parquet": "parquet", "to_json": "json"}[st.value.func.attr]
                    p = st.value.args[0]
                    cands = sorted((n for n in ast.walk(p) if isinstance(n, ast.Constant) and isinstance(n.value, str)), key=lambda n: (n.lineno, n.col_offset))
                    sinkpath = cands[0].value if cands else "FILL_ME"
                    if sinkpath.endswith("/"):
                        sinkpath = sinkpath.rstrip("/")
                    elif isinstance(p, (ast.BinOp, ast.JoinedStr)) or "." in sinkpath.rsplit("/", 1)[-1]:
                        sinkpath = sinkpath.rsplit("/", 1)[0] if "/" in sinkpath.strip("/") else sinkpath
                    keep.append(("sink", st.value.func.value))
                    continue
                if isinstance(st, ast.For):
                    # the for-over-files loop: descend; the first statement(s) are path handling
                    collect(st.body)
                    continue
                if isinstance(st, ast.If) and re.search(r"seen|processed|done|endswith|startswith", _src(st.test)):
                    if re.search(r"continue|not in|endswith|startswith", _src(st.test) + _src(ast.Module(st.body, []))):
                        collect(st.body if not st.orelse else st.body)
                        removed.append(_src(st.test))
                        continue
                if st is rd:
                    keep.append(("read", st))
                    continue
                if isinstance(st, ast.Expr) and isinstance(st.value, ast.Call) and _src(st.value.func) in ("time.sleep", "print", "shutil.move", "os.rename", "os.remove", "logging.info"):
                    removed.append(_src(st).split("(")[0])
                    continue
                if isinstance(st, ast.Assign) and isinstance(st.value, ast.Call) and _src(st.value.func) in ("os.path.join",):
                    continue
                if isinstance(st, ast.Expr) and isinstance(st.value, ast.Call) and isinstance(st.value.func, ast.Attribute) and st.value.func.attr in ("add", "append") and isinstance(st.value.func.value, ast.Name) and re.search("seen|done|processed", st.value.func.value.id):
                    continue
                if isinstance(st, ast.While):
                    collect(st.body)
                    continue
                keep.append(("stmt", st))
        collect(loop.body)
        body_stmts = [x for k, x in keep if k == "stmt"]
        pdsrc = "import pandas as pd\n" + f"{readvar} = pd.read_{fmt}('__STREAM_SRC__')\n" + "\n".join(_src(s_) for s_ in body_stmts) + "\n"
        from .pandas_conv import convert_pandas
        sub = Report()
        conv = convert_pandas(pdsrc, sub)
        for _, msg_ in sub.todos:
            report.todo(f"line {loop.lineno}", msg_)
        for _, msg_ in sub.warnings:
            report.warn(f"line {loop.lineno}", msg_)
        lines = [l for l in conv.splitlines() if not l.startswith(("from pyspark", "spark = SparkSession")) and l.strip()]
        lines = [l for l in lines if "__STREAM_SRC__" not in l]
        opt = ".option('header', 'true')" if fmt == "csv" else ""
        res_var = next((v for k, v in keep if k == "sink" and isinstance(v, ast.Name)), None)
        res = res_var.id if res_var is not None else readvar
        code = [HEAD, "spark.conf.set('spark.sql.streaming.schemaInference', 'true')  # file streams need a schema: set one explicitly in production",
                f"{readvar} = spark.readStream.format({fmt!r}){opt}.option('maxFilesPerTrigger', 1).load({path!r})", *lines]
        if sinkfmt:
            code.append(f"query = (({res}).writeStream.format({sinkfmt!r})" + (".option('header', 'true')" if sinkfmt == "csv" else "") +
                        f"\n         .option('path', {sinkpath!r})\n         .option('checkpointLocation', '/tmp/checkpoints/{re.sub(r'[^a-z0-9]+', '_', (path or 'dir').lower())}')\n         .outputMode('append')\n         .start())")
        else:
            code.append(f"query = ({res}).writeStream.format('console').outputMode('append').start()")
            report.warn(f"line {loop.lineno}", "no file sink found in the loop: writing to the console")
        code.append("query.awaitTermination()")
        if removed:
            report.note("polling bookkeeping dropped (sleep, seen-file sets, print, moves): Spark tracks processed files in the checkpoint: " + ", ".join(dict.fromkeys(removed))[:120])
        report.note("streaming restrictions: sort/limit/distinct and non-windowed aggregates in append mode are not allowed; check outputMode")
        out.append("\n".join(code))
    return out


# ---------------------------------------------------------------- 3. DStream
def dstream(tree, report):
    srcs = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
            s = _src(n.value)
            if "socketTextStream" in s or "textFileStream" in s or "createDirectStream" in s or "createStream" in s:
                srcs[n.targets[0].id] = n.value
    if not srcs and not any("StreamingContext" in _src(n) for n in ast.walk(tree) if isinstance(n, ast.Assign)):
        return []
    batch = 1
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and _src(n.func).endswith("StreamingContext") and len(n.args) > 1 and isinstance(_const(n.args[1]), (int, float)):
            batch = _const(n.args[1])
    L = [HEAD]
    lineage = {}                                    # dstream var -> (df var, kind, extra)
    def source(var, call):
        s = _src(call)
        if "socketTextStream" in s:
            a = call.args
            host, port = (_const(a[0]), _const(a[1])) if len(a) >= 2 else ("localhost", 9999)
            L.append(f"{var} = (spark.readStream.format('socket').option('host', {host!r}).option('port', {port!r}).load())")
            report.note("socket source is for testing only (not fault tolerant)")
            return ("lines", "value")
        if "textFileStream" in s:
            d = _const(call.args[0]) if call.args else "FILL_ME"
            L.append(f"{var} = spark.readStream.format('text').load({d!r})")
            return ("lines", "value")
        servers, topics = "localhost:9092", "FILL_ME"
        for a in call.args:
            if isinstance(a, ast.List):
                topics = ",".join(_const(e) for e in a.elts if isinstance(_const(e), str)) or topics
            if isinstance(a, ast.Dict):
                for k, v in zip(a.keys, a.values):
                    if _const(k) in ("metadata.broker.list", "bootstrap.servers") and isinstance(_const(v), str):
                        servers = _const(v)
        L.append(f"{var}_raw = (spark.readStream.format('kafka')\n       .option('kafka.bootstrap.servers', {servers!r})\n       .option('subscribe', {topics!r}).load())")
        L.append(f"{var} = {var}_raw.select(F.col('value').cast('string').alias('value'))")
        report.note("kafka: needs the spark-sql-kafka connector; DStream (key, value) tuples become the string column `value`")
        return ("lines", "value")
    for var, call in srcs.items():
        lineage[var] = source(var, call) + (var,)
    # transformations: chains assigned to names
    nstreams = 0
    for n in tree.body if isinstance(tree, ast.Module) else []:
        pass
    steps = []
    def chain_of(node):
        calls = []
        while isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            calls.append((node.func.attr, node.args))
            node = node.func.value
        return node, list(reversed(calls))
    state = {}                                       # var -> dict(df, kind, cols)
    for var, (kind, col, base) in lineage.items():
        state[var] = {"df": var, "kind": "lines", "key": None, "val": col}
    outputs = []
    for st in ast.walk(tree):
        pass
    for st in [n for n in ast.walk(tree) if isinstance(n, (ast.Assign, ast.Expr))]:
        if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name) and st.targets[0].id in srcs:
            continue
        v = st.value if isinstance(st, (ast.Assign, ast.Expr)) else None
        base, calls = chain_of(v)
        if not (isinstance(base, ast.Name) and base.id in state) or not calls:
            continue
        cur = dict(state[base.id])
        tvar = st.targets[0].id if isinstance(st, ast.Assign) else None
        n_ = 0
        for name, args in calls:
            n_ += 1
            lam = args[0] if args and isinstance(args[0], ast.Lambda) else None
            try:
                if name == "flatMap" and lam:
                    b = lam.body
                    if isinstance(b, ast.Call) and isinstance(b.func, ast.Attribute) and b.func.attr == "split":
                        sep = _const(b.args[0]) if b.args else " "
                        cur = {**cur, "kind": "words", "words": f"F.explode(F.split(F.col({cur['val']!r}), {_rx(sep or ' ')!r})).alias('word')"}
                        continue
                    raise Unsupported("flatMap")
                if name == "map" and lam and isinstance(lam.body, ast.Tuple) and len(lam.body.elts) == 2:
                    p = lam.args.args[0].arg
                    rx = RExpr([], [], None)
                    k, vv = lam.body.elts
                    col0 = "word" if cur.get("words") else cur["val"]
                    ctx = RExpr([], [p], None)
                    def sub(e):
                        s_ = RExpr([], [p], None).t(e)
                        return s_.replace(f"F.col({p!r})", f"F.col({col0!r})")
                    cur = {**cur, "kind": "pairs", "key": sub(k), "pval": sub(vv)}
                    continue
                if name == "map" and lam:
                    p = lam.args.args[0].arg
                    col0 = "word" if cur.get("words") else cur["val"]
                    e = RExpr([], [p], None).t(lam.body).replace(f"F.col({p!r})", f"F.col({col0!r})")
                    cur = {**cur, "kind": "mapped", "val": e}
                    continue
                if name == "filter" and lam:
                    p = lam.args.args[0].arg
                    col0 = "word" if cur.get("words") else cur["val"]
                    e = RExpr([], [p], None).t(lam.body).replace(f"F.col({p!r})", f"F.col({col0!r})")
                    cur.setdefault("filters", []).append(e)
                    continue
                if name == "reduceByKey" and cur["kind"] == "pairs" and lam and re.fullmatch(r"\s*\w+\s*\+\s*\w+\s*", _src(lam.body)):
                    cur = {**cur, "kind": "agg"}
                    continue
                if name == "countByValue":
                    cur = {**cur, "kind": "countval"}
                    continue
                if name == "pprint":
                    outputs.append((cur, "console", None))
                    continue
                if name in ("saveAsTextFiles",):
                    outputs.append((cur, "text", _const(args[0]) if args else "FILL_ME"))
                    continue
                if name == "foreachRDD":
                    outputs.append((cur, "foreach", _src(args[0]) if args else ""))
                    continue
                if name in ("window", "reduceByKeyAndWindow", "countByWindow", "updateStateByKey", "transform", "join", "union", "cogroup", "mapPartitions", "reduceByWindow"):
                    raise Unsupported(name)
                raise Unsupported(name)
            except Unsupported as e:
                report.todo(f"line {st.lineno}", f"DStream operation `{e}` has no automatic conversion (windowing needs an event-time column; stateful ops need mapGroupsWithState/applyInPandasWithState)")
                cur = None
                break
        if cur is None:
            continue
        if tvar:
            state[tvar] = cur
        # terminal pprint etc already queued
    # emit pipelines for each queued output
    qn = 0
    for cur, kind, extra in outputs:
        qn += 1
        flt = cur.get("filters", [])
        if cur.get("words"):
            L.append(f"words{qn} = {cur['df']}.select({cur['words']})")
            base, col = f"words{qn}", "word"
        else:
            base, col = cur["df"], cur["val"]
        if cur["kind"] == "agg":
            L.append(f"pairs{qn} = {base}.select({cur['key']}.alias('key'), {cur['pval']}.alias('v'))")
            for f in flt:
                L.append(f"pairs{qn} = pairs{qn}.filter({f})")
            L.append(f"result{qn} = pairs{qn}.groupBy('key').agg(F.sum('v').alias('total'))")
            mode = "complete"
        elif cur["kind"] == "countval":
            for f in flt:
                L.append(f"{base} = {base}.filter({f})")
            L.append(f"result{qn} = {base}.groupBy({col!r}).count()")
            mode = "complete"
        else:
            if cur["kind"] == "mapped":
                L.append(f"result{qn} = {base}.select({cur['val']}.alias('value'))")
            else:
                L.append(f"result{qn} = {base}")
            for f in flt:
                L.append(f"result{qn} = result{qn}.filter({f})")
            mode = "append"
        trig = f".trigger(processingTime='{int(batch)} seconds')"
        if kind == "console":
            L.append(f"q{qn} = result{qn}.writeStream.format('console').outputMode({mode!r}){trig}.start()")
        elif kind == "text":
            L.append(f"q{qn} = (result{qn}.writeStream.format('csv').option('path', {extra!r}).option('checkpointLocation', '/tmp/checkpoints/q{qn}').outputMode({mode!r}){trig}.start())")
            report.note("saveAsTextFiles wrote one directory per batch; the file sink appends part files to one directory (csv)")
        else:
            L.append(f"# TODO(sparqify): foreachRDD({cm(extra)}) -> write a foreachBatch function with the same logic (batch_df instead of rdd)")
            L.append(f"q{qn} = result{qn}.writeStream.foreachBatch(lambda batch_df, batch_id: None).outputMode({mode!r}){trig}.start()  # replace the lambda")
            report.todo("foreachRDD", "per-batch logic must be re-written as a foreachBatch function")
        if mode == "complete":
            report.note("running totals use outputMode complete (whole result table every trigger): fine for small key sets")
    if not outputs:
        report.todo("dstream", "no output operation (pprint/saveAsTextFiles/foreachRDD) found to convert")
        return []
    L.append("spark.streams.awaitAnyTermination()")
    report.note("DStream API is deprecated: the result uses Structured Streaming (micro-batch, trigger = old batch interval)")
    return ["\n".join(L)]


def convert_stream(source: str, report: Report) -> str:
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        report.todo(f"line {e.lineno}", f"syntax error: {e.msg}")
        return ""
    blocks = dstream(tree, report) + kafka_loops(tree, report) + poll_loops(tree, source, report)
    if not blocks:
        report.todo("stream", "no recognised streaming pattern (Kafka consumer loop, directory polling loop, or DStream pipeline)")
        return ""
    code = ("\n\n# " + "-" * 60 + "\n").join(blocks)
    try:
        ast.parse(code)
    except SyntaxError as e:
        report.todo("stream", f"generated code does not parse ({e.msg}, line {e.lineno})")
    return code + "\n"
