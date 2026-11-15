"""Native execution engines for Spark: Gluten + Velox (default), Gluten + ClickHouse, DataFusion Comet, Microsoft Fabric NEE, Databricks Photon.

All of them are *plugins or runtimes below the DataFrame/SQL API*: the converted code does not change. What a target does:
 * adds the settings the engine needs: to the generated SparkSession (Gluten, Comet), as a `%%configure` cell (Fabric) or as a
   cluster setting (Photon: a managed runtime, nothing to put in the code);
 * scans the generated code for constructs that run on the JVM/Python instead of the native engine (Python UDFs, RDD calls,
   pandas round trips, ...) and reports them as warnings, so you know where the speed-up will stop.
Which operators and functions are native depends on the engine and its version: the Spark UI shows the real fallbacks.
The setting names follow each project's documentation at the time of writing and differ between releases: every default can be
overridden (`--gluten-conf KEY=VALUE`). Not tested against real engines (no Spark here).
"""
from __future__ import annotations

import ast
import json
import re

ENGINES = ("gluten", "comet", "fabric", "photon")
LABEL = {"gluten": "Apache Gluten", "comet": "Apache DataFusion Comet", "fabric": "Microsoft Fabric native execution engine",
         "photon": "Databricks Photon"}

PLUGIN = "org.apache.gluten.GlutenPlugin"
SHUFFLE = "org.apache.spark.shuffle.sort.ColumnarShuffleManager"
COMET_PLUGIN = "org.apache.spark.CometPlugin"
COMET_SHUFFLE = "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
_SIZE = re.compile(r"\d{1,5}[mMgG]")
DEFAULT_OFFHEAP = "4g"
BACKENDS = {"velox": "velox", "clickhouse": "ch"}

# curated starting points (names from the Gluten Velox configuration reference and Spark itself): tune them for your workload
PRESETS = {
    "spill-safe": ("let Velox spill aggregations, joins and sorts under memory pressure; compress shuffle with lz4", {
        "spark.gluten.sql.columnar.backend.velox.spillStrategy": "auto",
        "spark.gluten.sql.columnar.backend.velox.aggregationSpillEnabled": "true",
        "spark.gluten.sql.columnar.backend.velox.joinSpillEnabled": "true",
        "spark.gluten.sql.columnar.backend.velox.orderBySpillEnabled": "true",
        "spark.gluten.sql.columnar.shuffle.codec": "lz4"}),
    "scan-heavy": ("bigger Velox in-memory cache for repeated Parquet/ORC scans", {
        "spark.gluten.sql.columnar.backend.velox.memCacheSize": "4g"}),
    "agg-heavy": ("give up partial aggregation sooner when it does not reduce the data", {
        "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct": "80",
        "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows": "100000"}),
    "adaptive": ("Spark adaptive query execution: partition coalescing and skew-join handling", {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true"}),
    "production": ("spill-safe + adaptive: a conservative default for shared clusters", None),
}
PRESETS["production"] = (PRESETS["production"][0], {**PRESETS["spill-safe"][1], **PRESETS["adaptive"][1]})

# attribute / function names whose work is not done by the native engine
_UDF = {"udf", "pandas_udf"}
_PANDAS = {"toPandas", "mapInPandas", "applyInPandas", "applyInArrow", "mapInArrow", "cogroup"}
_RDD = {"rdd", "parallelize", "textFile", "wholeTextFiles", "mapPartitions", "foreachPartition", "foreach", "flatMap", "reduceByKey",
        "groupByKey", "combineByKey", "aggregateByKey", "mapPartitionsWithIndex"}
_FORMATS_CHECK = {"json", "avro", "text", "jdbc", "xml", "binaryFile", "image"}

_KEY = re.compile(r"spark\.[A-Za-z0-9_.\-]{1,100}")
_VAL = re.compile(r"[A-Za-z0-9_./:@=,+\-]{1,300}")
_JAR = re.compile(r"[A-Za-z0-9_./\-]{1,300}")


def parse_extra(items) -> dict:
    """['spark.x=y', ...] -> dict, with strict character sets so nothing from the option can leave a string literal or a comment"""
    out = {}
    for it in items or ():
        k, sep, v = str(it).partition("=")
        if not sep or not _KEY.fullmatch(k) or not _VAL.fullmatch(v):
            raise ValueError(f"bad --gluten-conf {it!r}: use spark.some.key=value (letters, digits and _ . / : @ = , + - only)")
        out[k] = v
    return out


def preset_conf(names) -> dict:
    out = {}
    for n in names or ():
        if n not in PRESETS:
            raise ValueError(f"unknown preset {n!r}: choose from {', '.join(sorted(PRESETS))}")
        out.update(PRESETS[n][1])
    return out


def _check_common(offheap, jar, libpath):
    if not _SIZE.fullmatch(offheap or ""):
        raise ValueError("gluten_offheap must look like 4g or 2048m")
    for what, v in (("jar", jar), ("libpath", libpath)):
        if v and not _JAR.fullmatch(v):
            raise ValueError(f"gluten_{what} must be a plain path (letters, digits, _ . / - only)")


def settings(offheap: str = DEFAULT_OFFHEAP, extra: dict | None = None, *, engine: str = "gluten", backend: str = "",
             libpath: str = "", presets=()) -> list:
    """Settings for an engine, as an ordered list of (key, value). Layers: engine defaults < presets < `extra`; nothing is tied to one
    Spark/Java/engine release because every entry can be overridden."""
    _check_common(offheap, "", libpath)
    if backend and backend not in BACKENDS:
        raise ValueError(f"unknown backend {backend!r}: choose velox or clickhouse")
    preset_conf(presets)                                   # unknown preset names are rejected for every engine
    if engine == "gluten":
        conf = {"spark.plugins": PLUGIN, "spark.memory.offHeap.enabled": "true", "spark.memory.offHeap.size": offheap,
                "spark.shuffle.manager": SHUFFLE}
        if backend:
            if backend not in BACKENDS:
                raise ValueError(f"unknown backend {backend!r}: choose velox or clickhouse")
            conf["spark.gluten.sql.columnar.backend.lib"] = BACKENDS[backend]
            if backend == "clickhouse":
                lib = libpath or "libch.so"
                conf["spark.gluten.sql.columnar.libpath"] = lib
                conf["spark.gluten.sql.columnar.executor.libpath"] = lib
        pre = preset_conf(presets)
        if backend == "clickhouse" and any(k.startswith("spark.gluten.sql.columnar.backend.velox.") for k in pre):
            pre = {k: v for k, v in pre.items() if not k.startswith("spark.gluten.sql.columnar.backend.velox.")}
        conf.update(pre)
    elif engine == "comet":
        conf = {"spark.plugins": COMET_PLUGIN, "spark.shuffle.manager": COMET_SHUFFLE, "spark.comet.exec.shuffle.enabled": "true",
                "spark.memory.offHeap.enabled": "true", "spark.memory.offHeap.size": offheap}
        conf.update({k: v for k, v in preset_conf(presets).items() if not k.startswith("spark.gluten.")})
    elif engine == "fabric":
        conf = {"spark.native.enabled": "true", "spark.shuffle.manager": SHUFFLE}
    elif engine == "photon":
        conf = {}
    else:
        raise ValueError(f"unknown engine {engine!r}")
    for k, v in (extra or {}).items():
        if not _KEY.fullmatch(k) or not _VAL.fullmatch(v):
            raise ValueError(f"bad setting {k!r}")
        conf[k] = v
    return list(conf.items())


def _jar_var(engine: str) -> str:
    return "$COMET_JAR" if engine == "comet" else "$GLUTEN_JAR"


def _launch_block(offheap: str, comment: str, extra=None, jar: str = "", *, engine: str = "gluten", backend: str = "",
                  libpath: str = "", presets=()) -> str:
    c = comment
    jar = jar or _jar_var(engine)
    conf = settings(offheap, extra, engine=engine, backend=backend, libpath=libpath, presets=presets)
    if engine == "fabric":
        body = json.dumps({"conf": dict(conf)}, indent=2).splitlines()
        lines = [f"{c} {LABEL[engine]} (generated by sparqix, --target fabric)",
                 f"{c} Enable it for the whole environment (Environment > Acceleration > Enable native execution engine),",
                 f"{c} or put this in the FIRST notebook cell, before the Spark session starts (spark.shuffle.manager cannot change afterwards):",
                 f"{c}   %%configure"] + [f"{c}   {l}" for l in body]
        lines.append(f"{c} Check the 'Gluten SQL / DataFrame' tab in the Spark UI: it shows what ran natively and what fell back.")
        return "\n".join(lines) + "\n\n"
    if engine == "photon":
        spec = json.dumps({"runtime_engine": "PHOTON"})
        lines = [f"{c} {LABEL[engine]} (generated by sparqix, --target photon)",
                 f"{c} Photon is a managed engine: nothing to configure in code. Turn it on for the compute you run this on:",
                 f"{c}   UI: tick 'Use Photon Acceleration' (needs a Photon-capable runtime and instance type)",
                 f"{c}   Clusters / Jobs API: {spec}   (it is not on by default for compute created through the APIs)",
                 f"{c} The Spark UI / query profile shows which operators ran in Photon; the rest run in the standard Spark runtime."]
        return "\n".join(lines) + "\n\n"
    lines = [f"{c} {LABEL[engine]}" + (" + " + ("ClickHouse" if backend == "clickhouse" else "Velox") if engine == "gluten" else "")
             + f": native execution engine (generated by sparqix, --target {engine})",
             f"{c} Needs the {engine.capitalize()} jar that matches YOUR Spark, Scala and OS/Java build on BOTH classpaths, set when the application is launched",
             f"{c} (sparqix does not assume a version: point {_jar_var(engine)[1:]} at the right file):",
             f"{c}   spark-submit --jars {jar} \\",
             f"{c}     --conf spark.driver.extraClassPath={jar} --conf spark.executor.extraClassPath={jar} \\"]
    for k, v in conf:
        lines.append(f"{c}     --conf {k}={v} \\")
    lines.append(f"{c}     your_job.py")
    if engine == "gluten" and backend == "clickhouse":
        lines.append(f"{c} The ClickHouse backend also needs libch.so (a native library you build, see the Gluten ClickHouse guide) on every executor.")
    lines.append(f"{c} Check the Spark UI / physical plan: operators that did not run natively show as fallbacks.")
    return "\n".join(lines) + "\n\n"


def _add_session_conf(code: str, conf: list) -> tuple:
    """insert .config(...) calls right after `SparkSession.builder`; returns (code, changed)"""
    text = "".join(f'.config("{k}", "{v}")' for k, v in conf)
    new, n = re.subn(r"SparkSession\.builder(?=[.\s\\(])", lambda m: "SparkSession.builder" + text, code)
    return new, n > 0


def analyze_fallbacks(code: str, report, engine: str = "gluten") -> int:
    """warn about constructs that do not run in the native engine; returns the number of findings"""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return 0
    seen, n = {}, 0
    tag = engine
    where = {"gluten": "your Gluten version", "comet": "your Comet version", "fabric": "this Fabric runtime", "photon": "Photon"}[engine]

    def flag(kind, line, text):
        nonlocal n
        key = (kind, line)
        if key in seen:
            return
        seen[key] = 1
        n += 1
        report.warn(f"line {line}", f"{tag}: {text}")

    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        name = node.func.id if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) else \
            node.func.attr if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) else None
        if isinstance(node, ast.Attribute) and node.attr == "rdd":
            flag("rdd", line, ".rdd leaves the native engine (RDD code runs on the JVM, and converting to/from it costs a copy); rewrite with DataFrame operations")
        if name in _UDF:
            flag("udf", line, f"`{name}` is a Python UDF: it runs in Python workers, so this stage falls back from the native engine; prefer built-in functions or SQL expressions")
        elif name in _PANDAS:
            flag("pandas", line, f"`{name}` moves data through pandas/Arrow in Python: not native")
        elif name in _RDD and name != "rdd":
            flag("rdd", line, f"`{name}` is an RDD operation: not native; rewrite with DataFrame operations")
        elif name == "collect":
            flag("collect", line, ".collect() pulls rows to the driver (not an engine limit, but the final stage is row-based)")
        elif name == "expr" and node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            flag("expr", line, f"F.expr(...) is parsed by Spark; whether every function in it is native depends on {where} (check the Spark UI)")
        elif name in _FORMATS_CHECK and isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            flag("fmt", line, f"reading/writing `{name}` may not be native in {where} (Parquet/ORC/Delta are the best supported); verify in the Spark UI")
    return n


def to_native(code: str, report, engine: str = "gluten", *, sql_only: bool = False, offheap: str = DEFAULT_OFFHEAP, analyze: bool = True,
              extra: dict | None = None, jar: str = "", backend: str = "", libpath: str = "", presets=()) -> str:
    offheap = offheap or DEFAULT_OFFHEAP
    if engine not in ENGINES:
        raise ValueError(f"unknown engine {engine!r}")
    _check_common(offheap, jar, libpath)
    conf = settings(offheap, extra, engine=engine, backend=backend, libpath=libpath, presets=presets)   # validates everything
    if backend and engine != "gluten":
        report.warn("target", f"--gluten-backend only applies to the gluten target: ignored for {engine}")
    if presets and engine not in ("gluten", "comet"):
        report.warn("target", f"presets only apply to the gluten target (and its adaptive/spark.sql.* entries to comet): ignored for {engine}")
    report.note(f"target {engine}: {LABEL[engine]} runs below the DataFrame/SQL API; the code is unchanged apart from "
                f"{'the cluster setting' if engine == 'photon' else 'the settings'}. Operators it cannot run natively fall back to Spark automatically.")
    block = lambda cm_: _launch_block(offheap, cm_, extra, jar, engine=engine, backend=backend, libpath=libpath, presets=presets)
    if engine in ("fabric", "photon"):
        # no session settings can be applied from inside a running managed session: header only
        if code.startswith(("# Databricks notebook source", "-- Databricks notebook source")):
            first, _, rest = code.partition("\n")
            c = "--" if first.startswith("--") else "#"
            out = first + "\n" + block(c) + rest
        else:
            out = block("--" if sql_only else "#") + code
        report.note(f"target {engine}: no session settings in the code: the header comment says how to switch the engine on")
    elif sql_only:
        out = block("--") + code
        report.note(f"target {engine}: Spark SQL output has no session: the launch settings are in the header comment")
    else:
        out, changed = _add_session_conf(code, conf)
        if not changed:
            out = block("#") + code
            report.note(f"target {engine}: no SparkSession is created in this output: the launch settings are in the header comment")
        else:
            out = block("#").split("\n\n")[0] + "\n\n" + out
    if analyze:
        k = analyze_fallbacks(out, report, engine)            # on the final text, so the line numbers match the file
        report.note(f"target {engine}: {k} construct(s) may fall back from the native engine" if k else
                    f"target {engine}: no Python UDF / RDD / pandas round trip found in the generated code")
    return out


def to_gluten(code: str, report, **kw) -> str:
    return to_native(code, report, "gluten", **kw)


def defaults_conf(offheap: str = DEFAULT_OFFHEAP, extra: dict | None = None, jar: str = "/opt/gluten/gluten.jar", *,
                  engine: str = "gluten", backend: str = "", libpath: str = "", presets=()) -> str:
    """Settings as text: spark-defaults.conf lines (gluten, comet), a %%configure cell (fabric) or a cluster JSON fragment (photon)."""
    _check_common(offheap, jar, libpath)
    conf = settings(offheap, extra, engine=engine, backend=backend, libpath=libpath, presets=presets)
    if engine == "fabric":
        return json.dumps({"conf": dict(conf)}, indent=2) + "\n"
    if engine == "photon":
        return json.dumps({"runtime_engine": "PHOTON"}, indent=2) + "\n"
    conf = [("spark.driver.extraClassPath", jar), ("spark.executor.extraClassPath", jar)] + conf
    return f"# {LABEL[engine]} (generated by `sparqix gluten-conf`)\n" + "".join(f"{k} {v}\n" for k, v in conf)


def main(argv) -> int:
    import argparse
    import sys
    p = argparse.ArgumentParser(prog="sparqix gluten-conf",
                                description="print the settings for a native engine (version-agnostic: you choose the jar, the Spark and the Java)")
    p.add_argument("--engine", default="gluten", choices=ENGINES)
    p.add_argument("--backend", default="", choices=["", *BACKENDS], help="gluten backend (default: the build's own default, Velox)")
    p.add_argument("--libpath", default="", help="clickhouse backend: path of libch.so")
    p.add_argument("--preset", action="append", default=[], metavar="NAME", help="gluten/velox starting points, repeatable: " + ", ".join(PRESETS))
    p.add_argument("--list-presets", action="store_true")
    p.add_argument("--jar", default="/opt/gluten/gluten.jar", help="path of the bundle jar inside the image")
    p.add_argument("--offheap", default=DEFAULT_OFFHEAP)
    p.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE", help="extra or overriding setting, repeatable")
    a = p.parse_args(argv)
    if a.list_presets:
        for n, (d, c) in PRESETS.items():
            print(f"{n}: {d}")
            for k, v in c.items():
                print(f"    {k}={v}")
        return 0
    try:
        print(defaults_conf(a.offheap, parse_extra(a.conf), a.jar, engine=a.engine, backend=a.backend, libpath=a.libpath, presets=a.preset), end="")
    except ValueError as e:
        print(f"sparqix: {e}", file=sys.stderr)
        return 2
    return 0
