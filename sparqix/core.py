"""Shared helpers: reporting, language detection, dispatcher."""
from __future__ import annotations

import re
from dataclasses import dataclass, field


class Unsupported(Exception):
    """Raised by a translator when a construct has no safe Spark equivalent."""


class UnsafeOutput(Exception):
    """The generated code contains constructs a converter must never emit (imports, exec, file/network/OS access)."""


class ConvertError(Exception):
    """The converter hit an input it could not handle (reported to the user without a stack trace)."""


class InputRejected(ValueError):
    """The input was refused before parsing (for example XML with a DTD/entities)."""


# --------------------------------------------------------------------------- security helpers
def cm(x) -> str:
    """Make untrusted text safe to place after `#` in generated code: one line, printable characters only.
    (str.split() splits on every Unicode line/paragraph separator, so \r, \x0b, \x0c, \x85, U+2028/2029 cannot start a new line.)"""
    return " ".join("".join(c if c.isprintable() else " " for c in str(x)).split())


def dq(x) -> str:
    """Make untrusted text safe inside a generated triple-quoted docstring."""
    return cm(x).replace("\\", "/").replace('"', "'")


def tq(x) -> str:
    """Python triple-quoted string literal for arbitrary text (backslashes and quote runs escaped)."""
    return '"""' + str(x).replace("\\", "\\\\").replace('"', '\\"') + '"""'


NATIVE_TARGETS = ("gluten", "comet", "fabric", "photon")
MAX_SOURCE_CHARS = 20_000_000
MAX_XML_BYTES = 50_000_000
_DTD = re.compile(r"<!\s*(DOCTYPE|ENTITY)", re.I)


def safe_fromstring(text):
    """xml.etree.ElementTree.fromstring that refuses DTDs and entity declarations (billion-laughs / external-entity tricks).
    None of the supported formats needs a DTD."""
    import xml.etree.ElementTree as ET
    raw = text if isinstance(text, str) else text.decode("utf-8", "replace")
    if len(raw) > MAX_XML_BYTES:
        raise InputRejected(f"XML input larger than {MAX_XML_BYTES // 1_000_000} MB")
    if _DTD.search(raw):
        raise InputRejected("XML with a DOCTYPE / ENTITY declaration is refused (not needed by any supported format, and unsafe)")
    return ET.fromstring(text)


# Generated code for these input languages is built from foreign formats: it may only use these imports and must not touch the OS.
FOREIGN = {"sql", "r", "sas", "excel", "powercenter", "idmc", "talend", "alteryx", "pentaho", "pig", "ssis", "hive", "bteq", "datastage",
           "proc", "stream", "dbt", "tdload"}
_OK_IMPORTS = ("pyspark", "re", "sys", "datetime", "typing", "json", "math", "functools", "collections", "itertools", "time", "decimal")
_BAD_NAMES = {"__import__", "eval", "exec", "compile", "open", "input", "os", "subprocess", "socket", "ctypes", "builtins", "globals",
              "locals", "vars", "setattr", "delattr", "breakpoint", "shutil", "pickle", "marshal"}
_BAD_ATTRS = {"system", "popen", "__class__", "__globals__", "__subclasses__", "__bases__", "__builtins__", "__dict__", "__code__"}


def unsafe_constructs(tree) -> list:
    """Every forbidden construct in a parsed module as (name, line): imports outside the allow-list, exec/eval/open/OS/network names, dunder attributes."""
    import ast
    out = []
    for n in ast.walk(tree):
        what = None
        if isinstance(n, ast.Import):
            what = next((a.name for a in n.names if not a.name.startswith(_OK_IMPORTS)), None)
        elif isinstance(n, ast.ImportFrom):
            what = n.module if not (n.module or "").startswith(_OK_IMPORTS) else None
        elif isinstance(n, ast.Name) and n.id in _BAD_NAMES:
            what = n.id
        elif isinstance(n, ast.Attribute) and n.attr in _BAD_ATTRS:
            what = n.attr
        if what:
            out.append((what, getattr(n, "lineno", 0)))
    return out


def unsafe_construct(tree) -> str | None:
    """First forbidden construct in a parsed module (None when clean)."""
    found = unsafe_constructs(tree)
    return f"{found[0][0]} (line {found[0][1]})" if found else None


def guard_output(code: str, lang: str, sql_out: str, report) -> None:
    """Defence in depth: converters quote untrusted text, and this check refuses output that still contains something dangerous.
    Raises UnsafeOutput; a plain syntax error only adds a TODO (code that does not parse cannot run)."""
    if lang not in FOREIGN or (lang == "sql" and sql_out == "sql") or not code.strip():
        return
    import ast
    try:
        tree = ast.parse(code)
    except (SyntaxError, ValueError) as e:
        report.todo("output", f"generated code does not parse ({e.__class__.__name__}: {cm(e)}): the input probably contains characters a converter does not quote correctly; review before use")
        return
    bad = unsafe_construct(tree)
    if bad:
        raise UnsafeOutput(f"refusing to emit generated code that uses `{bad}`: the input likely contains an injection attempt")


@dataclass
class Report:
    todos: list = field(default_factory=list)      # (lineno, message) - needs a human
    warnings: list = field(default_factory=list)   # (lineno, message) - converted, but check
    notes: list = field(default_factory=list)      # informational

    def todo(self, line, msg):
        self.todos.append((line, msg))

    def warn(self, line, msg):
        if (line, msg) not in self.warnings:
            self.warnings.append((line, msg))

    def note(self, msg):
        if msg not in self.notes:
            self.notes.append(msg)

    def summary(self) -> str:
        out = []
        for title, items in (("TODO (manual work needed)", self.todos),
                             ("WARNING (converted, please verify)", self.warnings)):
            for line, msg in items:
                loc = f"line {line}" if isinstance(line, int) else str(line)
                out.append(f"  {title.split()[0]:8} {loc}: {msg}")
        for n in self.notes:
            out.append(f"  NOTE     {n}")
        return "\n".join(out)


_SQL_START = re.compile(r"^(?:--[^\n]*\n|/\*.*?\*/|\s)*(select|with|insert|update|delete|create|merge|drop|alter)\b",
                        re.I | re.S)


def detect_language(source: str, filename: str = "") -> str:
    low = filename.lower()
    if re.search(r"\{\{\s*(ref|source|config)\(", source):
        return "dbt"
    if source.lstrip().startswith(("# Databricks notebook source", "-- Databricks notebook source")):
        return "databricks"
    if "talendfile:ProcessType" in source[:3000] or re.search(r'<node\b[^>]*componentName="t[A-Z]', source[:30000]):
        return "talend"
    if low.endswith(".ktr") or re.match(r"\s*(<\?xml[^>]*\?>\s*)?<transformation>", source[:400]):
        return "pentaho"
    if low.endswith(".dsx") or re.match(r"\s*BEGIN HEADER\b", source[:200]):
        return "datastage"
    if low.endswith((".hql", ".impala")) or re.search(r"(?im)^[ \t]*(set\s+hive\.|msck\s+repair\s+table|compute\s+stats\s|invalidate\s+metadata)|\$\{hiveconf:|insert\s+overwrite\s+(local\s+)?(table|directory)\b", source):
        return "hive"
    if re.search(r"(?im)^[ \t]*(DEFINE[ \t]+JOB\b|\.BEGIN[ \t]+(IMPORT[ \t]+)?MLOAD\b|BEGIN[ \t]+LOADING\b)", source):
        return "tdload"
    if low.endswith(".bteq") or re.search(r"(?im)^[ \t]*\.(logon|logoff|export|import|if\s+errorcode|if\s+activitycount)\b", source):
        return "bteq"
    if low.endswith(".pig") or re.search(r"(?im)^[ \t]*\w+\s*=\s*LOAD\s+'[^']*'", source):
        return "pig"
    if low.endswith(".yxmd") or "<AlteryxDocument" in source[:3000]:
        return "alteryx"
    if low.endswith(".dtsx") or "<DTS:Executable" in source[:20000]:
        return "ssis"
    if "<POWERMART" in source[:4000].upper() or re.search(r"<REPOSITORY\b.*<FOLDER\b", source[:8000], re.S):
        return "powercenter"
    if low.endswith(".json") and '"transformations"' in source or (
            source.lstrip().startswith("{") and '"transformations"' in source and '"nbformat"' not in source):
        return "idmc"
    if re.search(r"KafkaConsumer|confluent_kafka|StreamingContext|socketTextStream|textFileStream", source) or (
            re.search(r"while\s+True", source) and "sleep" in source and re.search(r"os\.listdir|glob\.glob|\.glob\(", source)):
        return "stream"
    if re.search(r"(?im)^[ \t]*(CREATE|ALTER|REPLACE)\s+(OR\s+REPLACE\s+)?(EDITIONABLE\s+)?(PROC(EDURE)?|FUNCTION)\s", source):
        return "proc"
    if low.endswith((".sql", ".tdsql")) or re.search(r"(?im)^[ \t]*((REPLACE|CREATE)[ \t]+MACRO\b|EXEC(UTE)?[ \t]+\w+[ \t]*\(|COLLECT[ \t]+STAT)", source):
        return "sql"
    if low.endswith(".ipynb"):
        return "notebook"
    if low.endswith(".sas") or re.search(r"(?im)^[ \t]*(proc\s+(sql|sort|means|freq|print|import)\b|data\s+\w[\w.]*\s*;)", source):
        return "sas"
    if low.endswith(".scala") or re.search(r"(?m)^[ \t]*(object|case class)\s+\w+|\bval\s+\w+\s*=\s*(List|Seq|Array)\(", source):
        return "scala"
    if low.endswith(".java") or re.search(r"\.stream\(\)", source):
        return "java"
    if re.search(r"\{\{\s*(ref|source|config)\(", source):
        return "dbt"
    if low.endswith((".xlsx.txt", ".formulas")) or re.match(r"\s*(\w+\s*[:=]\s*)?=\s*[A-Za-z]+\(", source):
        return "excel"
    if low.endswith((".r", ".rmd")) or re.search(r"%>%|library\((dplyr|tidyverse)\)|<-\s*read[._]csv", source):
        return "r"
    if re.search(r"google\.cloud import bigquery|import pandas_gbq|read_gbq\(", source):
        return "pandas"
    if re.search(r"^[ \t]*(import polars|from polars)", source, re.M):
        return "polars"
    if re.search(r"^[ \t]*(import dask|from dask)", source, re.M):
        return "dask"
    if re.search(r"^[ \t]*(import pandas|from pandas)|\bpd\.\w+\(", source, re.M):
        return "pandas"
    try:
        import ast
        ast.parse(source)
        return "python"
    except SyntaxError:
        pass
    if _SQL_START.match(source):
        return "sql"
    return "python"


def strip_parens(code: str) -> str:
    """Remove one redundant pair of outer parentheses."""
    while code.startswith("(") and code.endswith(")"):
        depth = 0
        for i, ch in enumerate(code):
            depth += ch == "("
            depth -= ch == ")"
            if depth == 0 and i < len(code) - 1:
                return code
        code = code[1:-1]
    return code


def format_chain(code: str, width: int = 88) -> str:
    """Break a long `a.b(...).c(...)` chain into one call per line."""
    if len(code) <= width:
        return code
    cuts, depth, quote, i = [], 0, None, 0
    while i < len(code):
        ch = code[i]
        if quote:
            if ch == "\\":
                i += 1
            elif ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "." and depth == 0 and re.match(r"\.[A-Za-z_]\w*\(", code[i:]):
            cuts.append(i)
        i += 1
    if len(cuts) < 2:
        return code
    parts = [code[:cuts[0]]] + [code[a:b] for a, b in zip(cuts, cuts[1:] + [len(code)])]
    return "(\n    " + "\n    ".join(parts) + "\n)"


def convert(source: str, language: str = "auto", *, target: str = "spark", optimize: bool = False, broadcast=(), mode: str = "dataframe", delta: bool = False, catalog: str | None = None, schema: str = "default",
            ai: bool = False, ai_apply: bool = False, ai_model: str | None = None, ai_provider: str | None = None, ai_base_url: str | None = None, ai_max: int = 20, ai_dry_run: bool = False, ai_client=None, ai_prompts=None, gluten_offheap: str = "4g", gluten_conf=(), gluten_jar: str = "", gluten_backend: str = "", gluten_libpath: str = "", gluten_preset=(), **kw):
    """Convert `source` to Spark code (target='databricks' wraps it as a Databricks notebook)."""
    if not isinstance(source, str):
        raise InputRejected("source must be text")
    if len(source) > MAX_SOURCE_CHARS:
        raise InputRejected(f"input is larger than {MAX_SOURCE_CHARS // 1_000_000} MB; split it into smaller files")
    _g: dict = {}
    if target in NATIVE_TARGETS:
        from .gluten_conv import parse_extra, settings
        if isinstance(gluten_preset, str):
            gluten_preset = [gluten_preset]
        try:
            gluten_extra = parse_extra(gluten_conf)
            settings(gluten_offheap, gluten_extra, engine=target, backend=gluten_backend or "", libpath=gluten_libpath or "", presets=tuple(gluten_preset or ()))
            from .gluten_conv import _JAR
            if gluten_jar and not _JAR.fullmatch(gluten_jar):
                raise ValueError("gluten_jar must be a plain path (letters, digits, _ . / - only)")
        except ValueError as e:
            raise InputRejected(str(e))
        _g = dict(extra=gluten_extra, jar=gluten_jar, backend=gluten_backend or "", libpath=gluten_libpath or "", presets=tuple(gluten_preset or ()))
    if "\x00" in source:
        raise InputRejected("input contains NUL bytes: it is not a text file")
    if mode == "pandas-api":
        lang1 = language if language != "auto" else detect_language(source, kw.get("filename", ""))
        if lang1 == "pandas":
            from .pandas_api_conv import convert_pandas_api
            rep = Report()
            code = convert_pandas_api(source, rep)
            if optimize:
                rep.note("--optimize does not apply to pandas-API output")
            if target == "databricks":
                from .databricks_conv import to_databricks
                code = to_databricks(code)
            elif target in NATIVE_TARGETS:
                from .gluten_conv import to_native
                if target == "photon":
                    from .databricks_conv import to_databricks
                    code = to_databricks(code)
                code = to_native(code, rep, target, offheap=gluten_offheap, **_g)
            return code, rep
        language = lang1
        kw["_note"] = f"--mode pandas-api only applies to pandas code; {lang1} converted to PySpark DataFrames"
    note = kw.pop("_note", None)
    try:
        code, report = _convert(source, language, **kw)
    except (SyntaxError, Unsupported, UnsafeOutput, InputRejected, ConvertError):
        raise
    except RecursionError as e:
        raise ConvertError("the input is nested too deeply to convert") from e
    except (ValueError, KeyError, IndexError, AttributeError, TypeError, AssertionError, OverflowError) as e:
        raise ConvertError(f"could not convert this input ({type(e).__name__}: {cm(e)[:160]})") from e
    if note:
        report.note(note)
    if optimize:
        lang0 = language if language != "auto" else detect_language(source, kw.get("filename", ""))
        if lang0 in ("scala", "java", "notebook", "powercenter", "idmc", "talend", "alteryx", "pentaho", "pig", "hive", "bteq", "datastage", "tdload") or (lang0 == "sql" and kw.get("sql_out") == "sql"):
            report.note(f"--optimize does not apply to {lang0} output")
        else:
            from .optimize import optimize as _opt
            code = _opt(code, report, broadcast)
    lang_d = language if language != "auto" else detect_language(source, kw.get("filename", ""))
    if lang_d in ("scala", "java", "notebook", "powercenter_skip"):
        if delta or catalog:
            report.note(f"--delta/--catalog do not apply to {lang_d} output")
    elif lang_d == "sql" and kw.get("sql_out") == "sql":
        from .delta import process_sql_text
        code = process_sql_text(code, report, delta=delta, catalog=catalog, schema=schema, skip=set())
    elif lang_d in ("sql", "dbt") or delta or catalog:
        from .delta import process_python
        code = process_python(code, report, delta=delta, catalog=catalog, schema=schema)
    if catalog and "." not in catalog and schema == "default" and (delta or catalog):
        report.note("--schema not given: unqualified tables go to <catalog>.default")
    if ai or ai_dry_run:
        from .ai import make_client, suggest
        client = ai_client or (None if ai_dry_run else make_client(provider=ai_provider, model=ai_model, base_url=ai_base_url))
        code = suggest(code, report, client, lang=lang_d, apply=ai_apply, max_blocks=ai_max, dry_run=ai_dry_run, prompts_out=ai_prompts)
    if target in ("databricks", "photon") and not code.startswith(("# Databricks", "-- Databricks")):
        from .databricks_conv import to_databricks
        lang = language if language != "auto" else detect_language(source, kw.get("filename", ""))
        if lang == "notebook":
            report.warn("notebook", "target databricks: .ipynb output kept as ipynb (importable in Databricks)")
        elif lang in ("scala", "java"):
            report.warn("target", "Scala/Java output is not wrapped as a Databricks notebook")
        elif lang == "sql" and kw.get("sql_out") == "sql":
            code = to_databricks(code, sql_only=True)
        elif lang not in ("scala", "java"):
            code = to_databricks(code)
    if target in NATIVE_TARGETS:
        from .gluten_conv import to_native
        lang = language if language != "auto" else detect_language(source, kw.get("filename", ""))
        if lang in ("scala", "java"):
            report.warn("target", f"Scala/Java output: add the {target} settings to your spark-submit instead (see README)")
        elif lang == "notebook":
            report.warn("notebook", f"target {target}: .ipynb output is unchanged; set the engine settings when the cluster/session starts")
        else:
            code = to_native(code, report, target, sql_only=(lang == "sql" and kw.get("sql_out") == "sql"), offheap=gluten_offheap, **_g)
    guard_output(code, lang_d, kw.get("sql_out", "pyspark"), report)
    return code, report


def _convert(source: str, language: str = "auto", *, sql_dialect: str = "generic",
            sql_out: str = "pyspark", filename: str = "", model: str = "", excel_columns: dict | None = None):
    """Convert `source` to Spark code. Returns (code, Report)."""
    if language == "auto":
        language = detect_language(source, filename)
    report = Report()
    if language == "stream":
        from .stream_conv import convert_stream
        return convert_stream(source, report), report
    if language == "ssis":
        from .ssis_conv import convert_ssis
        return convert_ssis(source, report), report
        return "", report
    if language == "proc":
        from .proc_conv import convert_proc
        return convert_proc(source, report, sql_dialect), report
    if language == "databricks":
        from .databricks_conv import convert_databricks
        return convert_databricks(source, report, sql_dialect=sql_dialect), report
    if language == "pandas":
        from .pandas_conv import convert_pandas
        return convert_pandas(source, report), report
    if language == "sql":
        from .sql_conv import convert_sql
        return convert_sql(source, report, dialect=sql_dialect, out=sql_out), report
    if language == "polars":
        from .polars_conv import convert_polars
        return convert_polars(source, report), report
    if language == "dask":
        from .dask_conv import convert_dask
        return convert_dask(source, report), report
    if language == "r":
        from .r_conv import convert_r
        return convert_r(source, report), report
    if language == "notebook":
        from .notebook_conv import convert_notebook
        return convert_notebook(source, report, sql_dialect=sql_dialect), report
    if language in ("powercenter", "idmc"):
        from .infa_gen import generate_mappings
        if language == "powercenter":
            from .infa_pc import parse_powercenter
            mappings = parse_powercenter(source, report)
        else:
            from .infa_idmc import parse_idmc
            mappings = parse_idmc(source, report)
            report.note("IDMC reader is best-effort (export schema is not public in detail) - verify the generated graph")
        if not mappings:
            report.todo("informatica", "no mappings found in the input")
        return generate_mappings(mappings, report, sql_dialect), report
    if language == "talend":
        from .talend_conv import convert_talend
        return convert_talend(source, report), report
    if language == "datastage":
        from .datastage_conv import convert_datastage
        return convert_datastage(source, report), report
    if language == "tdload":
        from .tdload_conv import convert_tdload
        return convert_tdload(source, report), report
    if language in ("hive", "bteq"):
        from .script_conv import convert_bteq, convert_hive
        return (convert_hive if language == "hive" else convert_bteq)(source, report), report
    if language == "pig":
        from .pig_conv import convert_pig
        return convert_pig(source, report), report
    if language == "pentaho":
        from .pentaho_conv import convert_pentaho
        return convert_pentaho(source, report), report
    if language == "alteryx":
        from .alteryx_conv import convert_alteryx
        return convert_alteryx(source, report), report
    if language == "dbt":
        import os
        from .dbt_conv import convert_dbt
        name = model or (os.path.splitext(os.path.basename(filename))[0].replace(".dbt", "") if filename else "model") or "model"
        return convert_dbt(source, report, dialect=sql_dialect, model=re.sub(r"\W", "_", name)), report
    if language == "sas":
        from .sas_conv import convert_sas
        return convert_sas(source, report), report
    if language == "excel":
        from .excel_conv import convert_excel
        return convert_excel(source, report, excel_columns), report
    if language in ("scala", "java"):
        from .jvm_conv import convert_scala, convert_java
        return (convert_scala if language == "scala" else convert_java)(source, report), report
    if language == "python":
        from .python_conv import convert_python
        return convert_python(source, report), report
    raise ValueError(f"unknown language {language!r}")
