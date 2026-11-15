"""Orchestration conversion:  sparqix orchestrate INPUT [--to databricks|airflow] [--code-root PATH] [-o OUTDIR]

Reads an Airflow DAG (.py), a crontab, or an Informatica PowerCenter workflow (XML) into a neutral job graph
and writes it as a Databricks Jobs API definition (JSON, one file per job) or as an Airflow DAG that runs the
converted scripts on Databricks. The converted code itself comes from `sparqix` / `sparqix project`; this
tool only wires it together (schedule, dependencies, retries). Placeholders (FILL_ME) are left for cluster /
warehouse ids so that creating the job fails loudly instead of running with guessed settings."""
from __future__ import annotations

import ast
import json
import os
import re
import shlex
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

from .core import Report, safe_fromstring, cm


@dataclass
class Task:
    key: str
    kind: str = "unknown"          # python | sql | session | dbt | noop | unknown
    file: str = ""                 # script / sql file / mapping name
    args: list = field(default_factory=list)
    sql: str = ""
    retries: int = 0
    depends: list = field(default_factory=list)
    note: str = ""


@dataclass
class Job:
    name: str
    schedule: str | None = None    # 5-field cron (unix) or None
    tasks: list = field(default_factory=list)
    tz: str = "UTC"


# ------------------------------------------------------------------ helpers
PRESETS = {"@hourly": "0 * * * *", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@weekly": "0 0 * * 0",
           "@monthly": "0 0 1 * *", "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *"}
_DOW = {"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7}


def _dow_to_quartz(f: str) -> str:
    def one(tok):
        if tok.upper() in _DOW or not tok.isdigit():
            return tok
        return str(int(tok) % 7 + 1)
    def part(p):
        base, _, step = p.partition("/")
        if base in ("*", "?"):
            return p
        return "-".join(one(x) for x in base.split("-")) + (f"/{step}" if step else "")
    return ",".join(part(p) for p in f.split(","))


def cron_to_quartz(expr: str, report: Report, where: str) -> str | None:
    expr = PRESETS.get(expr.strip(), expr.strip())
    f = expr.split()
    if len(f) != 5:
        report.todo(where, f"cannot convert schedule {expr!r} to a Quartz cron")
        return None
    m, h, dom, mon, dow = f
    dow = _dow_to_quartz(dow)
    if dom == "*" and dow == "*":
        dow = "?"
    elif dom == "*":
        dom = "?"
    elif dow == "*":
        dow = "?"
    else:
        report.warn(where, "both day-of-month and day-of-week are set: cron ORs them, Quartz cannot; only day-of-month was kept")
        dow = "?"
    return f"0 {m} {h} {dom} {mon} {dow}"


def classify(cmd: str, task: Task, report: Report):
    """Fill kind/file/args from a shell command line."""
    def _jinja(m):
        inner = m.group(1).strip()
        if inner == "ds":
            return "{{job.start_time.iso_date}}"
        report.warn(task.key, f"Airflow template {{{{ {inner} }}}} has no automatic equivalent: use a Databricks dynamic value reference or a parameter")
        return "{{" + inner.replace(" ", "_") + "}}"
    cmd = re.sub(r"\{\{\s*(.*?)\s*\}\}", _jinja, cmd)
    segs = [x.strip() for x in re.split(r"&&|;", cmd) if x.strip() and not re.match(r"^(cd|export|set)\b", x.strip())]
    if len(segs) > 1:
        task.kind, task.note = "unknown", f"several chained commands: split into one task each ({cmd.strip()[:100]})"
        return
    cmd = segs[0] if segs else cmd
    try:
        parts = shlex.split(cmd.replace("\\\n", " "))
    except ValueError:
        parts = cmd.split()
    if parts:
        parts[0] = os.path.basename(parts[0])
    parts = [p for p in parts if not re.match(r"^[A-Za-z_]\w*=", p)]           # leading ENV=val
    while parts and parts[0] in ("cd", "nohup", "sudo", "time", "exec") and len(parts) > 2 and parts[0] != "cd":
        parts = parts[1:]
    if parts and parts[0] == "spark-submit":
        i = next((k for k, p in enumerate(parts[1:], 1) if p.endswith(".py") or p.endswith(".jar")), None)
        if i is not None:
            if parts[i].endswith(".jar"):
                task.kind, task.note = "unknown", "spark-submit of a JAR: use a spark_jar_task with the main class"
            else:
                task.kind, task.file, task.args = "python", parts[i], parts[i + 1:]
            return
    if parts and re.fullmatch(r"python[\d.]*", os.path.basename(parts[0])) and len(parts) > 1 and parts[1].endswith(".py"):
        task.kind, task.file, task.args = "python", parts[1], parts[2:]
        return
    if parts and parts[0] in ("spark-sql", "beeline", "psql") and "-f" in parts:
        task.kind, task.file = "sql", parts[parts.index("-f") + 1]
        return
    if parts and parts[0] == "dbt":
        task.kind, task.sql = "dbt", " ".join(parts)
        return
    task.kind, task.note = "unknown", f"shell command has no automatic mapping: {cmd.strip()[:120]}"


# ------------------------------------------------------------------ readers
def read_crontab(text: str, report: Report) -> list:
    jobs = []
    for n, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#") or re.match(r"^[A-Za-z_]\w*\s*=", line):
            continue
        if line.startswith("@"):
            sched, _, cmd = line.partition(" ")
        else:
            f = line.split(None, 5)
            if len(f) < 6:
                continue
            sched, cmd = " ".join(f[:5]), f[5]
        t = Task(key=f"cron_{n}")
        classify(cmd, t, report)
        if sched.startswith("@") and sched not in PRESETS:
            report.warn(f"line {n}", f"schedule {sched} has no Databricks equivalent (job left unscheduled)")
            sched = None
        jobs.append(Job(name=f"cron_line_{n}", schedule=sched, tasks=[t]))
    return jobs


_OPS_SQL = ("sql", "bigquery", "snowflake", "redshift", "postgres", "mysql", "mssql", "hive", "presto", "trino")


def _lit(n):
    try:
        return ast.literal_eval(n)
    except Exception:
        return None


def read_airflow(text: str, report: Report) -> list:
    tree = ast.parse(text)
    dag_kw, default_args, tasks, edges = {}, {}, {}, []
    varmap = {}                                     # python variable -> task_id(s)
    defaults_node = None
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and getattr(n.func, "id", getattr(n.func, "attr", "")) == "DAG":
            dag_kw = {k.arg: k.value for k in n.keywords if k.arg}
            if n.args and isinstance(_lit(n.args[0]), str):
                dag_kw.setdefault("dag_id", n.args[0])
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and getattr(n.targets[0], "id", "") == "default_args":
            defaults_node = n.value
    if "default_args" in dag_kw and isinstance(dag_kw["default_args"], ast.Dict):
        defaults_node = dag_kw["default_args"]
    if isinstance(defaults_node, ast.Dict):
        default_args = {_lit(k): _lit(v) for k, v in zip(defaults_node.keys, defaults_node.values) if k is not None and _lit(k) is not None}
    if any(isinstance(n, (ast.FunctionDef,)) and any(getattr(getattr(d, "func", d), "id", getattr(getattr(d, "func", d), "attr", "")) in ("task", "dag") for d in n.decorator_list) for n in ast.walk(tree)):
        report.todo("file", "TaskFlow API (@task / @dag) is not parsed: convert these tasks manually or use classic operators")

    def reg(call, var):
        cls = getattr(call.func, "id", getattr(call.func, "attr", ""))
        if not cls.endswith("Operator") and not cls.endswith("Sensor"):
            return
        kw = {k.arg: k.value for k in call.keywords if k.arg}
        tid = _lit(kw.get("task_id")) if "task_id" in kw else None
        if not isinstance(tid, str):
            report.todo(f"line {call.lineno}", f"{cls}: task_id is not a string literal")
            return
        t = Task(key=tid)
        t.retries = int(_lit(kw["retries"]) if "retries" in kw and isinstance(_lit(kw["retries"]), int) else default_args.get("retries", 0) or 0)
        low = cls.lower()
        if cls in ("EmptyOperator", "DummyOperator"):
            t.kind = "noop"
        elif cls == "BashOperator":
            cmd = _lit(kw.get("bash_command"))
            if isinstance(cmd, str):
                classify(cmd, t, report)
            else:
                t.note = "bash_command is not a literal string"
        elif cls == "SparkSubmitOperator":
            app = _lit(kw.get("application"))
            if isinstance(app, str) and app.endswith(".py"):
                t.kind, t.file = "python", app
                a = _lit(kw.get("application_args"))
                t.args = [str(x) for x in a] if isinstance(a, list) else []
            else:
                t.note = "SparkSubmitOperator without a literal .py application"
        elif "databricks" in low and "sql" not in low:
            t.note = "already a Databricks operator: kept manually"
        elif any(k in low for k in _OPS_SQL) or "sqlexecute" in low:
            q = _lit(kw.get("sql"))
            if q is None and "configuration" in kw:
                try:
                    q = _lit(kw["configuration"]).get("query", {}).get("query")
                except Exception:
                    q = None
            if isinstance(q, str):
                t.kind, t.sql = "sql", q
            elif isinstance(q, list) and all(isinstance(x, str) for x in q):
                t.kind, t.sql = "sql", ";\n".join(q)
            else:
                t.note = "SQL operator without literal SQL (templated or file reference)"
        elif cls in ("PythonOperator", "PythonVirtualenvOperator", "ExternalPythonOperator"):
            fn = kw.get("python_callable")
            t.note = f"PythonOperator callable {ast.unparse(fn) if fn else '?'}: wrap it in a script/notebook and point the task at it"
        else:
            t.note = f"{cls} has no automatic mapping"
        tasks[tid] = t
        if var:
            varmap[var] = [tid]

    for n in ast.walk(tree):
        if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
            reg(n.value, n.targets[0].id)
        elif isinstance(n, ast.Expr) and isinstance(n.value, ast.Call):
            reg(n.value, None)

    def ids(node):
        if isinstance(node, ast.Name):
            return varmap.get(node.id, [])
        if isinstance(node, (ast.List, ast.Tuple)):
            return [i for e in node.elts for i in ids(e)]
        if isinstance(node, ast.Call):                   # inline operator inside a >> expression
            reg(node, None)
            kw = {k.arg: k.value for k in node.keywords if k.arg}
            t = _lit(kw.get("task_id"))
            return [t] if isinstance(t, str) else []
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            return ids(node.right if isinstance(node.op, ast.RShift) else node.left)
        return []

    def chain_edges(node):
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            chain_edges(node.left); chain_edges(node.right)
            a, b = ids(node.left), ids(node.right)
            if isinstance(node.op, ast.LShift):
                a, b = ids(node.left if isinstance(node.left, (ast.Name, ast.List, ast.Tuple)) else node.left), ids(node.right)
                a, b = b, a
            for x in (a if isinstance(node.op, ast.RShift) else a):
                for y in b:
                    edges.append((x, y))

    for n in ast.walk(tree):
        if isinstance(n, ast.Expr) and isinstance(n.value, ast.BinOp):
            chain_edges(n.value)
        elif isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Attribute):
            f = n.value.func
            if f.attr in ("set_downstream", "set_upstream") and n.value.args:
                a, b = ids(f.value), ids(n.value.args[0])
                if f.attr == "set_upstream":
                    a, b = b, a
                edges += [(x, y) for x in a for y in b]
        elif isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and getattr(n.value.func, "id", "") == "chain":
            seq = [ids(a) for a in n.value.args]
            for a, b in zip(seq, seq[1:]):
                edges += [(x, y) for x in a for y in b]
    for a, b in dict.fromkeys(edges):
        if a in tasks and b in tasks and a != b and a not in tasks[b].depends:
            tasks[b].depends.append(a)

    name = _lit(dag_kw.get("dag_id")) or "airflow_dag"
    sch = _lit(dag_kw.get("schedule", dag_kw.get("schedule_interval")))
    cron = None
    if isinstance(sch, str):
        cron = sch
    elif sch is not None:
        report.warn("schedule", "schedule is not a cron string/preset (timedelta, timetable, dataset...): job left unscheduled")
    elif "schedule" in dag_kw or "schedule_interval" in dag_kw:
        report.warn("schedule", "schedule is an expression, not a literal: job left unscheduled")
    if any(k in dag_kw for k in ("catchup", "start_date", "sla_miss_callback")):
        report.note("start_date/catchup have no Databricks equivalent (backfills must be run explicitly)")
    return [Job(name=str(name), schedule=cron, tasks=list(tasks.values()))]


def read_pc_workflow(text: str, report: Report) -> list:
    root = safe_fromstring(text)
    sessions = {s.get("NAME"): s.get("MAPPINGNAME") for s in root.iter("SESSION")}
    jobs = []
    for wf in root.iter("WORKFLOW"):
        tasks = {}
        for ti in wf.iter("TASKINSTANCE"):
            nm, ttype = ti.get("NAME"), (ti.get("TASKTYPE") or "").lower()
            t = Task(key=nm)
            if ttype == "start":
                t.kind = "noop"
            elif ttype == "session":
                m = sessions.get(ti.get("TASKNAME") or nm)
                t.kind, t.file = "session", m or ""
                if not m:
                    t.kind, t.note = "unknown", f"session {nm}: mapping name not found in the export"
            elif ttype in ("command", "email", "decision", "timer", "assignment", "control", "event wait", "event raise"):
                t.kind, t.note = "unknown", f"{ttype} task: no automatic mapping"
            else:
                t.kind, t.note = "unknown", f"task type {ttype!r}: no automatic mapping"
            tasks[nm] = t
        for l in wf.iter("WORKFLOWLINK"):
            a, b = l.get("FROMTASK"), l.get("TOTASK")
            if a in tasks and b in tasks:
                tasks[b].depends.append(a)
                if l.get("CONDITION"):
                    report.warn(b, f"link {a} -> {b} has a condition ({l.get('CONDITION')}): Databricks 'run if' / condition tasks needed")
        if wf.find("SCHEDULER") is not None:
            report.warn(wf.get("NAME"), "workflow scheduler found: set the schedule manually (not converted)")
        jobs.append(Job(name=wf.get("NAME") or "workflow", tasks=list(tasks.values())))
    return jobs


def _ln(el):
    return el.tag.rsplit("}", 1)[-1].split(":")[-1]


def _attr(el, name):
    for k, v in el.attrib.items():
        if k.rsplit("}", 1)[-1].split(":")[-1].lower() == name.lower():
            return v
    return None


def read_ssis(text: str, report: Report) -> list:
    """SSIS package (.dtsx): control flow only. Data flow tasks are listed with their components, not converted."""
    root = safe_fromstring(text)
    tasks, order = {}, []
    def walk(el, path):
        for ex in [c for c in el if _ln(c) == "Executable"] + [c for ob in el if _ln(ob) in ("Executables",) for c in ob if _ln(c) == "Executable"]:
            ref = _attr(ex, "refId") or (path + "\\" + (_attr(ex, "ObjectName") or "task"))
            name = _attr(ex, "ObjectName") or ref.rsplit("\\", 1)[-1]
            typ = (_attr(ex, "ExecutableType") or "").lower()
            t = Task(key=name)
            if "executesqltask" in typ:
                t.kind = "sql"
                for e in ex.iter():
                    stmt = _attr(e, "SqlStatementSource")
                    if stmt:
                        t.sql = stmt
                        break
                if not t.sql:
                    t.kind, t.note = "unknown", "Execute SQL Task without an inline statement (variable or file source)"
            elif "pipeline" in typ:
                comps = [f"{_attr(c, 'name')} ({(_attr(c, 'componentClassID') or _attr(c, 'description') or '?').split('.')[-1]})"
                         for c in ex.iter() if _ln(c) == "component" and _attr(c, "name")]
                t.kind, t.note = "unknown", "Data Flow Task (components: " + (", ".join(comps) or "none found") + "): rebuild as a PySpark/SQL job - data flows are not converted"
            elif "executeprocess" in typ:
                exe = arg = None
                for e in ex.iter():
                    exe = exe or _attr(e, "Executable")
                    arg = arg or _attr(e, "Arguments")
                if exe:
                    classify(f"{exe} {arg or ''}", t, report)
                else:
                    t.note = "Execute Process Task without an executable"
            elif any(k in typ for k in ("sequence", "stsh", "foreachloop", "forloop")):
                t.kind = "noop"
                if "loop" in typ:
                    report.warn(name, "loop container flattened: its tasks run once, not per iteration (rewrite the loop as parameters / a for-each task)")
                walk(ex, ref)
            else:
                t.note = f"task type {typ or '?'} has no automatic mapping"
            tasks[ref] = t
            order.append(ref)
    walk(root, "Package")
    for pc in root.iter():
        if _ln(pc) == "PrecedenceConstraint":
            a, b = _attr(pc, "From"), _attr(pc, "To")
            if a in tasks and b in tasks:
                tasks[b].depends.append(tasks[a].key)
                val = _attr(pc, "Value")
                if (val not in (None, "0")) or _attr(pc, "Expression"):
                    report.warn(tasks[b].key, f"constraint {tasks[a].key} -> {tasks[b].key} is not plain 'on success' (failure/completion/expression): use a run-if condition")
    report.note("SSIS: schedule lives in SQL Server Agent (not in the package): set it on the Databricks job; connections/parameters are not converted")
    return [Job(name=_attr(root, "ObjectName") or "ssis_package", tasks=[tasks[k] for k in order])]


def _child_py(name: str) -> str:
    """file name the project converter gives to the converted child (same base name, .py)"""
    base = re.sub(r"\$\{[^}]*\}/?", "", (name or "").replace("\\", "/"))
    base = os.path.basename(base)
    return os.path.splitext(base)[0] + ".py" if base else "FILL_ME.py"


def read_pentaho_job(text: str, report: Report) -> list:
    """Pentaho Kettle job (.kjb): TRANS / JOB / SQL / SHELL entries, hops, START schedule."""
    root = safe_fromstring(text)
    jname = (root.findtext("name") or "pentaho_job").strip() or "pentaho_job"
    tasks, order, sched = {}, [], None
    for e in root.findall("entries/entry"):
        name, typ = (e.findtext("name") or "").strip(), (e.findtext("type") or "").strip().upper()
        t = Task(key=name or typ)
        if typ in ("TRANS", "JOB"):
            t.kind, t.file = "python", _child_py(e.findtext("filename") or e.findtext("name"))
            report.note(f"{name}: runs the {typ.lower()} converted from {e.findtext('filename') or '?'} (convert it with `sparqix project`; expected file {t.file})")
        elif typ == "SQL":
            t.kind, t.sql = "sql", (e.findtext("sql") or "").strip()
        elif typ in ("SHELL",):
            classify((e.findtext("script") or e.findtext("filename") or "").strip(), t, report)
        elif typ in ("SPECIAL", "SUCCESS", "DUMMY", "ABORT"):
            t.kind = "noop"
            if typ == "SPECIAL" and (e.findtext("start") or "").upper() == "Y":
                st = int(e.findtext("schedulerType") or 0)
                mi, hr = int(e.findtext("minutes") or 0), int(e.findtext("hour") or 0)
                if st == 1:
                    im = int(e.findtext("intervalMinutes") or 0)
                    ss = int(e.findtext("intervalSeconds") or 0)
                    if im:
                        sched = f"*/{im} * * * *"
                    elif ss:
                        report.warn(name, f"interval of {ss}s is below cron resolution: set a 1-minute schedule or use a continuous job")
                        sched = "* * * * *"
                elif st == 2:
                    sched = f"{mi} {hr} * * *"
                elif st == 3:
                    sched = f"{mi} {hr} * * {int(e.findtext('weekDay') or 1) % 7}"
                    report.warn(name, "weekly schedule: check the weekday numbering (Kettle's weekDay vs cron)")
                elif st == 4:
                    sched = f"{mi} {hr} {int(e.findtext('dayOfMonth') or 1)} * *"
            if typ == "ABORT":
                t.kind, t.note = "unknown", "Abort entry"
        else:
            t.note = f"job entry type {typ} has no automatic mapping"
        tasks[t.key] = t
        order.append(t.key)
    for h in root.findall("hops/hop"):
        a, b = (h.findtext("from") or "").strip(), (h.findtext("to") or "").strip()
        if (h.findtext("enabled") or "Y").upper() == "N":
            continue
        if a in tasks and b in tasks:
            tasks[b].depends.append(a)
            if (h.findtext("unconditional") or "Y").upper() != "Y" and (h.findtext("evaluation") or "Y").upper() == "N":
                report.warn(b, f"hop {a} -> {b} is an on-failure hop: converted to a plain dependency; set the task's run condition yourself")
    return [Job(name=re.sub(r"\W", "_", jname), schedule=sched, tasks=[tasks[k] for k in order])]


def read_talend_job(text: str, report: Report) -> list:
    """Talend job .item: tRunJob subjobs wired by SUBJOB_OK / COMPONENT_OK / RUN_IF / ERROR triggers.
    A job without tRunJob becomes one task that runs its converted script."""
    root = safe_fromstring(text)
    nodes = {}
    for n in root:
        if _ln(n) == "node":
            uid = next((p.get("value") for p in n if _ln(p) == "elementParameter" and p.get("name") == "UNIQUE_NAME"), n.get("componentName"))
            nodes[uid] = n
    flows, trig = [], []
    for c in root:
        if _ln(c) != "connection":
            continue
        k = c.get("connectorName")
        (flows if k in ("FLOW", "FILTER", "REJECT", "LOOKUP", "ITERATE") else trig).append((c.get("source"), c.get("target"), k))
    parent = {u: u for u in nodes}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for a, b, _ in flows:
        if a in parent and b in parent:
            parent[find(a)] = find(b)
    sub = {}
    for u in nodes:
        sub.setdefault(find(u), []).append(u)
    runjobs = [u for u in nodes if nodes[u].get("componentName") == "tRunJob"]
    if not runjobs:
        report.note("Talend job without tRunJob: one task running the converted job script (scheduling lives in Talend Administration Center / Cloud, not in the .item)")
        return [Job(name="talend_job", tasks=[Task(key="talend_job", kind="python", file="talend_job.py",
                                                    note="rename to the converted file")])]
    tasks, key_of = {}, {}
    for root_id, members in sub.items():
        rj = [m for m in members if m in runjobs]
        if rj:
            n = nodes[rj[0]]
            proc = next((p.get("value") for p in n if _ln(p) == "elementParameter" and p.get("name") == "PROCESS"), "")
            child = (proc or "").strip().strip('"') or rj[0]
            t = Task(key=re.sub(r"\W", "_", rj[0]), kind="python", file=re.sub(r"\W", "_", child) + ".py")
            report.note(f"{t.key}: runs child job {child}; convert it with `sparqix project` (expected file {t.file})")
        else:
            t = Task(key=re.sub(r"\W", "_", sorted(members)[0]), kind="unknown",
                     note="non-tRunJob subjob (" + ", ".join(sorted(nodes[m].get("componentName", "?") for m in members)[:4]) + "): convert the whole job with sparqix and run it as one task")
        tasks[t.key] = t
        for m in members:
            key_of[m] = t.key
    for a, b, k in trig:
        if a in key_of and b in key_of and key_of[a] != key_of[b]:
            tasks[key_of[b]].depends.append(key_of[a])
            if k in ("SUBJOB_ERROR", "COMPONENT_ERROR", "RUN_IF"):
                report.warn(key_of[b], f"trigger {k} from {a}: converted to a plain dependency; set the task's run condition yourself")
    for t in tasks.values():
        t.depends = list(dict.fromkeys(t.depends))
    return [Job(name="talend_job", tasks=list(tasks.values()))]


def read_alteryx_job(text: str, report: Report) -> list:
    report.note("Alteryx schedules live in Alteryx Server / Gallery, not in the .yxmd: converted to a single task, set the schedule on the Databricks job")
    return [Job(name="alteryx_workflow", tasks=[Task(key="alteryx_workflow", kind="python", file="alteryx_workflow.py", note="rename to the converted file")])]


def read_jobs(text: str, kind: str, report: Report) -> list:
    if kind == "auto":
        head = text[:20000]
        kind = ("ssis" if "<DTS:Executable" in head or "www.microsoft.com/SqlServer/Dts" in head else
                "pc-workflow" if "<WORKFLOW" in head.upper() else
                "pentaho-job" if re.search(r"<job>\s*<name>|<entries>\s*<entry>", head) else
                "talend" if re.search(r'<node\b[^>]*componentName="t[A-Z]', head) or "talendfile:ProcessType" in head else
                "alteryx" if "<AlteryxDocument" in head else
                "airflow" if re.search(r"\bDAG\s*\(|airflow", text) else "crontab")
    report.note(f"orchestration input: {kind}")
    readers = {"airflow": read_airflow, "crontab": read_crontab, "pc-workflow": read_pc_workflow, "ssis": read_ssis,
               "pentaho-job": read_pentaho_job, "talend": read_talend_job, "alteryx": read_alteryx_job}
    return readers[kind](text, report)


# ------------------------------------------------------------------ writers
def _fn_name(mapping: str) -> str:
    n = re.sub(r"\W", "_", mapping)
    return n if n.startswith("m_") else f"m_{n}"


def _runner(mapping: str, module: str):
    fn = _fn_name(mapping)
    return (f"run_{fn}.py", f"from pyspark.sql import SparkSession\nfrom {module} import {fn}\n\n"
                            f"spark = SparkSession.builder.getOrCreate()\n{fn}(spark)\n")


def _code_path(root: str, f: str) -> str:
    f = f.lstrip("./") if not os.path.isabs(f) else os.path.basename(f)
    return root.rstrip("/") + "/" + f


def _bridge(job: Job):
    """drop noop tasks, reconnecting their dependents to their upstreams"""
    by = {t.key: t for t in job.tasks}
    def up(key, seen=()):
        out = []
        for d in by[key].depends:
            if by[d].kind == "noop":
                out += up(d)
            else:
                out.append(d)
        return list(dict.fromkeys(out))
    keep = []
    for t in job.tasks:
        if t.kind != "noop":
            t.depends = up(t.key)
            keep.append(t)
    return keep


def to_databricks(job: Job, report: Report, code_root: str, module: str = "mappings"):
    """-> (job_json, {extra_filename: content})"""
    extra, tasks = {}, []
    for t in _bridge(job):
        d = {"task_key": re.sub(r"[^\w\-.]", "_", t.key)}
        if t.depends:
            d["depends_on"] = [{"task_key": re.sub(r"[^\w\-.]", "_", x)} for x in t.depends]
        if t.kind == "python":
            d["spark_python_task"] = {"python_file": _code_path(code_root, t.file), "parameters": t.args}
            d["job_cluster_key"] = "main"
        elif t.kind == "session":
            run, extra[run] = _runner(t.file, module)
            d["spark_python_task"] = {"python_file": _code_path(code_root, run), "parameters": []}
            d["job_cluster_key"] = "main"
            report.note(f"{t.key}: generated {run} (imports {_fn_name(t.file)} from module {module}); upload it next to {module}.py")
        elif t.kind == "sql":
            if t.file:
                path = _code_path(code_root, t.file)
            else:
                fn = d["task_key"] + ".sql"
                extra[fn] = t.sql.strip() + ("" if t.sql.strip().endswith(";") else ";") + "\n"
                path = _code_path(code_root, fn)
                report.warn(t.key, f"SQL extracted to {fn}: it still has the original dialect, run it through `sparqix --dialect ...` first")
            d["sql_task"] = {"file": {"path": path}, "warehouse_id": "FILL_ME"}
        elif t.kind == "dbt":
            d["dbt_task"] = {"commands": [t.sql], "warehouse_id": "FILL_ME"}
            report.warn(t.key, "dbt task: set project/profile settings (dbt_task) to match your repo")
        else:
            d["notebook_task"] = {"notebook_path": "FILL_ME"}
            d["job_cluster_key"] = "main"
            report.todo(t.key, t.note or "no automatic mapping")
        if t.retries:
            d["max_retries"] = t.retries
        tasks.append(d)
    out = {"name": job.name, "max_concurrent_runs": 1,
           "job_clusters": [{"job_cluster_key": "main", "new_cluster": {"spark_version": "FILL_ME", "node_type_id": "FILL_ME", "num_workers": 2}}],
           "tasks": tasks}
    if job.schedule:
        q = cron_to_quartz(job.schedule, report, job.name)
        if q:
            out["schedule"] = {"quartz_cron_expression": q, "timezone_id": job.tz, "pause_status": "PAUSED"}
            report.note(f"{job.name}: schedule created PAUSED (timezone assumed {job.tz}: check it)")
    report.note("job_clusters/warehouse ids are FILL_ME placeholders: creating the job fails until you set them")
    return out, extra


def to_airflow(job: Job, report: Report, code_root: str, module: str = "mappings") -> str:
    L = ["from datetime import datetime", "",
         "from airflow import DAG",
         "from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator",
         "from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator", ""]
    sched = repr(PRESETS.get(job.schedule, job.schedule)) if job.schedule else "None"
    L += [f"with DAG(dag_id={job.name!r}, schedule={sched}, start_date=datetime(2024, 1, 1), catchup=False) as dag:"]
    names = {}
    tasks = _bridge(job)
    for t in tasks:
        v = "t_" + re.sub(r"\W", "_", t.key)
        names[t.key] = v
        common = f"task_id={t.key!r}, retries={t.retries}"
        if t.kind in ("python", "session"):
            f = t.file if t.kind == "python" else _runner(t.file, module)[0]
            args = t.args if t.kind == "python" else []
            L.append(f"    {v} = DatabricksSubmitRunOperator({common}, existing_cluster_id='FILL_ME',")
            L.append(f"        spark_python_task={{'python_file': {_code_path(code_root, f)!r}, 'parameters': {args!r}}})")
            if t.kind == "session":
                report.note(f"{t.key}: needs {_runner(t.file, module)[0]} (imports {_fn_name(t.file)} from module {module}) next to the converted mapping")
        elif t.kind == "sql":
            q = t.sql if t.sql else f"-- run file {t.file}"
            L.append(f"    {v} = DatabricksSqlOperator({common}, sql_endpoint_name='FILL_ME', sql={q!r})")
            report.warn(t.key, "SQL keeps its original dialect: convert it with sparqix first")
        else:
            L.append(f"    # TODO(sparqix): {cm(t.note or t.kind)}")
            L.append(f"    {v} = DatabricksSubmitRunOperator({common}, existing_cluster_id='FILL_ME', notebook_task={{'notebook_path': 'FILL_ME'}})")
            report.todo(t.key, t.note or "no automatic mapping")
    L.append("")
    for t in tasks:
        for d in t.depends:
            L.append(f"    {names[d]} >> {names[t.key]}")
    code = "\n".join(L) + "\n"
    ast.parse(code)
    return code


# ------------------------------------------------------------------ CLI
def main(argv) -> int:
    import argparse
    p = argparse.ArgumentParser(prog="sparqix orchestrate")
    p.add_argument("file")
    p.add_argument("--from", dest="src", default="auto", choices=["auto", "airflow", "crontab", "pc-workflow", "ssis", "pentaho-job", "talend", "alteryx"])
    p.add_argument("--to", default="databricks", choices=["databricks", "airflow"])
    p.add_argument("--code-root", default="/Workspace/Shared/sparqix", help="where the converted scripts will live")
    p.add_argument("--mapping-module", default="mappings", help="Informatica: python module (converted file name, no .py) that holds the mapping functions")
    p.add_argument("-o", "--output", help="output folder (default: print)")
    a = p.parse_args(argv)
    report = Report()
    jobs = read_jobs(open(a.file, encoding="utf-8").read(), a.src, report)
    results = []
    for job in jobs:
        if a.to == "databricks":
            js, extra = to_databricks(job, report, a.code_root, a.mapping_module)
            results.append((f"{job.name}.job.json", json.dumps(js, indent=2) + "\n", extra))
        else:
            results.append((f"{job.name}_databricks_dag.py", to_airflow(job, report, a.code_root, a.mapping_module), {}))
    if a.output:
        os.makedirs(a.output, exist_ok=True)
        for fn, content, extra in results:
            for name, text in [(fn, content), *extra.items()]:
                with open(os.path.join(a.output, name), "w", encoding="utf-8") as fh:
                    fh.write(text)
            print("wrote", fn, *[f"+ {e}" for e in extra])
    else:
        for fn, content, extra in results:
            print(f"# ---- {fn}\n{content}")
            for name, text in extra.items():
                print(f"# ---- {name}\n{text}")
    text = report.summary()
    if text:
        import sys
        print("\n--- sparqix report ---\n" + text, file=sys.stderr)
    return 2 if report.todos else 0
