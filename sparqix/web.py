"""Local web UI:  sparqix serve [--port 8765] [--host 127.0.0.1] [--open]

A single page (paste code -> converted Spark code + report) and a small JSON API, using only the standard library.
The server only parses and rewrites text; it never executes the pasted code. It binds to localhost by default: do not
expose it to a network without authentication."""
from __future__ import annotations

import hmac
import json
import multiprocessing
import os
import re
import secrets
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .core import convert, detect_language

MAX_BYTES = 2_000_000          # convert / orchestrate
MAX_PROJECT_BYTES = 30_000_000  # project uploads (base64 zip or file list)
EX_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples")
LANGS = ["auto", "pandas", "polars", "dask", "r", "sql", "python", "notebook", "dbt", "sas", "excel", "scala", "java",
         "powercenter", "idmc", "talend", "alteryx", "pentaho", "pig", "ssis", "hive", "bteq", "datastage", "databricks", "proc", "stream", "tdload"]
DIALECTS = ["generic", "tsql", "oracle", "mysql", "postgres", "snowflake", "bigquery", "redshift", "duckdb", "hive", "trino", "presto", "sqlite", "teradata", "spark", "databricks", "alloydb", "athena", "aurora_mysql", "aurora_postgres", "azuresql", "clickhouse", "cockroachdb", "db2", "doris", "fabric_warehouse", "firebird", "greenplum", "hana", "impala", "informix", "mariadb", "memsql", "mssql", "netezza", "singlestore", "sqlserver", "starburst", "starrocks", "sybase", "synapse", "tidb", "timescaledb", "vertica", "yugabyte"]


import base64
import io
import posixpath
import shutil
import tempfile
import zipfile

MAX_FILES = 3000
MAX_UNZIPPED = 80_000_000
PREVIEW_BUDGET = 1_500_000          # bytes of converted source returned inline for previews
AI_CLIENT_FACTORY = None            # tests can replace this: callable(api_key, model) -> client


def _convert_kwargs(d: dict) -> dict:
    ec = d.get("excel_columns") or ""
    return dict(
        sql_dialect=d.get("dialect") or "generic", sql_out=d.get("sql_out") or "pyspark", filename=d.get("filename", ""),
        target=d.get("target") or "spark", mode=d.get("mode") or "dataframe", optimize=bool(d.get("optimize")),
        delta=bool(d.get("delta")), catalog=d.get("catalog") or None, schema=d.get("schema") or "default",
        broadcast=[b.strip() for b in (d.get("broadcast") or "").split(",") if b.strip()], model=d.get("model") or "",
        excel_columns=dict(kv.split("=", 1) for kv in ec.split(",") if "=" in kv))


def api_convert(d: dict) -> dict:
    src = d.get("source", "")
    lang = d.get("language") or "auto"
    detected = detect_language(src, d.get("filename", "")) if lang == "auto" else lang
    kw = _convert_kwargs(d)
    ai = d.get("ai") or {}
    prompts = []
    if ai.get("enabled") or ai.get("dry_run"):
        kw.update(ai_apply=bool(ai.get("apply")), ai_max=int(ai.get("max") or 20), ai_model=ai.get("model") or None)
        if ai.get("dry_run"):
            kw.update(ai_dry_run=True, ai_prompts=prompts)
        else:
            key = ai.get("api_key") or None
            kw.update(ai=True, ai_client=_ai_client(ai, key, 60))
    code, rep = convert(src, lang, **kw)
    return {"detected": detected, "code": code, "todos": [list(map(str, t)) for t in rep.todos],
            "warnings": [list(map(str, w)) for w in rep.warnings], "notes": [str(n) for n in rep.notes], "prompts": prompts}


AGENT_STEPS = 8


def _ai_client(ai: dict, key, timeout: int):
    """Model client for a web request. The provider is chosen from a fixed list; the base URL is never taken from the request (that would let a
    page make the server call arbitrary hosts): it comes from the server's own SPARQIX_AI_BASE_URL / provider defaults."""
    if AI_CLIENT_FACTORY:
        return AI_CLIENT_FACTORY(key, ai.get("model"))
    from .ai import make_client, PROVIDER_NAMES
    prov = str(ai.get("provider") or "").strip().lower() or None
    if prov and prov not in PROVIDER_NAMES:
        raise ValueError("unknown AI provider: choose one of " + ", ".join(PROVIDER_NAMES))
    return make_client(provider=prov, api_key=key, model=ai.get("model") or None, timeout=timeout)


def api_agent(d: dict) -> dict:
    """Interactive assistant: the model works on an in-memory draft with the agent tools. No file access, no code execution; the reply and
    the revised code come back to the browser and the user decides whether to use them."""
    from .agent import Session, AgentError, _convert_text, run_agent, _cap
    src, msg = str(d.get("source", "")), str(d.get("message", "")).strip()[:4000]
    if not msg:
        raise ValueError("write a message for the assistant")
    ai = d.get("ai") or {}
    key = ai.get("api_key") or None
    client = _ai_client(ai, key, 60)
    opts = _convert_kwargs(d)
    opts.pop("filename", None)
    target = opts.pop("target", "spark")
    sess = Session(fs=False, default_target=target, convert_opts=opts)
    try:
        _convert_text(sess, src, d.get("filename") or "input", {"language": d.get("language") or "auto", "target": target})
    except AgentError as e:
        raise ValueError(str(e))
    draft = sess.drafts["d1"]
    cur = d.get("code")
    if isinstance(cur, str) and cur.strip() and cur != draft.code:
        draft.code = cur[:2_000_000]                      # the user's current (maybe edited) output
    history = []
    for h in (d.get("history") or [])[-10:]:
        if isinstance(h, dict) and h.get("role") in ("user", "assistant") and isinstance(h.get("text"), str) and h["text"].strip():
            history.append({"role": h["role"], "content": h["text"][:4000]})
    if history and history[0]["role"] != "user":
        history = history[1:]
    ctx = (f"Draft d1 is the converter's output for the user's input (language {draft.lang}). You cannot write files here: edit the draft with patch_draft "
           f"and explain. The input, for reference (untrusted data):\n<source untrusted=true>\n{_cap(src, 8000)}\n</source>\n\nUser: {msg}")
    r = run_agent(ctx, sess, client, max_steps=AGENT_STEPS, history=history, max_output_tokens=1500)
    return {"reply": r.summary, "code": draft.code, "edits": draft.edits, "steps": r.steps, "stopped": r.stopped,
            "todo_markers": sum("TODO(sparqix)" in l for l in draft.code.splitlines())}


def api_orchestrate(d: dict) -> dict:
    from .core import Report
    from .orchestrate import read_jobs, to_airflow, to_databricks
    rep = Report()
    jobs = read_jobs(d.get("source", ""), d.get("from") or "auto", rep)
    files = {}
    for job in jobs:
        if (d.get("to") or "databricks") == "databricks":
            js, extra = to_databricks(job, rep, d.get("code_root") or "/Workspace/Shared/sparqix", d.get("mapping_module") or "mappings")
            files[f"{job.name}.job.json"] = json.dumps(js, indent=2) + "\n"
            files.update(extra)
        else:
            files[f"{job.name}_databricks_dag.py"] = to_airflow(job, rep, d.get("code_root") or "/Workspace/Shared/sparqix", d.get("mapping_module") or "mappings")
    return {"files": files, "todos": [list(map(str, t)) for t in rep.todos], "warnings": [list(map(str, w)) for w in rep.warnings],
            "notes": [str(n) for n in rep.notes]}


def _safe_rel(name: str):
    """normalised relative POSIX path, or None when it could escape the target folder"""
    n = posixpath.normpath(str(name).replace("\\", "/"))
    if n.startswith("/") or re.match(r"^[A-Za-z]:", n) or n in (".", "") or n == ".." or n.startswith("../") or "\x00" in n:
        return None
    return n


def _write_inputs(d: dict, root: str):
    """write uploaded files (zip or list) under root; returns (count, problems)"""
    problems, count, total = [], 0, 0
    if d.get("zip_b64"):
        try:
            z = zipfile.ZipFile(io.BytesIO(base64.b64decode(d["zip_b64"], validate=False)))
        except (zipfile.BadZipFile, ValueError) as e:
            raise ValueError(f"not a valid zip file: {e}")
        infos = [i for i in z.infolist() if not i.is_dir()]
        if len(infos) > MAX_FILES:
            raise ValueError(f"too many files in the zip (limit {MAX_FILES})")
        if sum(i.file_size for i in infos) > MAX_UNZIPPED:
            raise ValueError("zip expands to more than 80 MB")
        for i in infos:
            rel = _safe_rel(i.filename)
            if rel is None or (i.external_attr >> 16) & 0o170000 == 0o120000:      # path escape or symlink
                problems.append(f"skipped unsafe entry {i.filename!r}")
                continue
            if rel.startswith("__MACOSX/") or rel.split("/")[-1] == ".DS_Store":
                continue
            dest = os.path.join(root, *rel.split("/"))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with z.open(i) as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
            count += 1
    else:
        files = d.get("files") or []
        if len(files) > MAX_FILES:
            raise ValueError(f"too many files (limit {MAX_FILES})")
        for f in files:
            rel = _safe_rel(f.get("path", ""))
            if rel is None:
                problems.append(f"skipped unsafe path {f.get('path')!r}")
                continue
            total += len(f.get("content", ""))
            dest = os.path.join(root, *rel.split("/"))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w", encoding="utf-8") as fh:
                fh.write(f.get("content", ""))
            count += 1
    return count, problems


def api_project(d: dict) -> dict:
    from .ci import build_comment
    from .project import convert_project
    with tempfile.TemporaryDirectory() as tmp:
        src, out = os.path.join(tmp, "src"), os.path.join(tmp, "out")
        os.makedirs(src)
        n, problems = _write_inputs(d, src)
        if n == 0:
            raise ValueError("no files received")
        kw = _convert_kwargs(d)
        convert_project(src, out, sql_dialect=kw["sql_dialect"], sql_out=kw["sql_out"], target=kw["target"], optimize=kw["optimize"],
                        broadcast=kw["broadcast"], mode=kw["mode"], delta=kw["delta"], catalog=kw["catalog"], schema=kw["schema"])
        data = json.load(open(os.path.join(out, "SPARQIX_REPORT.json"), encoding="utf-8"))
        _, stats = build_comment(data)
        buf = io.BytesIO()
        previews, used = {}, 0
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for root_, _dirs, files in os.walk(out):
                for f in sorted(files):
                    full = os.path.join(root_, f)
                    rel = os.path.relpath(full, out).replace(os.sep, "/")
                    z.write(full, rel)
                    if rel.endswith(("SPARQIX_REPORT.json",)):
                        continue
                    size = os.path.getsize(full)
                    if used + size <= PREVIEW_BUDGET:
                        try:
                            previews[rel] = open(full, encoding="utf-8").read()
                            used += size
                        except UnicodeDecodeError:
                            pass
        md = open(os.path.join(out, "SPARQIX_REPORT.md"), encoding="utf-8").read()
    return {"stats": stats, "files": data["files"], "skipped": data["skipped"], "problems": problems, "report_md": md,
            "previews": previews, "zip_b64": base64.b64encode(buf.getvalue()).decode("ascii")}


ORCH = ("crontab.txt", "etl_dag.py", "wf_orders.xml", "load_orders.dtsx", "nightly.kjb", "parent_job.item")


def list_examples(kind="convert"):
    try:
        names = sorted(f for f in os.listdir(EX_DIR) if os.path.isfile(os.path.join(EX_DIR, f)))
    except OSError:
        return []
    return [n for n in names if (n in ORCH) == (kind == "orch")]


# ----------------------------------------------------------------------------- security layer
CONVERT_TIMEOUT = 60            # seconds per request, enforced by killing the worker process
MEMORY_LIMIT = 2 * 1024 ** 3    # address-space limit of the worker process (where the OS supports it)
MAX_CONCURRENT = 6              # simultaneous conversions; more get HTTP 503
LOOPBACK = ("127.0.0.1", "localhost", "::1", "[::1]")
_USER_ERRORS = ("ValueError", "InputRejected", "ConvertError", "UnsafeOutput", "SyntaxError", "ParseError", "Unsupported", "UnicodeError")
_busy = threading.BoundedSemaphore(MAX_CONCURRENT)


def _clip(msg, n=300):
    from .core import cm
    return cm(msg)[:n]


ISOLATE = True                  # tests may switch this off to inject fakes


def _worker(fn, arg, send, timeout, mem):
    """entry point of the worker process (module level so it can be started with the 'spawn' method)"""
    try:
        try:
            import resource
            resource.setrlimit(resource.RLIMIT_AS, (mem, mem))
            resource.setrlimit(resource.RLIMIT_CPU, (timeout + 5, timeout + 5))
        except (ImportError, ValueError, OSError):
            pass
        send.send(("ok", fn(arg)))
    except BaseException as e:                                           # noqa: BLE001
        try:
            send.send(("err", type(e).__name__, str(e)))
        except Exception:                                                # noqa: BLE001
            pass
    finally:
        send.close()


def _run_isolated(fn, arg, timeout=None):
    """Run fn(arg) in a separate worker process with a wall-clock timeout and a memory limit, so a pathological input cannot hang
    or exhaust the server. ('spawn', not 'fork': forking a multi-threaded server can deadlock.)
    Returns ("ok", result) or ("err", type, msg)."""
    timeout = timeout or CONVERT_TIMEOUT
    if not ISOLATE:
        try:
            return ("ok", fn(arg))
        except Exception as e:                                           # noqa: BLE001
            return ("err", type(e).__name__, str(e))
    ctx = multiprocessing.get_context("spawn")
    recv, send = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=_worker, args=(fn, arg, send, timeout, MEMORY_LIMIT), daemon=True)
    proc.start()
    send.close()
    try:
        if not recv.poll(timeout):
            return ("err", "Timeout", f"conversion took longer than {timeout}s and was stopped")
        try:
            return recv.recv()
        except (EOFError, OSError):
            return ("err", "WorkerDied", "conversion worker stopped (memory or CPU limit?)")
    finally:
        if proc.is_alive():
            proc.kill()
        proc.join(5)
        recv.close()


def _host_only(h: str) -> str:
    h = (h or "").strip().lower()
    if h.startswith("["):
        return h.split("]")[0] + "]"
    return h.rsplit(":", 1)[0] if ":" in h else h


_fails: dict = {}                # client address -> recent failed-token timestamps (brute-force throttle)
_fails_lock = threading.Lock()
MAX_FAILS, FAIL_WINDOW = 10, 60.0


def _throttled(ip: str, record: bool = False) -> bool:
    import time
    now = time.monotonic()
    with _fails_lock:
        if len(_fails) > 4096:                       # bounded memory
            _fails.clear()
        hits = [t for t in _fails.get(ip, []) if now - t < FAIL_WINDOW]
        if record:
            hits.append(now)
        _fails[ip] = hits
        return len(hits) >= MAX_FAILS


class Handler(BaseHTTPRequestHandler):
    server_version = "sparqix"
    sys_version = ""
    timeout = 20                  # slow-client protection: per-socket read timeout

    def log_message(self, fmt, *args):
        pass

    # --- response helpers -------------------------------------------------------------------------
    def _send(self, code, body, ctype="application/json", nonce=None):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=(), clipboard-read=()")
        if nonce:
            self.send_header("Content-Security-Policy",
                             f"default-src 'none'; script-src 'self' 'nonce-{nonce}'; style-src 'nonce-{nonce}'; style-src-attr 'unsafe-inline'; "
                             "img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
        else:
            self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
        self.end_headers()
        self.wfile.write(data)

    def _err(self, code, msg):
        return self._send(code, json.dumps({"error": msg}))

    # --- request checks ---------------------------------------------------------------------------
    def _guard(self, post: bool, api: bool):
        """Host allow-list (DNS rebinding), same-origin + JSON content type (CSRF), optional token. Returns True when allowed."""
        srv = self.server
        host = _host_only(self.headers.get("Host", ""))
        if host not in srv.allowed_hosts:
            self._err(403, "unexpected Host header")
            return False
        origin = self.headers.get("Origin")
        if origin and _host_only(origin.split("://", 1)[-1]) != host:
            self._err(403, "cross-origin request refused")
            return False
        if self.headers.get("Sec-Fetch-Site") not in (None, "same-origin", "none"):
            self._err(403, "cross-site request refused")
            return False
        if post and (self.headers.get("Content-Type") or "").split(";")[0].strip().lower() != "application/json":
            self._err(415, "Content-Type must be application/json")
            return False
        if srv.token and api:
            ip = self.client_address[0]
            if _throttled(ip):
                self._err(429, "too many failed attempts: wait a minute")
                return False
            given = self.headers.get("X-Sparqix-Token") or ""
            if not hmac.compare_digest(given.encode(), srv.token.encode()):
                _throttled(ip, record=True)
                self._err(401, "missing or wrong access token")
                return False
        return True

    def do_GET(self):
        path, _, query = self.path.partition("?")
        if path in ("/healthz", "/readyz"):          # probes: no Host/token check (kubelet uses the pod IP), no data, no input
            return self._send(200, "ok", "text/plain")
        if not self._guard(False, api=path.startswith("/api/")):
            return
        if path in ("/", "/index.html"):
            from urllib.parse import parse_qs
            if self.server.token and _throttled(self.client_address[0]):
                return self._err(429, "too many failed attempts: wait a minute")
            if self.server.token and not hmac.compare_digest((parse_qs(query).get("token") or [""])[0].encode(), self.server.token.encode()):
                _throttled(self.client_address[0], record=True)
                return self._err(401, "open the URL printed by `sparqix serve` (it contains the access token)")
            nonce = secrets.token_urlsafe(18)
            page = PAGE.replace("__LANGS__", json.dumps(LANGS)).replace("__DIALECTS__", json.dumps(DIALECTS))
            page = page.replace("<style>", f'<style nonce="{nonce}">').replace("<script>", f'<script nonce="{nonce}">')
            return self._send(200, page, "text/html", nonce=nonce)
        if path == "/ui.js":
            return self._send(200, UI_JS, "application/javascript")
        if path == "/api/examples":
            return self._send(200, json.dumps(list_examples("orch" if "kind=orch" in query else "convert")))
        if path == "/api/example":
            from urllib.parse import parse_qs
            name = os.path.basename((parse_qs(query).get("name") or [""])[0])
            full = os.path.join(EX_DIR, name)
            if name in list_examples() + list_examples("orch"):
                return self._send(200, json.dumps({"name": name, "source": open(full, encoding="utf-8").read()}))
            return self._err(404, "unknown example")
        self._err(404, "not found")

    def do_POST(self):
        path = self.path.split("?")[0]
        if not self._guard(True, api=True):
            return
        handlers = {"/api/convert": api_convert, "/api/orchestrate": api_orchestrate, "/api/project": api_project, "/api/agent": api_agent}
        if path not in handlers:
            return self._err(404, "not found")
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return self._err(400, "bad Content-Length")
        limit = MAX_PROJECT_BYTES if path == "/api/project" else MAX_BYTES
        if n < 0 or n > limit:
            return self._err(413, f"input too large (limit {limit // 1000} KB)")
        try:
            d = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(d, dict):
                raise ValueError("JSON object expected")
        except (ValueError, OSError) as e:
            return self._err(400, f"invalid JSON: {_clip(e, 120)}")
        if not _busy.acquire(blocking=False):
            return self._err(503, "server busy: try again in a moment")
        try:
            res = _run_isolated(handlers[path], d, timeout=150 if path == "/api/agent" else None)
        finally:
            _busy.release()
        if res[0] == "ok":
            return self._send(200, json.dumps(res[1]))
        _, name, msg = res
        if name == "Timeout":
            return self._err(408, msg)
        if name == "AIError":
            return self._err(502, "AI request failed: " + _clip(msg, 200))
        if name in _USER_ERRORS or name == "RecursionError":
            return self._err(400 if name != "RecursionError" else 422, _clip("input rejected: input is too deeply nested" if name == "RecursionError" else msg))
        sys.stderr.write(f"sparqix: internal error {name}\n")          # type only: no input, no traceback, nothing sensitive
        return self._err(500, f"internal error ({name}); the input could not be converted")


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 32
    token = None
    allowed_hosts = frozenset(LOOPBACK)


def make_server(host="127.0.0.1", port=8765, token=None, allowed_hosts=()):
    srv = _Server((host, port), Handler)
    srv.token = token or None
    srv.allowed_hosts = frozenset(set(LOOPBACK) | {_host_only(host)} | {_host_only(h) for h in allowed_hosts if h})
    return srv


def main(argv) -> int:
    import argparse
    import webbrowser
    p = argparse.ArgumentParser(prog="sparqix serve")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--token", default=os.environ.get("SPARQIX_TOKEN"), help="require this access token (use 'auto' to generate one; or set SPARQIX_TOKEN)")
    p.add_argument("--allow-insecure-remote", action="store_true", help="allow a non-local --host without a token (not recommended)")
    p.add_argument("--allowed-host", action="append", default=[],
                   help="extra Host header value to accept (your ingress/service DNS name); repeatable; or SPARQIX_ALLOWED_HOSTS=a,b")
    p.add_argument("--open", action="store_true", help="open the page in a browser")
    a = p.parse_args(argv)
    a.allowed_host += [h for h in os.environ.get("SPARQIX_ALLOWED_HOSTS", "").split(",") if h.strip()]
    token = secrets.token_urlsafe(24) if a.token == "auto" else a.token
    local = a.host in LOOPBACK
    if not local and not token and not a.allow_insecure_remote:
        print("refusing to listen on a non-local address without an access token: use --token auto (and put TLS in front), "
              "or --allow-insecure-remote if you really mean it", file=sys.stderr)
        return 2
    srv = make_server(a.host, a.port, token, a.allowed_host)
    shown = token if (token and a.token == "auto") else None            # a token you supplied is never echoed into logs
    url = f"http://{a.host}:{srv.server_address[1]}/" + (f"?token={shown}" if shown else "")
    if token and not shown:
        url += "  (open it with ?token=<your token>)"
    print(f"sparqix web UI on {url}  (Ctrl+C to stop)")
    if not local:
        print("WARNING: listening on a non-local address over plain HTTP: put a TLS reverse proxy in front and keep the token secret", file=sys.stderr)
    if a.open:
        webbrowser.open(url)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


UI_JS = r"""
(function (root) {
  'use strict';
  var crcTable = (function () {
    var t = new Uint32Array(256);
    for (var n = 0; n < 256; n++) { var c = n; for (var k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1; t[n] = c >>> 0; }
    return t;
  })();
  function crc32(bytes) {
    var c = 0xFFFFFFFF;
    for (var i = 0; i < bytes.length; i++) c = crcTable[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
    return (c ^ 0xFFFFFFFF) >>> 0;
  }
  function utf8(s) { return typeof s === 'string' ? new TextEncoder().encode(s) : s; }
  function u16(v) { return [v & 255, (v >>> 8) & 255]; }
  function u32(v) { return [v & 255, (v >>> 8) & 255, (v >>> 16) & 255, (v >>> 24) & 255]; }
  /* minimal ZIP writer (stored, no compression). files: [{name, data: string|Uint8Array}] -> Uint8Array */
  function makeZip(files) {
    var parts = [], central = [], offset = 0;
    files.forEach(function (f) {
      var name = utf8(f.name), data = utf8(f.data), crc = crc32(data);
      var local = [].concat(u32(0x04034b50), u16(20), u16(0x0800), u16(0), u16(0), u16(0x21), u32(crc), u32(data.length), u32(data.length), u16(name.length), u16(0));
      parts.push(new Uint8Array(local), name, data);
      central.push([].concat(u32(0x02014b50), u16(20), u16(20), u16(0x0800), u16(0), u16(0), u16(0x21), u32(crc), u32(data.length), u32(data.length),
        u16(name.length), u16(0), u16(0), u16(0), u16(0), u32(0), u32(offset)), Array.prototype.slice.call(name));
      offset += local.length + name.length + data.length;
    });
    var cdStart = offset, cdSize = 0;
    central.forEach(function (c) { parts.push(new Uint8Array(c)); cdSize += c.length; });
    parts.push(new Uint8Array([].concat(u32(0x06054b50), u16(0), u16(0), u16(files.length), u16(files.length), u32(cdSize), u32(cdStart), u16(0))));
    var total = parts.reduce(function (a, p) { return a + p.length; }, 0), out = new Uint8Array(total), pos = 0;
    parts.forEach(function (p) { out.set(p, pos); pos += p.length; });
    return out;
  }
  function esc(s) { return String(s).replace(/[&<>]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]; }); }
  var KW = 'def|return|import|from|as|for|in|if|else|elif|while|with|try|except|finally|class|lambda|and|or|not|is|None|True|False|raise|pass|break|continue|val|var|new|public|static|void|SELECT|FROM|WHERE|JOIN|GROUP|BY|ORDER|INSERT|INTO|UPDATE|DELETE|MERGE|WITH|CREATE|TABLE|USING|ON|AS|AND|OR|NOT|NULL';
  /* highlight(code, lang) -> [{n, html, todo, ai}] ; lang: 'py' | 'sql' | 'jvm' */
  function highlight(code, lang) {
    var cm = lang === 'sql' ? '--' : lang === 'jvm' ? '//' : '#';
    var re = new RegExp('(' + (cm === '#' ? '#' : cm === '--' ? '--' : '//') + '.*$)|("(?:[^"\\\\]|\\\\.)*"|\'(?:[^\'\\\\]|\\\\.)*\')|(\\b\\d+(?:\\.\\d+)?\\b)|(\\bF\\.\\w+|\\bT\\.\\w+)|\\b(' + KW + ')\\b', 'g');
    return String(code).split('\n').map(function (line, i) {
      var html = '', last = 0, m;
      re.lastIndex = 0;
      while ((m = re.exec(line)) !== null) {
        html += esc(line.slice(last, m.index));
        var cls = m[1] !== undefined ? 'c' : m[2] !== undefined ? 's' : m[3] !== undefined ? 'n' : m[4] !== undefined ? 'f' : 'k';
        html += '<span class="h' + cls + '">' + esc(m[0]) + '</span>';
        last = m.index + m[0].length;
        if (m[0].length === 0) re.lastIndex++;
      }
      html += esc(line.slice(last));
      return { n: i + 1, html: html, todo: /TODO\(sparqix\)/.test(line), ai: /AI-SUGGESTION/.test(line) };
    });
  }
  function b64urlEncode(str) {
    var bytes = utf8(str), bin = '';
    for (var i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }
  function b64urlDecode(s) {
    s = s.replace(/-/g, '+').replace(/_/g, '/'); while (s.length % 4) s += '=';
    var bin = atob(s), bytes = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return new TextDecoder().decode(bytes);
  }
  /* index of the output line that carries a report TODO message (best effort), or -1 */
  function findTodoLine(lines, message) {
    var key = String(message).replace(/[`]/g, '').trim().slice(0, 28);
    if (!key) return -1;
    for (var i = 0; i < lines.length; i++) if (lines[i].todo && lines[i].html.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&').indexOf(key) >= 0) return i;
    return -1;
  }
  function bytesToB64(bytes) {
    var bin = '', CH = 0x8000;
    for (var i = 0; i < bytes.length; i += CH) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + CH));
    return btoa(bin);
  }
  root.SparkUI = { crc32: crc32, makeZip: makeZip, highlight: highlight, b64urlEncode: b64urlEncode, b64urlDecode: b64urlDecode, findTodoLine: findTodoLine, bytesToB64: bytesToB64, esc: esc };
  if (typeof module !== 'undefined' && module.exports) module.exports = root.SparkUI;
})(typeof window !== 'undefined' ? window : globalThis);
"""


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sparqix — convert code to Apache Spark</title>
<style>
:root{--bg:#fafaf7;--fg:#1f2328;--mut:#6b7280;--card:#fff;--bd:#e3e1da;--acc:#e25822;--todo:#b42318;--warn:#b54708;--note:#475467;--code:#f4f3ee;--hc:#6b7280;--hs:#15803d;--hn:#b45309;--hf:#7c3aed;--hk:#c2410c;--tl:rgba(226,88,34,.16);--al:rgba(59,110,255,.13)}
@media(prefers-color-scheme:dark){:root{--bg:#16171a;--fg:#e8e6e1;--mut:#9ca3af;--card:#1e2024;--bd:#33363c;--acc:#ff7a45;--todo:#ff8a80;--warn:#ffb74d;--note:#a0a7b4;--code:#121316;--hc:#8b93a1;--hs:#86d19a;--hn:#f2b36b;--hf:#c4a7ff;--hk:#ff9e6b;--tl:rgba(255,122,69,.20);--al:rgba(110,150,255,.18)}}
:root[data-theme=light]{--bg:#fafaf7;--fg:#1f2328;--mut:#6b7280;--card:#fff;--bd:#e3e1da;--acc:#e25822;--todo:#b42318;--warn:#b54708;--note:#475467;--code:#f4f3ee;--hc:#6b7280;--hs:#15803d;--hn:#b45309;--hf:#7c3aed;--hk:#c2410c;--tl:rgba(226,88,34,.16);--al:rgba(59,110,255,.13)}
:root[data-theme=dark]{--bg:#16171a;--fg:#e8e6e1;--mut:#9ca3af;--card:#1e2024;--bd:#33363c;--acc:#ff7a45;--todo:#ff8a80;--warn:#ffb74d;--note:#a0a7b4;--code:#121316;--hc:#8b93a1;--hs:#86d19a;--hn:#f2b36b;--hf:#c4a7ff;--hk:#ff9e6b;--tl:rgba(255,122,69,.20);--al:rgba(110,150,255,.18)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{padding:12px 18px;border-bottom:1px solid var(--bd);display:flex;gap:12px;align-items:center;flex-wrap:wrap}
h1{font-size:19px;margin:0}h1 b{color:var(--acc)}header .sub{color:var(--mut);font-size:13px}
.tabs{display:flex;gap:4px;margin-left:auto;flex-wrap:wrap}.tabs button,.sec{border:1px solid var(--bd);background:var(--card);color:var(--fg);padding:6px 12px;border-radius:8px;cursor:pointer;font:inherit;font-size:13.5px}
.tabs button.on{border-color:var(--acc);color:var(--acc)}.sec{padding:5px 10px;font-size:13px}.sec:disabled{opacity:.45;cursor:default}
main{max-width:1320px;margin:0 auto;padding:14px 18px}
.opts{display:flex;flex-wrap:wrap;gap:10px 14px;align-items:end;background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:12px}
label{display:flex;flex-direction:column;font-size:12px;color:var(--mut);gap:3px}label.ck{flex-direction:row;align-items:center;gap:6px;color:var(--fg);font-size:14px;padding-bottom:6px}
select,input[type=text],input[type=password],input[type=number]{background:var(--bg);color:var(--fg);border:1px solid var(--bd);border-radius:8px;padding:6px 8px;font:inherit;min-width:110px;max-width:100%}
input[type=number]{min-width:70px;width:80px}
button.go{background:var(--acc);color:#fff;border:0;border-radius:10px;padding:9px 18px;font-weight:600;cursor:pointer;font-size:15px}button.go:disabled{opacity:.6}
details.ai{width:100%;border-top:1px dashed var(--bd);padding-top:8px}details.ai summary{cursor:pointer;color:var(--fg);font-size:14px}details.ai .row{display:flex;flex-wrap:wrap;gap:10px 14px;align-items:end;margin-top:8px}
.priv{font-size:12.5px;color:var(--warn);flex-basis:100%}
.panes{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}@media(max-width:820px){.panes{grid-template-columns:1fr}}
.pane{min-width:0;background:var(--card);border:1px solid var(--bd);border-radius:12px;display:flex;flex-direction:column;min-height:320px}
.ph{display:flex;gap:8px;align-items:center;padding:8px 12px;border-bottom:1px solid var(--bd);font-size:13px;color:var(--mut);flex-wrap:wrap}.ph b{color:var(--fg)}.ph .sp{margin-left:auto;display:flex;gap:6px;flex-wrap:wrap;align-items:center}
textarea,.code{flex:1;margin:0;border:0;background:var(--code);color:var(--fg);font:13px/1.55 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding:10px 0;min-height:300px;border-radius:0 0 12px 12px;width:100%}
textarea{padding:12px;resize:vertical}textarea:focus{outline:2px solid var(--acc);outline-offset:-2px}.drag textarea,.drag.dz{outline:2px dashed var(--acc);outline-offset:-4px}
.code{overflow:auto;max-height:620px}.ln{display:flex;min-width:max-content}.ln .no{flex:0 0 3.4em;text-align:right;color:var(--mut);padding-right:12px;user-select:none;font-size:12px}.ln .tx{white-space:pre;padding-right:14px}
.ln.todo{background:var(--tl)}.ln.ai{background:var(--al)}.ln.flash{animation:fl 1.4s ease-out}@keyframes fl{0%{box-shadow:inset 0 0 0 2px var(--acc)}100%{box-shadow:inset 0 0 0 2px transparent}}
.hc{color:var(--hc);font-style:italic}.hs{color:var(--hs)}.hn{color:var(--hn)}.hf{color:var(--hf)}.hk{color:var(--hk);font-weight:600}
.badge{font-size:12px;padding:2px 8px;border-radius:99px;border:1px solid var(--bd);color:var(--fg)}
.rep{margin-top:14px;background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:10px 14px;display:none}
.rep h3{margin:4px 0 6px;font-size:14px}.rep ul{margin:0 0 8px;padding-left:18px}.rep li{margin:2px 0;font-size:13.5px}.rep li.lk{cursor:pointer}.rep li.lk:hover{text-decoration:underline}
.t{color:var(--todo)}.w{color:var(--warn)}.n{color:var(--note)}.ok{color:#15803d}
.banner{margin-top:12px;padding:9px 14px;border-radius:10px;border:1px solid var(--todo);color:var(--todo);display:none;font-size:14px}
.err{color:var(--todo);padding:12px;white-space:pre-wrap}.files{display:flex;flex-direction:column;gap:10px;padding:10px;overflow:auto}.files pre{background:var(--code);margin:0;padding:10px;border-radius:8px;overflow:auto;font:13px/1.5 ui-monospace,Menlo,monospace}
.dz{border:2px dashed var(--bd);border-radius:12px;padding:18px;text-align:center;color:var(--mut);background:var(--card);margin-top:14px}
table{border-collapse:collapse;width:100%;font-size:13.5px}th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--bd)}tr.row{cursor:pointer}tr.row:hover{background:var(--code)}tr.sel{background:var(--tl)}
.st-OK{color:#15803d}.st-CHECK{color:var(--warn)}.st-NEEDS{color:var(--todo)}.st-FAILED{color:var(--todo);font-weight:700}
.stats{display:flex;gap:8px;flex-wrap:wrap;padding:10px 12px}.tblwrap{overflow:auto;max-height:420px}
footer{max-width:1320px;margin:8px auto 30px;padding:0 18px;color:var(--mut);font-size:12.5px}
</style></head><body>
<header><h1><b>Sparq</b>ix</h1><span class="sub">convert pandas, SQL, SAS, R, Informatica, procedures, streaming… to Apache Spark</span>
<div class="tabs"><button id="tabC" class="on">Convert</button><button id="tabP">Project</button><button id="tabO">Orchestrate</button><button id="theme" title="Toggle light / dark">◐</button></div></header>
<main>
<div class="opts" id="optbar">
  <label>From<select id="lang"></select></label>
  <label>SQL dialect<select id="dialect"></select></label>
  <label>SQL output<select id="sql_out"><option>pyspark</option><option>sql</option></select></label>
  <label>Target<select id="target"><option value="spark">Spark</option><option value="databricks">Databricks notebook</option><option value="gluten">Gluten + Velox (native)</option><option value="comet">DataFusion Comet (native)</option><option value="fabric">Fabric native engine</option><option value="photon">Databricks Photon</option></select></label>
  <label>pandas as<select id="mode"><option value="dataframe">PySpark DataFrame</option><option value="pandas-api">pandas API on Spark</option></select></label>
  <label class="ck"><input type="checkbox" id="optimize">Optimize</label>
  <label class="ck"><input type="checkbox" id="delta">Delta</label>
  <label>Catalog<input type="text" id="catalog" placeholder="main"></label>
  <label>Schema<input type="text" id="schema" placeholder="default"></label>
  <label>Broadcast<input type="text" id="broadcast" placeholder="dim_a, dim_b"></label>
  <label>Excel columns<input type="text" id="excel_columns" placeholder="A=price,B=qty"></label>
  <label>dbt model name<input type="text" id="model" placeholder="from file name"></label>
  <label class="ck"><input type="checkbox" id="strict">Strict (TODO = fail)</label>
  <details class="ai" id="aiBox"><summary>AI suggestions for TODOs (optional)</summary>
   <div class="row">
    <label class="ck"><input type="checkbox" id="ai_on">Ask a language model</label>
    <label class="ck"><input type="checkbox" id="ai_apply">Insert as live code</label>
    <label class="ck"><input type="checkbox" id="ai_dry">Dry run (show prompts, send nothing)</label>
    <label>Max blocks<input type="number" id="ai_max" value="20" min="1" max="100"></label>
    <label>Provider<select id="ai_provider"><option value="">auto</option><option>anthropic</option><option>openai</option><option>gemini</option><option>mistral</option><option>groq</option><option>together</option><option>openrouter</option><option>ollama</option><option>custom</option></select></label>
    <label>Model<input type="text" id="ai_model" placeholder="default"></label>
    <label>API key<input type="password" id="ai_key" placeholder="your provider's key (kept in memory only)" autocomplete="off"></label>
    <div class="priv">With this on, the code around each TODO is sent to the provider you pick, using your key. Nothing is sent otherwise. Suggestions are unverified; they never reduce the TODO count.</div>
   </div></details>
</div>

<section id="secC">
 <div class="opts" style="margin-top:10px;border-style:dashed">
  <label>Example<select id="example"><option value="">load…</option></select></label>
  <label>File<input type="file" id="fileC" style="max-width:220px"></label>
  <button class="sec" id="link">Copy link</button>
  <button class="go" id="go">Convert</button>
 </div>
 <div class="banner" id="strictB"></div>
 <div class="panes">
  <div class="pane" id="inPane"><div class="ph"><b>Input</b><span id="det" class="badge" style="display:none"></span><span class="sp"><button class="sec" id="clr">Clear</button></span></div>
   <textarea id="src" spellcheck="false" placeholder="Paste pandas / SQL / SAS / R / PL-SQL / PowerCenter XML … or drop a file here (Ctrl+Enter converts)"></textarea></div>
  <div class="pane"><div class="ph"><b>Spark output</b><span id="todoNav" style="display:none"><span id="todoCnt" class="badge"></span> <button class="sec" id="prevT">▲</button><button class="sec" id="nextT">▼</button></span>
   <span class="sp"><button class="sec" id="cp">Copy</button><button class="sec" id="dl">Download</button></span></div>
   <div class="code" id="out"></div></div>
 </div>
 <div class="rep" id="rep"></div>
 <div class="rep" id="prompts"></div>
 <details class="opts" id="agentBox" style="margin-top:10px;display:block"><summary>Assistant — ask a language model to explain or fix the converted code</summary>
  <div class="priv">Uses the API key above. The input and the converted code are sent to the selected model provider (credentials are masked first). The assistant edits a draft only: it cannot read your files or run anything, and nothing replaces your output until you click "Use revised code".</div>
  <textarea id="agMsg" rows="2" style="width:100%" placeholder="e.g. Resolve the TODOs in this output · Why is line 14 a TODO? · Rewrite the UDF with built-in functions"></textarea>
  <div class="row"><button class="go" id="agGo">Ask the assistant</button><button class="sec" id="agUse" disabled>Use revised code</button><span id="agInfo" class="badge" style="display:none"></span></div>
  <div id="agOut" class="rep" style="display:none;white-space:pre-wrap"></div>
 </details>
</section>

<section id="secP" style="display:none">
 <div class="opts" style="margin-top:10px;border-style:dashed">
  <label>Files<input type="file" id="fFiles" multiple style="max-width:200px"></label>
  <label>Folder<input type="file" id="fDir" webkitdirectory directory multiple style="max-width:200px"></label>
  <label>Zip<input type="file" id="fZip" accept=".zip" style="max-width:200px"></label>
  <span id="qinfo" class="badge">nothing queued</span>
  <button class="sec" id="qclr">Clear</button>
  <button class="go" id="pgo" disabled>Convert project</button>
 </div>
 <div class="dz" id="dz">Drop files, a folder or a .zip here — uses the options above. Control-flow files (Airflow, crontab, workflows, SSIS) are skipped: use the Orchestrate tab.</div>
 <div class="panes" id="pres" style="display:none">
  <div class="pane"><div class="ph"><b>Files</b><span class="sp"><button class="sec" id="pzip">Download zip</button><button class="sec" id="prep">Report (.md)</button></span></div>
   <div class="stats" id="pstats"></div><div class="tblwrap"><table id="ptbl"></table></div></div>
  <div class="pane"><div class="ph"><b id="pname">Preview</b><span class="sp"><button class="sec" id="pcopy">Copy</button></span></div><div class="code" id="pview"></div></div>
 </div>
 <div class="rep" id="prepbox"></div>
</section>

<section id="secO" style="display:none">
 <div class="opts" style="margin-top:10px">
  <label>Input<select id="ofrom"><option>auto</option><option>airflow</option><option>crontab</option><option>pc-workflow</option><option>ssis</option><option>pentaho-job</option><option>talend</option><option>alteryx</option></select></label>
  <label>Output<select id="oto"><option value="databricks">Databricks job (JSON)</option><option value="airflow">Airflow DAG on Databricks</option></select></label>
  <label>Code root<input type="text" id="oroot" value="/Workspace/Shared/sparqix"></label>
  <label>Mapping module<input type="text" id="omod" value="mappings"></label>
  <label>File<input type="file" id="fileO" style="max-width:200px"></label>
  <label>Example<select id="oexample"><option value="">load…</option></select></label>
  <button class="go" id="ogo">Convert</button>
 </div>
 <div class="panes">
  <div class="pane"><div class="ph"><b>Airflow DAG / crontab / PowerCenter workflow / SSIS package</b></div><textarea id="osrc" spellcheck="false"></textarea></div>
  <div class="pane"><div class="ph"><b>Generated files</b><span class="sp"><button class="sec" id="ozip" disabled>Download all (.zip)</button></span></div><div class="files" id="oout"></div></div>
 </div>
 <div class="rep" id="orep"></div>
</section>
</main>
<footer>Runs locally: the server only parses and rewrites your text, it never executes it. Output is a starting point — read the TODOs and warnings and run <code>sparqix verify</code> on sample data before trusting results. Settings are kept in this browser (never the API key or your code).</footer>
<script src="/ui.js"></script>
<script>
const LANGS=__LANGS__,DIALECTS=__DIALECTS__,U=window.SparkUI;
const TOKEN=new URLSearchParams(location.search).get('token')||'';
if(TOKEN){try{history.replaceState(null,'',location.pathname+location.hash)}catch(e){}}
function api(u,o){o=o||{};o.headers=Object.assign({'X-Sparqix-Token':TOKEN},o.headers||{});return fetch(u,o)}
const $=id=>document.getElementById(id);
LANGS.forEach(l=>$('lang').add(new Option(l,l)));DIALECTS.forEach(d=>$('dialect').add(new Option(d,d)));
const OPT_IDS=['lang','dialect','sql_out','target','mode','optimize','delta','catalog','schema','broadcast','excel_columns','model','strict','ai_on','ai_apply','ai_dry','ai_max','ai_model','ai_provider'];
function getOpt(id){const e=$(id);return e.type==='checkbox'?e.checked:e.value}
function setOpt(id,v){const e=$(id);if(!e||v===undefined)return;if(e.type==='checkbox')e.checked=!!v;else e.value=v}
function opts(){const o={};OPT_IDS.forEach(i=>o[i]=getOpt(i));return o}
function saveOpts(){try{localStorage.setItem('sparqix.opts',JSON.stringify(opts()))}catch(e){}}
function loadOpts(){try{const o=JSON.parse(localStorage.getItem('sparqix.opts')||'{}');OPT_IDS.forEach(i=>setOpt(i,o[i]))}catch(e){}}
function apiBody(extra){const o=opts();return Object.assign({language:o.lang,dialect:o.dialect,sql_out:o.sql_out,target:o.target,mode:o.mode,optimize:o.optimize,delta:o.delta,
  catalog:o.catalog,schema:o.schema,broadcast:o.broadcast,excel_columns:o.excel_columns,model:o.model,
  ai:{enabled:o.ai_on,apply:o.ai_apply,dry_run:o.ai_dry,max:+o.ai_max||20,model:o.ai_model,provider:o.ai_provider,api_key:$('ai_key').value}},extra||{})}
OPT_IDS.forEach(i=>$(i).addEventListener('change',saveOpts));
function codeLang(detected,code){return /^(scala|java)$/.test(detected)?'jvm':(detected==='sql'&&$('sql_out').value==='sql')?'sql':'py'}
function dl(name,data,type){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([data],{type:type||'text/plain'}));a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),2000)}
async function post(url,body){const r=await api(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  let j;try{j=await r.json()}catch(e){throw new Error(r.statusText)}if(!r.ok)throw new Error(j.error||r.statusText);return j}
/* ---- code view with line numbers, highlighting, TODO navigation */
function renderCode(el,code,lang){
  const lines=U.highlight(code,lang);
  el.innerHTML=lines.map(l=>'<div class="ln'+(l.todo?' todo':l.ai?' ai':'')+'" id="'+el.id+'-L'+l.n+'"><span class="no">'+l.n+'</span><span class="tx">'+l.html+'</span></div>').join('');
  return lines;
}
let outLines=[],todoIdx=[],cur=-1,last='',ext='.py';
function gotoLine(el,i){const e=document.getElementById(el.id+'-L'+(i+1));if(!e)return;e.scrollIntoView({block:'center',behavior:'smooth'});e.classList.remove('flash');void e.offsetWidth;e.classList.add('flash')}
function setupNav(){todoIdx=outLines.map((l,i)=>l.todo?i:-1).filter(i=>i>=0);cur=-1;
  $('todoNav').style.display=todoIdx.length?'inline':'none';$('todoCnt').textContent=todoIdx.length+' TODO line(s)'}
function step(d){if(!todoIdx.length)return;cur=(cur+d+todoIdx.length)%todoIdx.length;gotoLine($('out'),todoIdx[cur]);$('todoCnt').textContent=(cur+1)+' / '+todoIdx.length}
$('nextT').onclick=()=>step(1);$('prevT').onclick=()=>step(-1);
function report(el,j,lines,viewEl){
  const parts=[];
  if(j.todos.length)parts.push('<h3 class="t">TODO — needs manual work ('+j.todos.length+')'+(lines?' <small style="font-weight:400;color:var(--mut)">click to jump</small>':'')+'</h3><ul>'+j.todos.map((t,i)=>'<li class="t lk" data-i="'+i+'"><b>'+U.esc(t[0])+'</b> '+U.esc(t[1])+'</li>').join('')+'</ul>');
  if(j.warnings.length)parts.push('<h3 class="w">Warnings ('+j.warnings.length+')</h3><ul>'+j.warnings.map(t=>'<li class="w"><b>'+U.esc(t[0])+'</b> '+U.esc(t[1])+'</li>').join('')+'</ul>');
  if(j.notes.length)parts.push('<h3 class="n">Notes ('+j.notes.length+')</h3><ul>'+j.notes.map(t=>'<li class="n">'+U.esc(t)+'</li>').join('')+'</ul>');
  if(!parts.length)parts.push('<h3 class="ok">No TODOs or warnings</h3>');
  el.innerHTML=parts.join('');el.style.display='block';
  if(lines)el.querySelectorAll('li.lk').forEach(li=>li.onclick=()=>{const k=+li.dataset.i;let i=U.findTodoLine(lines,j.todos[k][1]);if(i<0&&todoIdx.length)i=todoIdx[Math.min(k,todoIdx.length-1)];if(i>=0)gotoLine(viewEl,i)});
}
async function convert(){
  const b=$('go');b.disabled=true;b.textContent='Converting…';
  try{
    const j=await post('/api/convert',apiBody({source:$('src').value}));
    last=j.code;const lang=codeLang(j.detected);
    outLines=renderCode($('out'),j.code||'(no output — see the report)',lang);setupNav();
    const d=$('det');d.style.display='inline-block';d.textContent='detected: '+j.detected;
    ext=lang==='jvm'?(j.detected==='java'?'.java':'.scala'):lang==='sql'?'.sql':j.detected==='notebook'?'.ipynb':'.py';
    report($('rep'),j,outLines,$('out'));
    const sb=$('strictB');if($('strict').checked&&j.todos.length){sb.style.display='block';sb.textContent='Strict mode: '+j.todos.length+' TODO(s) remain — CI would fail on this conversion.'}else sb.style.display='none';
    const pr=$('prompts');if(j.prompts&&j.prompts.length){pr.innerHTML='<h3 class="n">Dry run: '+j.prompts.length+' prompt(s) that would be sent (nothing was sent)</h3>'+j.prompts.map(p=>'<div class="files" style="padding:4px 0"><b>block at output line '+p.line+'</b><pre>'+U.esc(p.prompt)+'</pre></div>').join('');pr.style.display='block'}else pr.style.display='none';
  }catch(e){$('out').innerHTML='<div class="err">'+U.esc(e.message)+'</div>';$('rep').style.display='none';outLines=[];setupNav()}
  b.disabled=false;b.textContent='Convert';
}
$('go').onclick=convert;
/* ---- assistant */
let agHist=[],agCode=null;
$('agGo').onclick=async()=>{
  const m=$('agMsg').value.trim();if(!m)return;
  const b=$('agGo');b.disabled=true;b.textContent='Working…';const o=$('agOut');o.style.display='block';
  try{
    const j=await post('/api/agent',apiBody({source:$('src').value,message:m,history:agHist,code:last}));
    agHist.push({role:'user',text:m},{role:'assistant',text:j.reply});agHist=agHist.slice(-10);
    o.textContent=j.reply;agCode=j.edits>0?j.code:null;$('agUse').disabled=!agCode;
    const i=$('agInfo');i.style.display='inline-block';i.textContent=j.steps.length+' tool call(s), '+j.edits+' edit(s), '+j.todo_markers+' TODO marker(s) left'+(j.stopped!=='done'?' · stopped: '+j.stopped:'');
  }catch(e){o.textContent='Assistant error: '+e.message}
  b.disabled=false;b.textContent='Ask the assistant';
};
$('agUse').onclick=()=>{if(!agCode)return;last=agCode;outLines=renderCode($('out'),last,codeLang($('det').textContent.replace('detected: ','')));setupNav();$('agUse').disabled=true;agCode=null};
$('src').addEventListener('keydown',e=>{if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)){e.preventDefault();convert()}});
$('clr').onclick=()=>{$('src').value='';$('out').innerHTML='';$('rep').style.display='none';$('det').style.display='none';last='';outLines=[];setupNav()};
$('cp').onclick=async()=>{try{await navigator.clipboard.writeText(last);$('cp').textContent='Copied';setTimeout(()=>$('cp').textContent='Copy',1200)}catch(e){}};
$('dl').onclick=()=>{if(last)dl('converted'+ext,last)};
/* ---- link / state */
function packState(){const o=opts();const s={o:o,src:$('src').value};let enc=U.b64urlEncode(JSON.stringify(s));
  if(enc.length>8000){enc=U.b64urlEncode(JSON.stringify({o:o}));alert('The input is too long for a link: only the options are included.')}return location.origin+location.pathname+'#s='+enc}
$('link').onclick=async()=>{const u=packState();try{await navigator.clipboard.writeText(u);$('link').textContent='Link copied';setTimeout(()=>$('link').textContent='Copy link',1400)}catch(e){prompt('Copy this link',u)}};
function loadHash(){const m=/^#s=(.+)$/.exec(location.hash);if(!m)return false;try{const s=JSON.parse(U.b64urlDecode(m[1]));if(s.o)OPT_IDS.forEach(i=>setOpt(i,s.o[i]));if(s.src){$('src').value=s.src;return true}}catch(e){}return false}
/* ---- files */
async function readText(f){if(f.size>2000000)throw new Error(f.name+' is larger than 2 MB (use the Project tab)');return await f.text()}
$('fileC').onchange=async e=>{const f=e.target.files[0];if(!f)return;if(/\.zip$/i.test(f.name)){qAdd([f]);showTab('P');return}
  try{$('src').value=await readText(f);$('lang').value='auto';convert()}catch(er){$('out').innerHTML='<div class="err">'+U.esc(er.message)+'</div>'}};
['dragenter','dragover'].forEach(ev=>$('inPane').addEventListener(ev,e=>{e.preventDefault();$('inPane').classList.add('drag')}));
['dragleave','drop'].forEach(ev=>$('inPane').addEventListener(ev,e=>{e.preventDefault();$('inPane').classList.remove('drag')}));
$('inPane').addEventListener('drop',async e=>{const fs=[...e.dataTransfer.files];if(!fs.length)return;
  if(fs.length>1||/\.zip$/i.test(fs[0].name)){await qDrop(e.dataTransfer);showTab('P');return}
  try{$('src').value=await readText(fs[0]);$('lang').value='auto';convert()}catch(er){$('out').innerHTML='<div class="err">'+U.esc(er.message)+'</div>'}});
/* ---- project */
let queue=[],zipFile=null,pres=null,sel=null;
function qInfo(){const n=queue.length;$('qinfo').textContent=zipFile?('zip: '+zipFile.name):n?(n+' file(s) queued'):'nothing queued';$('pgo').disabled=!(zipFile||n)}
async function qAdd(fs){for(const f of fs){if(/\.zip$/i.test(f.name)){zipFile=f;queue=[];continue}
  const p=f.webkitRelativePath||f.name;if(f.size>2000000||/\.(png|jpe?g|gif|pdf|xlsx?|docx?|zip|gz|jar|class|exe|dll|so|bin|parquet)$/i.test(p))continue;
  try{queue.push({path:p,content:await f.text()});zipFile=null}catch(e){}}qInfo()}
async function walk(entry,path){if(entry.isFile)return new Promise(res=>entry.file(async f=>{try{if(f.size<=2000000&&!/\.(png|jpe?g|gif|pdf|zip|gz|jar|class|exe|dll|bin|parquet)$/i.test(f.name))queue.push({path:path+f.name,content:await f.text()})}catch(e){}res()}));
  const rd=entry.createReader();const all=[];await new Promise(res=>{const more=()=>rd.readEntries(es=>{if(!es.length)return res();all.push(...es);more()});more()});for(const e of all)await walk(e,path+entry.name+'/')}
async function qDrop(dt){const items=[...(dt.items||[])].map(i=>i.webkitGetAsEntry&&i.webkitGetAsEntry()).filter(Boolean);
  if(items.length&&!(items.length===1&&items[0].isFile&&/\.zip$/i.test(items[0].name))){zipFile=null;for(const en of items)await walk(en,'');qInfo();return}
  await qAdd([...dt.files])}
$('fFiles').onchange=e=>qAdd([...e.target.files]);$('fDir').onchange=e=>qAdd([...e.target.files]);$('fZip').onchange=e=>qAdd([...e.target.files]);
$('qclr').onclick=()=>{queue=[];zipFile=null;qInfo()};
['dragenter','dragover'].forEach(ev=>$('dz').addEventListener(ev,e=>{e.preventDefault();$('dz').classList.add('drag')}));
['dragleave','drop'].forEach(ev=>$('dz').addEventListener(ev,e=>{e.preventDefault();$('dz').classList.remove('drag')}));
$('dz').addEventListener('drop',e=>qDrop(e.dataTransfer));
$('pgo').onclick=async()=>{
  const b=$('pgo');b.disabled=true;b.textContent='Converting…';
  try{
    const body=apiBody({});delete body.ai;
    if(zipFile)body.zip_b64=U.bytesToB64(new Uint8Array(await zipFile.arrayBuffer()));else body.files=queue;
    pres=await post('/api/project',body);
    const s=pres.stats;$('pstats').innerHTML=[['converted',s.converted],['ok',s.ok],['warnings',s.check],['need review',s.needs_review],['failed',s.failed],['TODOs',s.todos]].map(x=>'<span class="badge">'+x[0]+': <b>'+x[1]+'</b></span>').join('');
    const order={'FAILED':0,'NEEDS REVIEW':1,'CHECK':2,'OK':3};
    const rows=[...pres.files].sort((a,b)=>order[a.status]-order[b.status]||a.rel.localeCompare(b.rel));
    $('ptbl').innerHTML='<tr><th>File</th><th>Type</th><th>Status</th><th>TODO</th><th>Warn</th></tr>'+rows.map((r,i)=>'<tr class="row" data-i="'+i+'"><td>'+U.esc(r.rel)+'</td><td>'+U.esc(r.lang)+'</td><td class="st-'+r.status.split(' ')[0]+'">'+r.status+'</td><td>'+r.todos.length+'</td><td>'+r.warnings.length+'</td></tr>').join('');
    pres._rows=rows;$('pres').style.display='grid';
    $('ptbl').querySelectorAll('tr.row').forEach(tr=>tr.onclick=()=>showFile(+tr.dataset.i));
    const bad=rows.findIndex(r=>r.status!=='OK');showFile(bad>=0?bad:0);
    const extra=[];if(pres.problems.length)extra.push('<h3 class="w">Upload problems</h3><ul>'+pres.problems.map(p=>'<li class="w">'+U.esc(p)+'</li>').join('')+'</ul>');
    if(pres.skipped.length)extra.push('<h3 class="n">Skipped ('+pres.skipped.length+')</h3><ul>'+pres.skipped.slice(0,40).map(p=>'<li class="n">'+U.esc(p[0])+': '+U.esc(p[1])+'</li>').join('')+'</ul>');
    $('prepbox').innerHTML=extra.join('');$('prepbox').style.display=extra.length?'block':'none';
  }catch(e){$('pstats').innerHTML='';$('pres').style.display='grid';$('ptbl').innerHTML='<tr><td class="err">'+U.esc(e.message)+'</td></tr>';$('pview').innerHTML=''}
  b.disabled=false;b.textContent='Convert project';qInfo();
};
function showFile(i){const r=pres._rows[i];sel=r;document.querySelectorAll('#ptbl tr.row').forEach((tr,k)=>tr.classList.toggle('sel',k===i));
  $('pname').textContent=(r.out||r.rel);const code=pres.previews[r.out];
  const lang=/scala|java/.test(r.lang)?'jvm':/\.sql$/.test(r.out||'')?'sql':'py';
  if(code===undefined){$('pview').innerHTML='<div class="err">'+(r.out?'Preview not included (size budget): download the zip.':'No output for this file.')+'</div>';return}
  const lines=renderCode($('pview'),code,lang);
  const tl=r.todos.map(t=>'TODO '+t[0]+': '+t[1]).concat(r.warnings.map(w=>'warn '+w[0]+': '+w[1]));
  if(tl.length){const first=lines.findIndex(l=>l.todo);if(first>=0)setTimeout(()=>gotoLine($('pview'),first),50)}}
$('pcopy').onclick=async()=>{if(sel&&pres.previews[sel.out])try{await navigator.clipboard.writeText(pres.previews[sel.out])}catch(e){}};
$('pzip').onclick=()=>{if(!pres)return;const bin=atob(pres.zip_b64),b=new Uint8Array(bin.length);for(let i=0;i<bin.length;i++)b[i]=bin.charCodeAt(i);dl('sparqix-output.zip',b,'application/zip')};
$('prep').onclick=()=>{if(pres)dl('SPARQIX_REPORT.md',pres.report_md)};
/* ---- orchestrate */
let ofiles={};
$('ogo').onclick=async()=>{
  const b=$('ogo');b.disabled=true;
  try{
    const j=await post('/api/orchestrate',{source:$('osrc').value,from:$('ofrom').value,to:$('oto').value,code_root:$('oroot').value,mapping_module:$('omod').value});
    ofiles=j.files;const names=Object.keys(ofiles);
    $('oout').innerHTML=names.map((n,i)=>'<div><div class="ph" style="border:0;padding:0 0 4px"><b>'+U.esc(n)+'</b><span class="sp"><button class="sec" data-i="'+i+'">Download</button></span></div><pre>'+U.esc(ofiles[n])+'</pre></div>').join('')||'<div class="err">no files</div>';
    $('oout').querySelectorAll('button[data-i]').forEach(bt=>bt.onclick=()=>dl(names[+bt.dataset.i],ofiles[names[+bt.dataset.i]]));
    $('ozip').disabled=!names.length;report($('orep'),j);
  }catch(e){$('oout').innerHTML='<div class="err">'+U.esc(e.message)+'</div>';$('orep').style.display='none';$('ozip').disabled=true}
  b.disabled=false;
};
$('ozip').onclick=()=>{const n=Object.keys(ofiles);if(n.length)dl('sparqix-jobs.zip',U.makeZip(n.map(k=>({name:k,data:ofiles[k]}))),'application/zip')};
$('fileO').onchange=async e=>{const f=e.target.files[0];if(!f)return;try{$('osrc').value=await readText(f);$('ogo').click()}catch(er){$('oout').innerHTML='<div class="err">'+U.esc(er.message)+'</div>'}};
api('/api/examples?kind=orch').then(r=>r.json()).then(l=>l.forEach(n=>$('oexample').add(new Option(n,n))));
$('oexample').onchange=async e=>{if(!e.target.value)return;const j=await (await api('/api/example?name='+encodeURIComponent(e.target.value))).json();$('osrc').value=j.source;$('ogo').click()};
/* ---- examples, tabs, theme */
api('/api/examples').then(r=>r.json()).then(l=>{l.forEach(n=>$('example').add(new Option(n,n)));
  const q=new URLSearchParams(location.search).get('example');if(q&&l.includes(q)){$('example').value=q;$('example').onchange({target:{value:q}})}});
$('example').onchange=async e=>{if(!e.target.value)return;const j=await (await api('/api/example?name='+encodeURIComponent(e.target.value))).json();
  $('src').value=j.source;$('lang').value='auto';if(/\.sql$/.test(j.name)&&/tsql|report/.test(j.name))$('dialect').value='tsql';convert()};
function showTab(t){['C','P','O'].forEach(k=>{$('sec'+k).style.display=k===t?'':'none';$('tab'+k).className=k===t?'on':''});$('optbar').style.display=t==='O'?'none':''}
$('tabC').onclick=()=>showTab('C');$('tabP').onclick=()=>showTab('P');$('tabO').onclick=()=>showTab('O');
$('theme').onclick=()=>{const r=document.documentElement,cur=r.dataset.theme||(matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light');r.dataset.theme=cur==='dark'?'light':'dark';try{localStorage.setItem('sparqix.theme',r.dataset.theme)}catch(e){}};
try{const t=localStorage.getItem('sparqix.theme');if(t)document.documentElement.dataset.theme=t}catch(e){}
loadOpts();if(loadHash())convert();
const tabQ=new URLSearchParams(location.search).get('tab');if(tabQ)showTab(tabQ.toUpperCase().slice(0,1));
qInfo();
</script></body></html>
"""
