"""Run a generated script in a throw-away container: no network, read-only file system, no capabilities, small memory/process limits.
Only used when the user passes --sandbox-image. The script is mounted read-only; sample data (optional) is mounted read-only at /data.
Not tested against a real Docker daemon (none available when this was written): the command line is what the tests check."""
from __future__ import annotations

import os
import re
import subprocess
import tempfile

from .ai import redact

_IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/@\-]{0,200}")
TIMEOUT = 300


def docker_args(image: str, script_dir: str, data_dir: str | None = None) -> list:
    if not _IMAGE.fullmatch(image or "") or image.startswith("-"):
        raise ValueError("bad image name")
    args = ["docker", "run", "--rm", "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,size=256m", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--memory", "2g", "--pids-limit", "256", "--user", "10001:10001",
            "-v", f"{script_dir}:/work:ro"]
    if data_dir:
        args += ["-v", f"{os.path.realpath(data_dir)}:/data:ro", "-w", "/data"]
    return args + [image, "python", "/work/script.py"]


def run_in_sandbox(code: str, data_dir: str | None, image: str, *, runner=None, timeout: int = TIMEOUT) -> str:
    run = runner or (lambda argv, t: subprocess.run(argv, capture_output=True, text=True, timeout=t, shell=False))
    with tempfile.TemporaryDirectory() as d:
        os.chmod(d, 0o755)
        path = os.path.join(d, "script.py")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(code)
        os.chmod(path, 0o644)
        try:
            r = run(docker_args(image, d, data_dir), timeout)
        except subprocess.TimeoutExpired:
            return f"sandbox: stopped after {timeout}s"
        except (OSError, ValueError) as e:
            return f"sandbox: could not run ({type(e).__name__}: {str(e)[:200]})"
    out = ((r.stdout or "") + (r.stderr or ""))[-3000:]
    return redact(f"sandbox exit code {r.returncode}\n{out}")
