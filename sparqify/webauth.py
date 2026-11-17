"""Per-user access for the web UI (stdlib only).

Three ways to tell users apart, all optional and usable together:
  * a users file: one line per user, `name:sha256:<hex digest of that user's token>` (the file holds no usable secrets)
  * the single shared token (`--token`), reported as the user `shared`
  * a user name sent by a trusted reverse proxy that already signed the user in (oauth2-proxy, an OIDC ingress, ...): `--auth-header`, which is only honoured
    together with a proxy secret the proxy adds to every request, so a client that reaches the pod directly cannot claim to be somebody
The identity goes into the access log, and drives per-user rate limits and a daily cap on AI calls.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import threading
import time
from collections import deque

NAME_RE = re.compile(r"^[A-Za-z0-9._@+-]{1,64}$")            # users file names
HEADER_NAME_RE = re.compile(r"^[A-Za-z0-9._@+-]{1,128}$")    # names taken from a proxy header (an e-mail address is the usual value)
SHARED = "shared"


class UsersFileError(ValueError):
    pass


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_user_line(name: str):
    """(token, users-file line) for a new user: the token is shown once, the line (which only has its hash) goes into the users file"""
    if not NAME_RE.match(name or ""):
        raise UsersFileError("a user name is 1 to 64 characters from A-Z a-z 0-9 . _ @ + -")
    token = secrets.token_urlsafe(24)
    return token, f"{name}:sha256:{hash_token(token)}"


def parse_users(text: str) -> dict:
    """{name: sha256 hex digest}. Raises UsersFileError naming the line numbers (never the line contents: they may be near-secrets)."""
    users, bad = {}, []
    for i, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) != 3 or parts[1] != "sha256" or not NAME_RE.match(parts[0]) or not re.fullmatch(r"[0-9a-fA-F]{64}", parts[2]) \
                or parts[0].lower() == SHARED or parts[0] in users:
            bad.append(i)
            continue
        users[parts[0]] = parts[2].lower()
    if bad:
        raise UsersFileError("users file: invalid or duplicate entries on line(s) " + ", ".join(map(str, bad[:10]))
                             + " (expected `name:sha256:<64 hex digits>`; create lines with `sparqify users add NAME`)")
    return users


class UserStore:
    """the users file, re-read when its modification time changes (a Kubernetes Secret mounted as a file is updated in place)"""

    def __init__(self, path: str | None):
        self.path, self._mtime, self._users, self._lock = path, None, {}, threading.Lock()
        if path:
            self._load(strict=True)

    def _load(self, strict: bool):
        try:
            st = os.stat(self.path)
            with open(self.path, encoding="utf-8") as fh:
                users = parse_users(fh.read())
        except (OSError, UsersFileError):
            if strict:
                raise
            return                                              # a half-written file while it is replaced: keep the previous users
        with self._lock:
            self._users, self._mtime = users, st.st_mtime_ns

    def users(self) -> dict:
        if self.path:
            try:
                if os.stat(self.path).st_mtime_ns != self._mtime:
                    self._load(strict=False)
            except OSError:
                pass
        with self._lock:
            return self._users

    def find(self, token: str):
        """name of the user owning `token`, or None. Compares digests in constant time against every entry."""
        if not token:
            return None
        digest = hash_token(token)
        found = None
        for name, h in self.users().items():
            if hmac.compare_digest(digest, h):
                found = name
        return found

    def __bool__(self):
        return bool(self.path)


class SlidingLimit:
    """at most `limit` events per `window` seconds per key; limit 0 switches it off. Memory is bounded."""

    def __init__(self, limit: int, window: float = 60.0):
        self.limit, self.window, self._hits, self._lock = limit, window, {}, threading.Lock()

    def hit(self, key: str) -> bool:
        """record an event; False when `key` is over the limit (the event is then not counted)"""
        if self.limit <= 0:
            return True
        now = time.monotonic()
        with self._lock:
            if len(self._hits) > 4096:
                self._hits.clear()
            q = self._hits.setdefault(key, deque())
            while q and now - q[0] >= self.window:
                q.popleft()
            if len(q) >= self.limit:
                return False
            q.append(now)
            return True


class DailyCap:
    """at most `limit` events per user per calendar day (UTC); limit 0 switches it off"""

    def __init__(self, limit: int):
        self.limit, self._day, self._n, self._lock = limit, None, {}, threading.Lock()

    def hit(self, key: str) -> bool:
        if self.limit <= 0:
            return True
        day = time.strftime("%Y-%m-%d", time.gmtime())
        with self._lock:
            if day != self._day:
                self._day, self._n = day, {}
            if self._n.get(key, 0) >= self.limit:
                return False
            self._n[key] = self._n.get(key, 0) + 1
            return True


def users_main(argv) -> int:
    """`sparqify users add NAME [--file PATH]`: make a token for a new user, print it once, and add (or print) the users-file line"""
    import argparse
    import sys
    p = argparse.ArgumentParser(prog="sparqify users")
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("add", help="create a user and a token")
    a.add_argument("name")
    a.add_argument("--file", help="append the user's line to this users file (created with owner-only permissions); without it the line is only printed")
    args = p.parse_args(argv)
    try:
        if args.file and os.path.exists(args.file):
            with open(args.file, encoding="utf-8") as fh:
                if args.name in parse_users(fh.read()):
                    print(f"user {args.name!r} already exists in {args.file}", file=sys.stderr)
                    return 2
        token, line = new_user_line(args.name)
    except (UsersFileError, OSError) as e:
        print(str(e), file=sys.stderr)
        return 2
    if args.file:
        fd = os.open(args.file, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        print(f"added {args.name} to {args.file}")
    else:
        print("add this line to your users file:\n  " + line)
    print(f"\ntoken for {args.name} (shown once, not stored anywhere in clear):\n  {token}\nopen the UI with  ?token=<token>")
    return 0
