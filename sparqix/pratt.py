"""Tiny shared Pratt parser used by the Excel and SAS front-ends.

Leaves: number, string, identifier / cell reference, function call f(a, b), parenthesised group.
Nodes are tuples: ('num', s) ('str', s) ('id', s) ('call', name, [args]) ('bin', op, l, r) ('un', op, x)
('paren', x) ('list', [items])
"""
from __future__ import annotations

import re

from .core import Unsupported


def tokenize(src, token_re, skip=("ws",)):
    toks = []
    for m in token_re.finditer(src):
        if m.lastgroup in skip:
            continue
        toks.append((m.lastgroup, m.group()))
    return toks


class Pratt:
    def __init__(self, toks, binops, unary, name="expression"):
        self.t, self.i, self.bp, self.un, self.name = toks, 0, binops, unary, name

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else ("eof", "")

    def next(self):
        t = self.peek()
        self.i += 1
        return t

    def expect(self, v):
        t = self.next()
        if t[1] != v:
            raise Unsupported(f"{self.name} parse error: expected {v!r}, got {t[1]!r}")

    def done(self):
        return self.i >= len(self.t)

    def expr(self, rbp=0):
        left = self.prefix()
        while True:
            k, v = self.peek()
            key = v.lower() if k in ("op", "word") else None
            if key not in self.bp or self.bp[key] <= rbp:
                break
            self.next()
            if key == "in":
                self.expect("(")
                items = []
                while self.peek()[1] != ")":
                    items.append(self.expr())
                    if self.peek()[1] == ",":
                        self.next()
                self.expect(")")
                left = ("bin", "in", left, ("list", items))
                continue
            right = self.expr(self.bp[key] - (1 if key in ("^", "**") else 0))
            left = ("bin", key, left, right)
        return left

    def prefix(self):
        k, v = self.next()
        if k == "num":
            return ("num", v)
        if k == "str":
            return ("str", v)
        if k in ("op", "word") and v.lower() in self.un:
            return ("un", v.lower(), self.expr(self.un[v.lower()]))
        if k == "op" and v == "(":
            e = self.expr()
            self.expect(")")
            return ("paren", e)
        if k in ("id", "ref", "word"):
            if self.peek()[1] == "(":
                self.next()
                args = []
                while self.peek()[1] != ")":
                    if self.peek()[1] == ",":      # empty arg
                        self.next()
                        continue
                    args.append(self.expr())
                    if self.peek()[1] == ",":
                        self.next()
                self.expect(")")
                return ("call", v, args)
            return ("ref" if k == "ref" else "id", v)
        raise Unsupported(f"{self.name} parse error near {v!r}")
