"""SAS macro language, expanded at conversion time (SAS itself expands macros as text before running the code):

    %macro name(a, b=default); ... &a ... %mend;     %name(1, b=2);       definitions and calls (also inline, e.g. where x = %name(1))
    %let v = value;  &v  &v.suffix                    macro variables (not resolved inside single quotes, like SAS)
    %if cond %then ...; %else ...;  %do; ... %end;    conditions are evaluated here: numbers compare as numbers, other text as text
    %do i = 1 %to 5 %by 1; ... %end;  %do %while(c)   loops are unrolled (bounded)
    %eval(1+2)  %str(x)  %upcase(x)  %lowcase(x)  %put (dropped)

What cannot be resolved statically (%sysfunc, &&indirection, CALL SYMPUT, conditions on run-time data) is left in the text, so the SAS converter reports it
as a TODO instead of guessing. The result is only ever SAS statements that go through the normal converters: nothing here produces Python.
"""
from __future__ import annotations

import re

from .core import Unsupported
from .pratt import Pratt, tokenize

MAX_DEPTH = 20
MAX_CALLS = 2000
MAX_LOOP = 200
MAX_STATEMENTS = 50_000

_TOK = re.compile(r"""(?P<ws>\s+)|(?P<num>\d+\.?\d*)|(?P<str>"[^"]*"|'[^']*')|(?P<word>[A-Za-z_][\w.]*)
 |(?P<op>\*\*|<=|>=|\^=|~=|<>|[-+*/=<>(),!|&^~])""", re.X)
_BP = {"or": 10, "|": 10, "and": 12, "&": 12, "=": 20, "eq": 20, "ne": 20, "^=": 20, "~=": 20, "<>": 20, "<": 20, ">": 20,
       "<=": 20, ">=": 20, "lt": 20, "gt": 20, "le": 20, "ge": 20, "+": 30, "-": 30, "*": 40, "/": 40, "**": 60}
_UN = {"-": 50, "+": 50, "not": 15, "^": 15, "~": 15}
_CMP = {"=": "eq", "eq": "eq", "ne": "ne", "^=": "ne", "~=": "ne", "<>": "ne", "<": "lt", ">": "gt", "<=": "le", ">=": "ge",
        "lt": "lt", "gt": "gt", "le": "le", "ge": "ge"}
_NUM = re.compile(r"[-+]?\d+(?:\.\d*)?")
UNRESOLVED = object()


def _split_top(text: str, sep: str = ","):
    """split on `sep` outside parentheses and quotes"""
    out, cur, depth, q = [], [], 0, None
    for ch in text:
        if q:
            cur.append(ch)
            q = None if ch == q else q
        elif ch in "'\"":
            q = ch
            cur.append(ch)
        elif ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == sep and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return out


def _match_paren(text: str, i: int) -> int:
    """index of the ')' matching text[i] == '(' (quote aware), or -1"""
    depth, q = 0, None
    for j in range(i, len(text)):
        ch = text[j]
        if q:
            q = None if ch == q else q
        elif ch in "'\"":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return j
    return -1


class Macros:
    def __init__(self, report):
        self.report = report
        self.defs = {}                 # name -> (params [(name, default|None)], body statements)
        self.globals = {}
        self.calls = 0
        self.emitted = 0
        self.used = False

    # ---- definitions -------------------------------------------------------------------------------------------
    def collect(self, stmts):
        out, i = [], 0
        while i < len(stmts):
            m = re.match(r"(?is)^%macro\s+(\w+)\s*(?:\((.*)\))?\s*(?:/.*)?$", stmts[i])
            if not m:
                out.append(stmts[i])
                i += 1
                continue
            depth, j = 1, i + 1
            while j < len(stmts) and depth:
                low = stmts[j].lower()
                depth += bool(re.match(r"%macro\b", low))
                depth -= bool(re.match(r"%mend\b", low))
                j += 1
            if depth:
                raise Unsupported(f"%macro {m.group(1)} has no matching %mend")
            params = []
            for p in _split_top(m.group(2) or ""):
                p = p.strip()
                if not p:
                    continue
                n, _, d = p.partition("=")
                params.append((n.strip().lower(), d.strip() if "=" in p else None))
            self.defs[m.group(1).lower()] = (params, stmts[i + 1:j - 1])
            self.used = True
            i = j
        return out

    # ---- substitution ------------------------------------------------------------------------------------------
    def _get(self, name, scope):
        k = name.lower()
        if k in scope:
            return scope[k]
        return self.globals.get(k, None)

    def subst(self, text, scope, depth=0):
        """&var / &var. / inline functions in `text`, outside single-quoted strings"""
        if depth > 8:
            return text
        out, i, n = [], 0, len(text)
        while i < n:
            ch = text[i]
            if ch == "'":
                j = text.find("'", i + 1)
                j = n if j < 0 else j + 1
                out.append(text[i:j])
                i = j
                continue
            if ch == "&" and i + 1 < n and text[i + 1] == "&":
                out.append("&&")
                i += 2
                self.report.warn("sas", "&&indirect macro references are not resolved")
                continue
            m = re.compile(r"&([A-Za-z_]\w*)\.?").match(text, i) if ch == "&" else None
            if m:
                v = self._get(m.group(1), scope)
                if v is None or v is UNRESOLVED:
                    out.append(m.group(0))
                else:
                    out.append(self.subst(str(v), scope, depth + 1))
                i = m.end()
                continue
            if ch == "%":
                fm = re.compile(r"%([A-Za-z_]\w*)\s*\(").match(text, i)
                if fm:
                    close = _match_paren(text, fm.end() - 1)
                    if close > 0:
                        rep = self._inline(fm.group(1).lower(), text[fm.end():close], scope, depth)
                        if rep is not None:
                            out.append(rep)
                            i = close + 1
                            continue
            out.append(ch)
            i += 1
        return "".join(out)

    def _inline(self, name, arg, scope, depth):
        arg_s = self.subst(arg, scope, depth + 1)
        if name == "eval":
            try:
                v = self.evaluate(arg_s)
            except Unsupported:
                return None
            return str(int(v)) if isinstance(v, (int, float)) and not isinstance(v, bool) and float(v) == int(v) else str(v)
        if name in ("str", "nrstr", "quote", "nrquote", "bquote"):
            return arg_s
        if name == "upcase":
            return arg_s.upper()
        if name == "lowcase":
            return arg_s.lower()
        if name in self.defs:
            outs = self.call(name, arg, scope, depth + 1)
            if len(outs) != 1:
                raise Unsupported(f"%{name}() is used inside a statement but expands to {len(outs)} statements")
            return outs[0]
        return None            # %sysfunc, %scan, %substr ...: left in the text, reported by the converter

    # ---- conditions ---------------------------------------------------------------------------------------------
    def evaluate(self, text):
        text = text.strip()
        p = Pratt(tokenize(text, _TOK), _BP, _UN, "macro expression")
        node = p.expr()
        if not p.done():
            raise Unsupported(f"cannot evaluate macro expression `{text[:60]}`")
        return self._ev(node)

    def _ev(self, n):
        t = n[0]
        if t == "num":
            return float(n[1]) if "." in n[1] else int(n[1])
        if t == "str":
            return n[1][1:-1]
        if t == "id":
            return n[1]
        if t == "paren":
            return self._ev(n[1])
        if t == "un":
            v = self._ev(n[2])
            if n[1] == "-":
                return -self._num(v)
            if n[1] == "+":
                return self._num(v)
            return not self._truth(v)
        if t == "bin":
            op = n[1].lower()
            a, b = self._ev(n[2]), self._ev(n[3])
            if op in ("and", "&"):
                return self._truth(a) and self._truth(b)
            if op in ("or", "|"):
                return self._truth(a) or self._truth(b)
            if op in _CMP:
                if _NUM.fullmatch(str(a).strip()) and _NUM.fullmatch(str(b).strip()):
                    a, b = float(a), float(b)
                else:
                    a, b = str(a), str(b)
                return {"eq": a == b, "ne": a != b, "lt": a < b, "gt": a > b, "le": a <= b, "ge": a >= b}[_CMP[op]]
            x, y = self._num(a), self._num(b)
            if op == "+":
                return x + y
            if op == "-":
                return x - y
            if op == "*":
                return x * y
            if op == "/":
                if y == 0:
                    raise Unsupported("division by zero in a macro expression")
                return int(x / y)
            if op == "**":
                return x ** y
        raise Unsupported("unsupported macro expression")

    @staticmethod
    def _num(v):
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return v
        if _NUM.fullmatch(str(v).strip()):
            s = str(v).strip()
            return float(s) if "." in s else int(s)
        raise Unsupported(f"`{str(v)[:30]}` is not a number in a macro expression")

    @staticmethod
    def _truth(v):
        if isinstance(v, str):
            return bool(_NUM.fullmatch(v.strip())) and float(v) != 0
        return bool(v)

    # ---- running -------------------------------------------------------------------------------------------------
    def run(self, stmts, scope, depth=0):
        if depth > MAX_DEPTH:
            raise Unsupported("macro calls are nested too deeply (recursion?)")
        out, i = [], 0
        stmts = list(stmts)
        while i < len(stmts):
            s = stmts[i]
            if self.emitted + len(out) > MAX_STATEMENTS:
                raise Unsupported("macro expansion is too large")
            low = s.lower()
            if s.startswith("__SPQX_DATA_"):
                out.append(s)
                i += 1
                continue
            start = i
            try:
                if re.match(r"%let\b", low):
                    self._let(s, scope)
                    i += 1
                elif re.match(r"%(put|global|local|\*|symdel)", low):
                    i += 1
                elif re.match(r"%if\b", low):
                    i = self._if(stmts, i, scope, depth, out)
                elif re.match(r"%do\b", low):
                    i = self._do(stmts, i, scope, depth, out)
                elif re.match(r"%(end|else|mend|macro)\b", low):
                    raise Unsupported(f"stray {s.split()[0]}")
                elif re.match(r"%[A-Za-z_]\w*", low) and re.match(r"%([A-Za-z_]\w*)", s).group(1).lower() in self.defs:
                    m = re.match(r"(?s)%([A-Za-z_]\w*)\s*(.*)$", s)
                    args = m.group(2).strip()
                    if args.startswith("("):                       # a call may be written without ';': whatever follows the ')' is the next statement
                        close = _match_paren(args, 0)
                        if close < 0:
                            raise Unsupported("unbalanced parentheses in a macro call")
                        rest = args[close + 1:].strip()
                        args = args[1:close]
                        if rest:
                            stmts.insert(i + 1, rest)
                    out += self.call(m.group(1).lower(), args, scope, depth + 1)
                    i += 1
                else:
                    out.append(self.subst(s, scope, depth))
                    i += 1
            except Unsupported as e:
                self.report.warn("sas", f"macro construct left as is: {str(e)[:120]}")
                out.append(s)                       # reaches the converter, which reports it as a TODO with the original text
                i = max(i, start) + (0 if i > start else 1)
        return out

    def _let(self, s, scope):
        m = re.match(r"(?is)^%let\s+([A-Za-z_]\w*)\s*=\s*(.*)$", s)
        if not m:
            raise Unsupported("unsupported %LET")
        name, val = m.group(1).lower(), self.subst(m.group(2).strip(), scope)
        if re.search(r"%[A-Za-z_]\w*\s*\(", val) or "&" in val:
            self.report.warn("sas", f"%LET {name} depends on something only known at run time ({val[:40]}): left unresolved")
            val = UNRESOLVED
        target = scope if (scope is not self.globals and (name in scope or name not in self.globals)) else self.globals
        target[name] = val
        self.used = True

    def _block_end(self, stmts, i):
        depth, j = 1, i + 1
        while j < len(stmts):
            low = stmts[j].lower()
            if re.match(r"(%do\b|%if\b.*?%then\s*%do\b|%else\s+%do\b|%else\s+%if\b.*?%then\s*%do\b)", low, re.S):
                depth += 1
            elif re.match(r"%end\b", low):
                depth -= 1
                if depth == 0:
                    return j
            j += 1
        raise Unsupported("%DO without a matching %END")

    def _if(self, stmts, i, scope, depth, out):
        taken = False
        while True:
            t = stmts[i].strip()
            kind = "if"
            if re.match(r"(?i)%else\b", t):
                t = t[5:].strip()
                kind = "elseif" if re.match(r"(?i)%if\b", t) else "else"
            cond_true = True
            rest = t
            if kind != "else":
                m = re.match(r"(?is)^%if\s+(.*?)\s*%then\s*(.*)$", t)
                if not m:
                    raise Unsupported("unsupported %IF")
                v = self.evaluate(self.subst(m.group(1), scope))
                cond_true = v if isinstance(v, bool) else self._truth(v)
                rest = m.group(2).strip()
            if re.fullmatch(r"(?i)%do", rest):
                end = self._block_end(stmts, i)
                body, j = stmts[i + 1:end], end + 1
            else:
                if not rest:
                    raise Unsupported("%THEN without a statement")
                body, j = [rest], i + 1
            if not taken and cond_true:
                out += self.run(body, scope, depth)
                taken = True
            if j < len(stmts) and re.match(r"(?i)%else\b", stmts[j]):
                i = j
                continue
            return j

    def _do(self, stmts, i, scope, depth, out):
        s = stmts[i].strip()
        end = self._block_end(stmts, i)
        body = stmts[i + 1:end]
        nxt = end + 1
        if re.fullmatch(r"(?i)%do", s):
            out += self.run(body, scope, depth)
            return nxt
        m = re.match(r"(?is)^%do\s+([A-Za-z_]\w*)\s*=\s*(.*?)\s+%to\s+(.*?)(?:\s+%by\s+(.*))?$", s)
        if m:
            var = m.group(1).lower()
            lo = int(self._num(self.evaluate(self.subst(m.group(2), scope))))
            hi = int(self._num(self.evaluate(self.subst(m.group(3), scope))))
            by = int(self._num(self.evaluate(self.subst(m.group(4), scope)))) if m.group(4) else 1
            if by == 0 or (hi - lo) / by + 1 > MAX_LOOP:
                raise Unsupported(f"%DO loop with more than {MAX_LOOP} iterations is not unrolled")
            k = lo
            while (k <= hi) if by > 0 else (k >= hi):
                inner = dict(scope) if scope is not self.globals else scope
                inner[var] = str(k)
                out += self.run(body, inner, depth)
                k += by
            return nxt
        m = re.match(r"(?is)^%do\s+%(while|until)\s*\((.*)\)\s*$", s)
        if m:
            n = 0
            while True:
                c = self._truth(self.evaluate(self.subst(m.group(2), scope)))
                if m.group(1).lower() == "while" and not c:
                    break
                n += 1
                if n > MAX_LOOP:
                    raise Unsupported(f"%DO %{m.group(1).upper()} loop runs more than {MAX_LOOP} times")
                out += self.run(body, scope, depth)
                if m.group(1).lower() == "until" and self._truth(self.evaluate(self.subst(m.group(2), scope))):
                    break
            return nxt
        raise Unsupported("unsupported %DO form")

    def call(self, name, argtext, scope, depth):
        self.calls += 1
        if self.calls > MAX_CALLS:
            raise Unsupported("too many macro calls (recursion?)")
        params, body = self.defs[name]
        pos = [a for a in _split_top(argtext)] if argtext.strip() else []
        local, names = {}, [p[0] for p in params]
        k = 0
        for a in pos:
            a = a.strip()
            m = re.match(r"(?s)^([A-Za-z_]\w*)\s*=(?!=)(.*)$", a)
            if m and m.group(1).lower() in names:
                local[m.group(1).lower()] = self.subst(m.group(2).strip(), scope)
            else:
                while k < len(names) and names[k] in local:
                    k += 1
                if k >= len(names):
                    raise Unsupported(f"%{name} called with too many arguments")
                local[names[k]] = self.subst(a, scope)
                k += 1
        for pn, dv in params:
            if pn not in local:
                local[pn] = self.subst(dv, scope) if dv is not None else ""
        self.used = True
        return self.run(body, local, depth)


_KEYWORDS = re.compile(r"(?i)%(let|if|then|else|do|end|macro|mend|put|global|local|eval|str|upcase|lowcase|sysfunc|symdel)\b")


def _split_bare_calls(stmts):
    """'%name(args)' written without ';' and followed by another %-statement: split them so %macro definitions after it are still collected"""
    out = []
    for s in stmts:
        while True:
            m = re.match(r"%[A-Za-z_]\w*\s*\(", s)
            if not m or _KEYWORDS.match(s):
                break
            close = _match_paren(s, m.end() - 1)
            rest = s[close + 1:].strip() if close > 0 else ""
            if not rest.startswith("%"):
                break
            out.append(s[:close + 1])
            s = rest
        out.append(s)
    return out


def expand_macros(stmts, report):
    """statements (already split on ';') -> statements with macros expanded; unresolvable pieces are left in place"""
    if not any(s.lstrip().startswith("%") or "&" in s for s in stmts):
        return stmts
    m = Macros(report)
    try:
        stmts = _split_bare_calls(stmts)
        stmts = m.collect(stmts)
        out = m.run(stmts, m.globals, 0)
    except Unsupported as e:
        report.warn("sas", f"macro expansion stopped: {str(e)[:120]}")
        return stmts
    if m.used:
        report.note("SAS macros and %LET variables were expanded at conversion time (like SAS expands them before running): the output contains their values, "
                    "not Python variables; macro logic that depends on run-time data is left as TODO")
    return out
