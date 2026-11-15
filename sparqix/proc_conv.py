"""Stored procedures / functions (T-SQL and Oracle PL/SQL) -> a PySpark function per routine.

 * variables and parameters become Python variables; values reach SQL through named parameters
   (`spark.sql(query, args={...})`, Spark 3.4+/Databricks) - they cannot be used as table/column names
 * SQL statements are converted with the SQL converter (dialect tsql / oracle)
 * control flow (IF/ELSE, WHILE, LOOP, FOR, TRY/CATCH, EXCEPTION), SELECT..INTO / SELECT @v=..., EXEC/calls, PRINT,
   RAISERROR/THROW/RAISE_APPLICATION_ERROR, #temp tables via temp views
 * not converted (loud at runtime via _todo(), listed in the report): cursors, dynamic SQL, @@ROWCOUNT / SQL%ROWCOUNT,
   LIKE/IN-subquery/CASE inside procedural conditions, GOTO
Row-by-row loops over query results run on the driver. Not run against real Spark."""
from __future__ import annotations

import ast
import builtins
import keyword
import re
from dataclasses import dataclass, field

from .core import Report, Unsupported, cm, dq

TOK = re.compile(r"""(?P<ws>\s+)|(?P<cmt>--[^\n]*|/\*.*?\*/)|(?P<str>'(?:[^']|'')*')|(?P<qid>"[^"]*"|\[[^\]]*\]|`[^`]*`)"""
                 r"""|(?P<var>@@?\w+)|(?P<tmp>\#\#?\w+)|(?P<num>\d+(?:\.\d+)?)|(?P<id>[A-Za-z_][\w$]*)"""
                 r"""|(?P<sym>:=|\.\.|<>|<=|>=|!=|\|\||.)""", re.S)


@dataclass
class T:
    k: str
    s: str
    a: int
    b: int

    @property
    def u(self):
        return self.s.upper()


def tokenize(src):
    out = []
    for m in TOK.finditer(src):
        k = m.lastgroup
        if k in ("ws", "cmt"):
            continue
        out.append(T(k, m.group(), m.start(), m.end()))
    return out


@dataclass
class Routine:
    name: str
    params: list = field(default_factory=list)       # (name, default_text|None, is_out)
    body: list = field(default_factory=list)
    is_function: bool = False
    dialect: str = "tsql"


def _py_name(n: str) -> str:
    n = n.lstrip("@:").replace("#", "tmp_")
    n = re.sub(r"\W", "_", n)
    if n.startswith("__"):
        n = "u" + n
    return n + "_" if keyword.iskeyword(n) or n in ("spark", "args") or n in dir(builtins) else n


_START = {"BEGIN", "SELECT", "SET", "EXEC", "EXECUTE", "PRINT", "RETURN", "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
          "DROP", "CREATE", "ALTER", "WITH", "RAISERROR", "THROW", "COMMIT", "ROLLBACK", "DECLARE", "WHILE", "IF", "GOTO", "END", "ELSE",
          "OPEN", "FETCH", "CLOSE", "DEALLOCATE", "USE", "GO"}
_IF_NOT_START = {"TABLE", "VIEW", "INDEX", "SCHEMA", "DATABASE", "FUNCTION", "PROC", "PROCEDURE"}


# ===================================================================== T-SQL parser
class TSQL:
    def __init__(self, src, toks, report):
        self.src, self.t, self.i, self.r = src, toks, 0, report

    def peek(self, n=0):
        return self.t[self.i + n] if self.i + n < len(self.t) else T("eof", "", len(self.src), len(self.src))

    def text(self, a, b):
        return self.src[self.t[a].a:self.t[b - 1].b] if b > a else ""

    def scan_expr_end(self, stop_at_start=True):
        """advance to the end of an expression/statement at depth 0; return end index"""
        depth, case, j, first = 0, 0, self.i, True
        while j < len(self.t):
            tk = self.t[j]
            if tk.s == "(":
                depth += 1
            elif tk.s == ")":
                depth -= 1
            elif depth == 0:
                if tk.s == ";":
                    return j
                if tk.u == "CASE" and tk.k == "id":
                    case += 1
                elif tk.u == "END" and case > 0 and tk.k == "id":
                    case -= 1
                elif stop_at_start and case == 0 and tk.k == "id" and tk.u in _START and not first:
                    prev = self.t[j - 1]
                    if not self._continues(j, prev):
                        return j
            first = False
            j += 1
        return j

    def _continues(self, j, prev):
        tk = self.t[j]
        kw0 = self.t[self.stmt_start].u if hasattr(self, "stmt_start") else ""
        if tk.u == "SELECT" and (prev.u in ("UNION", "ALL", "INTERSECT", "EXCEPT") or prev.s == "(" or kw0 in ("INSERT", "CREATE", "WITH", "MERGE")):
            return True
        if tk.u in ("INSERT", "UPDATE", "DELETE", "MERGE") and kw0 in ("WITH", "MERGE"):
            return True
        if tk.u == "SET" and kw0 in ("UPDATE", "MERGE"):
            return True
        if tk.u == "IF" and prev.u in _IF_NOT_START:
            return True
        if tk.u == "END" and False:
            return True
        if kw0 == "MERGE":
            return True
        if tk.u == "ELSE" and False:
            return True
        return False

    # ---- header
    def routines(self):
        out = []
        while self.i < len(self.t):
            tk = self.peek()
            if tk.u == "CREATE" or tk.u == "ALTER":
                j = self.i + 1
                while self.t[j].u in ("OR", "REPLACE", "ALTER"):
                    j += 1
                if self.t[j].u in ("PROC", "PROCEDURE", "FUNCTION"):
                    self.i = j
                    out.append(self.routine())
                    continue
            self.i += 1
        return out

    def routine(self):
        kind = self.peek().u
        self.i += 1
        name = []
        while self.peek().k in ("id", "qid") or self.peek().s == ".":
            name.append(self.peek().s.strip('[]"`'))
            self.i += 1
        r = Routine(name=_py_name("".join(name[-1:]) or "proc"), is_function=kind == "FUNCTION")
        paren = self.peek().s == "("
        if paren:
            self.i += 1
        while self.peek().k == "var":
            pn = _py_name(self.peek().s)
            self.i += 1
            if self.peek().u == "AS":
                self.i += 1
            while self.peek().s not in (",", "=", ")") and self.peek().u not in ("AS", "OUTPUT", "OUT", "READONLY", "WITH", "BEGIN") and self.peek().k != "var" and self.peek().k != "eof":
                if self.peek().s == "(":                    # type size
                    while self.peek().s != ")":
                        self.i += 1
                self.i += 1
            default = None
            if self.peek().s == "=":
                self.i += 1
                st = self.i
                self.i += 1
                default = self.text(st, self.i)
            out = False
            if self.peek().u in ("OUTPUT", "OUT"):
                out = True
                self.i += 1
            r.params.append((pn, default, out))
            if self.peek().s == ",":
                self.i += 1
        if paren and self.peek().s == ")":
            self.i += 1
        if kind == "FUNCTION":
            while self.peek().u != "AS" and self.peek().k != "eof":
                self.i += 1
        while self.peek().u in ("WITH", "RECOMPILE", "ENCRYPTION", "EXECUTE", "OWNER", "CALLER") or (self.peek().s == "," ):
            self.i += 1
        if self.peek().u == "AS":
            self.i += 1
        r.body = self.block_or_rest(top=True)
        return r

    def block_or_rest(self, top=False):
        stmts = []
        while self.i < len(self.t):
            tk = self.peek()
            if tk.s == ";":
                self.i += 1
                continue
            if tk.u == "END":
                break
            if top and tk.u in ("CREATE", "ALTER") and self.t[self.i + 1].u in ("PROC", "PROCEDURE", "FUNCTION", "OR"):
                break
            if top and tk.u == "GO":
                self.i += 1
                break
            stmts.append(self.stmt())
            if top and stmts[-1][0] == "block":
                pass
        return stmts

    def body_block(self):
        """BEGIN ... END  or a single statement"""
        if self.peek().u == "BEGIN" and self.peek(1).u not in ("TRY", "TRAN", "TRANSACTION", "CATCH"):
            self.i += 1
            b = self.block_or_rest()
            if self.peek().u == "END":
                self.i += 1
            return b
        return [self.stmt()]

    def stmt(self):
        tk = self.peek()
        self.stmt_start = self.i
        u = tk.u if tk.k == "id" else ""
        if u == "DECLARE":
            return self.declare()
        if u == "SET" and self.peek(1).k == "var":
            self.i += 1
            name = _py_name(self.peek().s)
            self.i += 1
            if self.peek().s in ("=", "+", "-", "*", "/"):
                op = self.peek().s
                self.i += 1
                if op != "=":
                    self.i += 1                            # +=  (the '=' token)
            st = self.i
            end = self.scan_expr_end()
            e = self.text(st, end)
            self.i = end
            if re.match(r"(?is)^\(\s*SELECT\b", e):
                return ("select_assign", [name], e.strip()[1:-1].strip())
            if op != "=":
                e = f"{name} {op} ({e})"
            return ("assign", name, e)
        if u == "SET":
            end = self.scan_expr_end()
            txt = self.text(self.i, end)
            self.i = end
            return ("noop", txt)
        if u == "IF":
            self.i += 1
            st = self.i
            end = self.scan_cond()
            cond = self.text(st, end)
            self.i = end
            body = self.body_block()
            els = None
            if self.peek().u == "ELSE":
                self.i += 1
                els = self.body_block()
            return ("if", [(cond, body)], els)
        if u == "WHILE":
            self.i += 1
            st = self.i
            end = self.scan_cond()
            cond = self.text(st, end)
            self.i = end
            return ("while", cond, self.body_block())
        if u == "BEGIN":
            nxt = self.peek(1).u
            if nxt == "TRY":
                self.i += 2
                body = self.block_or_rest()
                self.i += 2                                  # END TRY
                handler = []
                if self.peek().u == "BEGIN" and self.peek(1).u == "CATCH":
                    self.i += 2
                    handler = self.block_or_rest()
                    self.i += 2                              # END CATCH
                return ("try", body, handler)
            if nxt in ("TRAN", "TRANSACTION"):
                self.i += 2
                while self.peek().s not in (";",) and self.peek().u not in _START and self.peek().k != "eof":
                    self.i += 1
                return ("noop", "BEGIN TRAN")
            b = self.body_block()
            return ("block", b)
        if u in ("EXEC", "EXECUTE"):
            return self.exec_()
        if u == "RETURN":
            self.i += 1
            if self.peek().k == "eof" or self.peek().u in _START or self.peek().s == ";":
                return ("return", None)
            st = self.i
            end = self.scan_expr_end()
            self.i = end
            return ("return", self.text(st, end))
        if u == "PRINT":
            self.i += 1
            st = self.i
            end = self.scan_expr_end()
            self.i = end
            return ("print", self.text(st, end))
        if u in ("RAISERROR", "THROW"):
            self.i += 1
            st = self.i
            end = self.scan_expr_end()
            self.i = end
            return ("raise", self.text(st, end).strip("() "))
        if u in ("COMMIT", "ROLLBACK"):
            end = self.scan_expr_end()
            txt = self.text(self.i, end)
            self.i = end
            return ("noop", txt)
        if u in ("OPEN", "FETCH", "CLOSE", "DEALLOCATE", "GOTO"):
            end = self.scan_expr_end()
            txt = self.text(self.i, end)
            self.i = end
            return ("todo", txt, "cursors / GOTO have no automatic conversion: rewrite set-based")
        if u == "SELECT":
            return self.select_()
        # generic SQL statement
        st = self.i
        self.i += 1
        end = self.scan_expr_end()
        self.i = end
        return ("sql", self.text(st, end))

    def scan_cond(self):
        depth, j, case = 0, self.i, 0
        while j < len(self.t):
            tk = self.t[j]
            if tk.s == "(":
                depth += 1
            elif tk.s == ")":
                depth -= 1
            elif depth == 0 and tk.k == "id":
                if tk.u == "CASE":
                    case += 1
                elif tk.u == "END" and case:
                    case -= 1
                elif case == 0 and tk.u in _START and tk.u not in ("END",) and j > self.i:
                    return j
            elif depth == 0 and tk.s == ";":
                return j
            j += 1
        return j

    def declare(self):
        self.i += 1
        decls = []
        while True:
            if self.peek().k == "var":
                name = _py_name(self.peek().s)
                self.i += 1
                if self.peek().u == "AS":
                    self.i += 1
                typ = self.peek().u
                while self.peek().s not in (",", "=", ";") and self.peek().u not in _START and self.peek().k != "eof":
                    self.i += 1
                default = None
                if self.peek().s == "=":
                    self.i += 1
                    st = self.i
                    d = 0
                    while self.peek().k != "eof" and (d > 0 or (self.peek().s not in (",", ";") and self.peek().u not in _START)):
                        d += self.peek().s == "("
                        d -= self.peek().s == ")"
                        self.i += 1
                    default = self.text(st, self.i)
                decls.append(("declare", name, typ, default))
            else:
                end = self.scan_expr_end()
                txt = self.text(self.i, end)
                self.i = end
                return ("todo", "DECLARE " + txt, "cursors / table variables have no automatic conversion")
            if self.peek().s == ",":
                self.i += 1
                continue
            break
        return decls[0] if len(decls) == 1 else ("block", decls)

    def exec_(self):
        self.i += 1
        if self.peek().s == "(":
            end = self.scan_expr_end()
            txt = self.text(self.i, end)
            self.i = end
            return ("todo", "EXEC " + txt, "dynamic SQL: build the statement in Python and call spark.sql()")
        out = None
        if self.peek().k == "var" and self.peek(1).s == "=":
            out = _py_name(self.peek().s)
            self.i += 2
        name = []
        while self.peek().k in ("id", "qid") or (self.peek().s == "." and name):
            name.append(self.peek().s.strip('[]"`'))
            self.i += 1
        st = self.i
        end = self.scan_expr_end()
        self.i = end
        args = self.text(st, end)
        return ("call", _py_name([n for n in name if n != "."][-1]) if name else "?", _split_args(args), out)

    def select_(self):
        st = self.i
        end = self.scan_expr_end()
        self.i = end
        txt = self.text(st, end)
        m = re.match(r"(?is)^SELECT\s+(?:(?:TOP\s+\(?\d+\)?|DISTINCT|ALL)\s+)*(@\w+\s*=.*?)(\s+FROM\b.*)?$", txt)
        if m and re.match(r"@\w+\s*=", m.group(1)):
            parts = _split_args(m.group(1))
            names, cols = [], []
            for p in parts:
                mm = re.match(r"(?s)\s*@(\w+)\s*=\s*(.*)$", p)
                if not mm:
                    return ("todo", txt, "mixed variable assignment and column list in SELECT")
                names.append(_py_name(mm.group(1)))
                cols.append(mm.group(2))
            return ("select_assign", names, "SELECT " + ", ".join(cols) + (m.group(2) or ""))
        m = re.match(r"(?is)^(SELECT\s+.*?)\s+INTO\s+(#\w+)\s+(FROM\b.*)$", txt)
        if m:
            return ("select_view", _py_name(m.group(2)), m.group(1) + " " + m.group(3))
        return ("sql", txt)


# ===================================================================== PL/SQL parser
class PLSQL(TSQL):
    def routines(self):
        out = []
        while self.i < len(self.t):
            tk = self.peek()
            if tk.u == "CREATE":
                j = self.i + 1
                while self.t[j].u in ("OR", "REPLACE", "EDITIONABLE", "NONEDITIONABLE"):
                    j += 1
                if self.t[j].u in ("PROCEDURE", "FUNCTION"):
                    self.i = j
                    out.append(self.routine())
                    continue
            self.i += 1
        return out

    def routine(self):
        kind = self.peek().u
        self.i += 1
        name = [self.peek().s.strip('"')]
        self.i += 1
        while self.peek().s == ".":
            self.i += 1
            name.append(self.peek().s.strip('"'))
            self.i += 1
        r = Routine(name=_py_name(name[-1]), is_function=kind == "FUNCTION", dialect="oracle")
        if self.peek().s == "(":
            self.i += 1
            while self.peek().s != ")" and self.peek().k != "eof":
                pn = _py_name(self.peek().s)
                self.i += 1
                out = False
                if self.peek().u == "IN":
                    self.i += 1
                if self.peek().u == "OUT":
                    out = True
                    self.i += 1
                    if self.peek().u == "NOCOPY":
                        self.i += 1
                while self.peek().s not in (",", ")", ":=") and self.peek().u != "DEFAULT":
                    if self.peek().s == "(":
                        while self.peek().s != ")":
                            self.i += 1
                    self.i += 1
                default = None
                if self.peek().s == ":=" or self.peek().u == "DEFAULT":
                    self.i += 1
                    st = self.i
                    while self.peek().s not in (",", ")"):
                        self.i += 1
                    default = self.text(st, self.i)
                r.params.append((pn, default, out))
                if self.peek().s == ",":
                    self.i += 1
            self.i += 1
        if kind == "FUNCTION":
            while self.peek().u not in ("IS", "AS") and self.peek().k != "eof":
                self.i += 1
        while self.peek().u not in ("IS", "AS") and self.peek().k != "eof":
            self.i += 1
        self.i += 1
        decls = []
        while self.peek().u != "BEGIN" and self.peek().k != "eof":      # declarations
            if self.peek().k == "id" and self.peek().u == "CURSOR" and self.peek(1).k == "id":
                end = self.semi()
                head = self.text(self.i + 1, end)
                m = re.match(r"(?is)^(\w+)\s+(WITH_RETURN\s+)?(?:IS|AS|FOR)\s+(.*)$", head.strip())
                if m:
                    decls.append(("cursor", _py_name(m.group(1)), m.group(3).strip(), bool(m.group(2))))
                else:
                    decls.append(("todo", head, "cursor declaration not recognised"))
                self.i = end + 1
                continue
            if self.peek().k == "id" and self.peek().u == "HANDLER_DECL":
                kind, cond = self.peek(1).u, self.peek(2).u
                self.i += 4                                      # HANDLER_DECL kind cond DO
                decls.append(("handler", kind, cond, [self.pstmt()]))
                continue
            if self.peek().k == "id" and self.peek().u in ("CURSOR", "TYPE", "PRAGMA"):
                end = self.semi()
                decls.append(("todo", self.text(self.i, end), "cursor/type/pragma declarations have no automatic conversion"))
                self.i = end + 1
                continue
            if self.peek().k == "id" and self.peek(1).u == "CURSOR":
                end = self.semi()
                decls.append(("todo", self.text(self.i, end), "cursors have no automatic conversion: rewrite set-based"))
                self.i = end + 1
                continue
            name = _py_name(self.peek().s)
            self.i += 1
            default = None
            while self.peek().s != ";" and self.peek().k != "eof":
                if self.peek().s == ":=" or self.peek().u == "DEFAULT":
                    self.i += 1
                    st = self.i
                    while self.peek().s != ";":
                        self.i += 1
                    default = self.text(st, self.i)
                    break
                self.i += 1
            self.i += 1
            decls.append(("declare", name, "", default))
        r.body = decls + self.plsql_block()
        return r

    def semi(self):
        j = self.i
        depth = 0
        while j < len(self.t) and not (self.t[j].s == ";" and depth == 0):
            depth += self.t[j].s == "("
            depth -= self.t[j].s == ")"
            j += 1
        return j

    def plsql_block(self):
        """BEGIN stmts [EXCEPTION ...] END [name];  -> statements"""
        assert self.peek().u == "BEGIN", self.peek()
        self.i += 1
        body = self.stmts(("END", "EXCEPTION"))
        if self.peek().u == "EXCEPTION":
            self.i += 1
            handlers = []
            while self.peek().u == "WHEN":
                self.i += 1
                while self.peek().u != "THEN":
                    self.i += 1
                self.i += 1
                handlers += self.stmts(("WHEN", "END"))
            body = [("try", body, handlers)]
        if self.peek().u == "END":
            self.i += 1
            while self.peek().s != ";" and self.peek().k != "eof":
                self.i += 1
            self.i += 1
        return body

    def stmts(self, stop):
        out = []
        while self.i < len(self.t) and not (self.peek().k == "id" and self.peek().u in stop):
            if self.peek().s == ";":
                self.i += 1
                continue
            out.append(self.pstmt())
        return out

    def until_kw(self, kw):
        st, depth = self.i, 0
        while self.i < len(self.t):
            tk = self.peek()
            depth += tk.s == "("
            depth -= tk.s == ")"
            if depth == 0 and tk.k == "id" and tk.u == kw:
                break
            self.i += 1
        txt = self.text(st, self.i)
        self.i += 1
        return txt

    def pstmt(self):
        tk = self.peek()
        u = tk.u if tk.k == "id" else ""
        if u == "IF":
            self.i += 1
            branches, els = [], None
            cond = self.until_kw("THEN")
            body = self.stmts(("ELSIF", "ELSE", "END"))
            branches.append((cond, body))
            while self.peek().u == "ELSIF":
                self.i += 1
                cond = self.until_kw("THEN")
                branches.append((cond, self.stmts(("ELSIF", "ELSE", "END"))))
            if self.peek().u == "ELSE":
                self.i += 1
                els = self.stmts(("END",))
            self.i += 3                                          # END IF ;
            return ("if", branches, els)
        if u == "WHILE":
            self.i += 1
            cond = self.until_kw("LOOP")
            body = self.stmts(("END",))
            self.i += 3
            return ("while", cond, body)
        if u == "LOOP":
            self.i += 1
            body = self.stmts(("END",))
            self.i += 3
            return ("while", "TRUE", body)
        if u == "FOR":
            self.i += 1
            var = _py_name(self.peek().s)
            self.i += 2                                          # var IN
            rev = False
            if self.peek().u == "REVERSE":
                rev = True
                self.i += 1
            head = self.until_kw("LOOP")
            body = self.stmts(("END",))
            self.i += 3
            m = re.match(r"(?s)^(.*?)\s*\.\.\s*(.*)$", head)
            if m and not head.lstrip().startswith("("):
                return ("for_range", var, m.group(1), m.group(2), rev, body)
            h = head.strip()
            if h.startswith("(") and h.endswith(")"):
                return ("for_query", var, h[1:-1].strip(), body)
            return ("todo", f"FOR {var} IN {head} LOOP", "FOR over a named cursor has no automatic conversion")
        if u == "BEGIN":
            return ("block", self.plsql_block())
        if u == "NULL" and self.peek(1).s == ";":
            self.i += 2
            return ("noop", "NULL")
        if u in ("COMMIT", "ROLLBACK", "SAVEPOINT"):
            end = self.semi()
            txt = self.text(self.i, end)
            self.i = end + 1
            return ("noop", txt)
        if u == "RETURN":
            self.i += 1
            end = self.semi()
            txt = self.text(self.i, end)
            self.i = end + 1
            return ("return", txt or None)
        if u == "EXIT" or u == "CONTINUE":
            end = self.semi()
            txt = self.text(self.i + 1, end)
            self.i = end + 1
            m = re.match(r"(?is)^WHEN\s+(.*)$", txt.strip())
            return ("exit" if u == "EXIT" else "continue", m.group(1) if m else None)
        if u in ("OPEN", "FETCH", "CLOSE") or (u == "EXECUTE" and self.peek(1).u == "IMMEDIATE"):
            end = self.semi()
            txt = self.text(self.i, end)
            self.i = end + 1
            m = re.match(r"(?is)^(OPEN|CLOSE)\s+(\w+)$", txt.strip())
            if m:
                return (m.group(1).lower(), _py_name(m.group(2)))
            m = re.match(r"(?is)^FETCH\s+(?:NEXT\s+|FIRST\s+)?(?:FROM\s+)?(\w+)\s+INTO\s+(.+)$", txt.strip())
            if m:
                return ("fetch", _py_name(m.group(1)), [_py_name(x.strip()) for x in m.group(2).split(",")])
            return ("todo", txt, "cursors / dynamic SQL (EXECUTE IMMEDIATE) have no automatic conversion")
        if u == "SELECT":
            end = self.semi()
            txt = self.text(self.i, end)
            self.i = end + 1
            m = re.match(r"(?is)^(SELECT\s+.*?)\s+INTO\s+(.*?)\s+(FROM\b.*)$", txt)
            if m:
                names = [_py_name(x) for x in m.group(2).split(",")]
                return ("select_assign", names, m.group(1) + " " + m.group(3))
            return ("sql", txt)
        if u in ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "CREATE", "DROP", "ALTER", "WITH"):
            end = self.semi()
            txt = self.text(self.i, end)
            self.i = end + 1
            return ("sql", txt)
        # assignment / call
        if self.peek(1).s == ":=":
            name = _py_name(tk.s)
            self.i += 2
            end = self.semi()
            e = self.text(self.i, end)
            self.i = end + 1
            return ("assign", name, e)
        end = self.semi()
        txt = self.text(self.i, end)
        self.i = end + 1
        m = re.match(r"(?is)^DBMS_OUTPUT\s*\.\s*PUT_LINE\s*\((.*)\)$", txt.strip())
        if m:
            return ("print", m.group(1))
        m = re.match(r"(?is)^RAISE_APPLICATION_ERROR\s*\((.*)\)$", txt.strip())
        if m:
            return ("raise", m.group(1))
        if re.match(r"(?is)^RAISE\b", txt.strip()):
            return ("raise", "")
        if re.match(r"(?is)^RESIGNAL\b", txt.strip()):
            return ("reraise",)
        if re.match(r"(?is)^SIGNAL\b", txt.strip()):
            return ("raise", txt.strip()[6:])
        m = re.match(r"(?is)^([\w.]+)\s*(?:\((.*)\))?$", txt.strip())
        if m:
            return ("call", _py_name(m.group(1).split(".")[-1]), _split_args(m.group(2) or ""), None)
        return ("todo", txt, "statement not recognised")


def _split_args(s: str):
    out, depth, cur, q = [], 0, [], None
    for ch in s:
        if q:
            cur.append(ch)
            if ch == q:
                q = None
        elif ch in "'\"":
            q = ch
            cur.append(ch)
        elif ch in "([":
            depth += 1
            cur.append(ch)
        elif ch in ")]":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


# ===================================================================== emitter
class Emitter:
    def __init__(self, routine: Routine, report: Report):
        self.r, self.rep, self.L = routine, report, []
        self.vars = {p[0].lower(): p[0] for p in routine.params}
        self.helpers, self.has_result, self.outs = set(), False, [p[0] for p in routine.params if p[2]]
        self.cursors, self.not_found, self.rowvars, self.exprs = {}, None, {}, {}

    # ---- sql
    def vars_to_params(self, text: str):
        used = []
        if self.r.dialect == "tsql":
            def sub(m):
                n = _py_name(m.group(0))
                used.append(n)
                return ":" + n
            text = re.sub(r"(?<![\w@])@(?!@)\w+", sub, text)
        else:
            def sub(m):
                n = m.group(0)
                if n.lower() in self.vars and not text[:m.start()].rstrip().endswith("."):
                    used.append(self.vars[n.lower()])
                    return ":" + self.vars[n.lower()]
                return n
            parts = re.split(r"('(?:[^']|'')*')", text)

            def rowfield(m):
                v = self.rowvars.get(m.group(1).lower())
                if not v:
                    return m.group(0)
                n = f"{v}_{m.group(2)}"
                self.exprs[n] = f"{v}[{m.group(2)!r}]"
                used.append(n)
                return ":" + n
            if self.r.dialect == "teradata":  # SPL writes variables in SQL as :name already
                parts = [p if i % 2 else re.sub(r"(?<![:\w]):(?=[A-Za-z_])", "", p) for i, p in enumerate(parts)]
            if self.rowvars:
                parts = [p if i % 2 else re.sub(r"(?<![\w.])(\w+)\.(\w+)\b", rowfield, p) for i, p in enumerate(parts)]
                parts = [p if i % 2 else re.sub(r"(?<![\w])(\w+)\.(\w+)\b", lambda m: m.group(0), p) for i, p in enumerate(parts)]
            text = "".join(p if i % 2 else re.sub(r"(?<![:\w])\b[A-Za-z_][\w$]*\b", sub, p) for i, p in enumerate(parts))
        text = re.sub(r"#\w+", lambda m: "tmp_" + m.group(0).lstrip("#"), text)
        return text, list(dict.fromkeys(used))

    def sql_call(self, text: str, where: str) -> str:
        from .sql_conv import convert_sql
        text, used = self.vars_to_params(text)
        sub = Report()
        out = convert_sql(text, sub, dialect=self.r.dialect, out="sql").strip().rstrip(";").strip()
        for _, msg in sub.todos:
            (self.rep.warn if "only works on Delta" in msg else self.rep.todo)(where, msg)
        for _, msg in sub.warnings:
            self.rep.warn(where, msg)
        out = "\n".join(l for l in out.splitlines() if not l.startswith("--"))
        lit = '"""' + out.replace("\\", "\\\\").replace('"""', '\\"\\"\\"') + '"""' if "\n" in out or '"' in out else repr(out)
        if used:
            return f"spark.sql({lit}, args={{{', '.join(repr(u) + ': ' + self.exprs.get(u, u) for u in used)}}})"
        return f"spark.sql({lit})"

    # ---- python expressions
    def expr(self, text: str, where: str) -> str:
        t = text.strip()
        if not t:
            return "None"
        exists = []

        def ex(m):
            start = m.end()
            depth, j = 1, start
            while j < len(t) and depth:
                depth += t[j] == "("
                depth -= t[j] == ")"
                j += 1
            sub = t[start:j - 1]
            exists.append((bool(m.group(1)), sub, (m.start(), j)))
            return f" __EXISTS{len(exists) - 1}__ "
        # replace from the end so spans stay valid: do a single regex pass instead
        pieces, pos, res = [], 0, ""
        for m in re.finditer(r"(?i)\b(NOT\s+)?EXISTS\s*\(", t):
            if m.start() < pos:
                continue
            start = m.end()
            depth, j = 1, start
            while j < len(t) and depth:
                depth += t[j] == "("
                depth -= t[j] == ")"
                j += 1
            exists.append((bool(m.group(1)), t[start:j - 1]))
            res += t[pos:m.start()] + f" __EXISTS{len(exists) - 1}__ "
            pos = j
        t = res + t[pos:]
        toks, out = tokenize(t), []
        for k, tk in enumerate(toks):
            u = tk.u
            if tk.k == "str":
                out.append(tk.s)
            elif tk.k == "num":
                out.append(tk.s)
            elif tk.k == "var":
                if tk.s.startswith("@@"):
                    raise Unsupported(f"{tk.s} has no equivalent")
                out.append(_py_name(tk.s))
            elif tk.k == "id":
                if u in ("AND", "OR", "NOT"):
                    out.append(u.lower())
                elif u == "NULL":
                    out.append("None")
                elif u in ("TRUE", "FALSE"):
                    out.append(u.capitalize())
                elif u == "IS":
                    nxt = toks[k + 1].u if k + 1 < len(toks) else ""
                    out.append("is")
                elif u in ("LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "IN", "SELECT"):
                    raise Unsupported(f"{u} inside a procedural expression")
                elif tk.s.startswith("__EXISTS"):
                    n = int(re.sub(r"\D", "", tk.s))
                    neg, sub = exists[n]
                    q = self.sql_call("SELECT 1 WHERE EXISTS (" + sub + ")", where)
                    out.append(f"({q}.count() {'==' if neg else '>'} 0)")
                elif k + 1 < len(toks) and toks[k + 1].s == "(":
                    out.append(tk.s)
                elif tk.s.lower() in self.vars:
                    out.append(self.vars[tk.s.lower()])
                else:
                    out.append(_py_name(tk.s))
            elif tk.k == "sym":
                m = {"=": "==", "<>": "!=", "||": "+", ":=": "="}.get(tk.s, tk.s)
                out.append(m)
            else:
                raise Unsupported(f"token {tk.s!r}")
        py = " ".join(out)
        py = re.sub(r"\bis\s+not\s+None", "is not None", py)
        py = re.sub(r"\bis\s+not\b(?!\s+None)", "is not", py)
        py = re.sub(r"\bis not None\b", "is not None", py)
        py = re.sub(r"\bis\s+not\s+not\b", "is not", py)
        py = py.replace("is not None", "\0").replace("is None", "\1")
        py = py.replace("\0", "is not None").replace("\1", "is None")
        py = re.sub(r"\bis\s+not\s+(None)\b", r"is not \1", py)
        py = re.sub(r"\bis\s+NOT\b", "is not", py)
        py = re.sub(r"\bis\s+not\s+None\b", "is not None", py)
        py = re.sub(r"\bis not\b\s+(?=None)", "is not ", py)
        py = re.sub(r"\bis\s+not\s+", "is not ", py)
        py = re.sub(r"\bis\s+None\b", "is None", py)
        py = re.sub(r"\bis\s+(?=None)", "is ", py)
        py = re.sub(r"\bis (?!not|None)", "== ", py) if False else py
        py = re.sub(r"\bnot\s+is\b", "is not", py)
        try:
            tree = ast.parse(py, mode="eval")
        except SyntaxError:
            raise Unsupported(f"could not translate expression: {text.strip()[:80]}")
        outer = self

        class F(ast.NodeTransformer):
            def visit_Call(self, n):
                self.generic_visit(n)
                if isinstance(n.func, ast.Name):
                    f = n.func.id.upper()
                    a = n.args
                    simple = {"LEN": "len", "LENGTH": "len", "ABS": "abs", "ROUND": "round", "STR": "str", "TO_CHAR": "str"}
                    if f in simple:
                        n.func.id = simple[f]
                    elif f in ("GETDATE", "SYSDATETIME", "SYSDATE", "SYSTIMESTAMP", "CURRENT_TIMESTAMP") and not a:
                        outer.helpers.add("datetime")
                        return ast.parse("datetime.now()", mode="eval").body
                    elif f in ("ISNULL", "NVL", "COALESCE") and len(a) == 2:
                        outer.helpers.add("coalesce")
                        n.func.id = "_coalesce"
                    elif f in ("UPPER", "LOWER") and len(a) == 1:
                        return ast.Call(ast.Attribute(ast.Call(ast.Name("str", ast.Load()), a, []), f.lower(), ast.Load()), [], [])
                    elif f in ("LTRIM", "RTRIM", "TRIM") and len(a) == 1:
                        m = {"LTRIM": "lstrip", "RTRIM": "rstrip", "TRIM": "strip"}[f]
                        return ast.Call(ast.Attribute(ast.Call(ast.Name("str", ast.Load()), a, []), m, ast.Load()), [], [])
                    else:
                        raise Unsupported(f"function {n.func.id}() in a procedural expression")
                return n
        try:
            tree = F().visit(tree)
        except Unsupported:
            raise
        return ast.unparse(ast.fix_missing_locations(tree))

    def safe_expr(self, text, where):
        try:
            return self.expr(text, where)
        except Unsupported as e:
            self.rep.todo(where, f"{e}: `{text.strip()[:90]}`")
            self.helpers.add("todo")
            return f"_todo({('%s: %s' % (e, text.strip()))[:120]!r})"

    # ---- statements
    def w(self, ind, line):
        self.L.append("    " * ind + line)

    def body(self, stmts, ind):
        n0 = len(self.L)
        for s in stmts:
            self.stmt(s, ind)
        if len(self.L) == n0:
            self.w(ind, "pass")

    def stmt(self, s, ind):
        k = s[0]
        loc = f"{self.r.name}"
        if k == "block":
            for x in s[1]:
                self.stmt(x, ind)
        elif k == "declare":
            _, name, typ, default = s
            self.vars[name.lower()] = name
            self.w(ind, f"{name} = {self.safe_expr(default, loc + '.' + name) if default else 'None'}")
        elif k == "assign":
            _, name, e = s
            self.vars.setdefault(name.lower(), name)
            self.w(ind, f"{name} = {self.safe_expr(e, loc + '.' + name)}")
        elif k == "select_assign":
            _, names, q = s
            for n in names:
                self.vars.setdefault(n.lower(), n)
            call = self.sql_call(q, loc)
            if len(names) == 1:
                self.w(ind, f"{names[0]} = ({call}.first() or [None])[0]")
            else:
                self.w(ind, f"_row = {call}.first()")
                for i, n in enumerate(names):
                    self.w(ind, f"{n} = _row[{i}] if _row is not None else None")
        elif k == "select_view":
            _, view, q = s
            self.w(ind, f"{self.sql_call(q, loc)}.createOrReplaceTempView({view!r})")
        elif k == "sql":
            text = s[1]
            if re.match(r"(?is)^\s*(SELECT|WITH)\b", text) and not re.search(r"(?is)\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bMERGE\b", text[:60]):
                self.has_result = True
                self.w(ind, f"result = {self.sql_call(text, loc)}")
            else:
                if re.match(r"(?is)^\s*(INSERT\s+INTO|UPDATE|DELETE\s+FROM|DELETE|MERGE\s+INTO)\s+#", text):
                    self.rep.todo(loc, "DML against a #temp table: temp views are read-only; use a real (Delta) table")
                    self.w(ind, f"# TODO(sparqix): DML on #temp table: {cm(' '.join(text.split())[:100])}")
                    return
                if re.match(r"(?is)^\s*CREATE\s+TABLE\s+#", text):
                    self.rep.warn(loc, "CREATE TABLE #temp skipped: the temp view is created by the later SELECT/INSERT")
                    self.w(ind, f"# CREATE TABLE #temp skipped: {cm(' '.join(text.split())[:80])}")
                    return
                m = re.match(r"(?is)^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?#(\w+)", text)
                if m:
                    self.w(ind, f"spark.catalog.dropTempView('tmp_{m.group(1)}')")
                    return
                self.w(ind, self.sql_call(text, loc))
        elif k == "if":
            _, branches, els = s
            for i, (cond, body) in enumerate(branches):
                self.w(ind, f"{'if' if i == 0 else 'elif'} {self.safe_expr(cond, loc + ' IF')}:")
                self.body(body, ind + 1)
            if els:
                self.w(ind, "else:")
                self.body(els, ind + 1)
        elif k == "while":
            _, cond, body = s
            self.w(ind, f"while {self.safe_expr(cond, loc + ' WHILE')}:")
            self.body(body, ind + 1)
        elif k == "for_range":
            _, var, lo, hi, rev, body = s
            self.vars[var.lower()] = var
            lo_, hi_ = self.safe_expr(lo, loc), self.safe_expr(hi, loc)
            rng = f"range({hi_}, {lo_} - 1, -1)" if rev else f"range({lo_}, {hi_} + 1)"
            self.w(ind, f"for {var} in {rng}:")
            self.body(body, ind + 1)
        elif k == "for_query":
            _, var, q, body = s
            self.vars[var.lower()] = var
            self.rowvars[var.lower()] = var
            self.rep.warn(loc, "FOR rec IN (query) collects the rows to the driver and loops in Python: prefer set-based SQL")
            self.w(ind, f"for {var} in {self.sql_call(q, loc)}.collect():")
            self.body(body, ind + 1)
        elif k == "cursor":
            _, name, q, ret = s
            self.cursors[name] = (q, ret)
        elif k == "handler":
            pass
        elif k == "open":
            name = s[1]
            if name not in self.cursors:
                self.rep.todo(loc, f"OPEN {name}: cursor not declared in this procedure")
                self.helpers.add("todo")
                self.w(ind, f"_todo('OPEN {name}: cursor not declared')")
                return
            q, ret = self.cursors[name]
            if ret:
                self.rep.note(f"{loc}: cursor {name} WITH RETURN -> OPEN returns the DataFrame (the result set) from the function")
                self.w(ind, f"return {self.sql_call(q, loc)}")
            else:
                self.rep.warn(loc, f"cursor {name} is fetched to the driver (.collect()) and read row by row: prefer set-based SQL")
                self.w(ind, f"{name}_rows = {self.sql_call(q, loc)}.collect()")
                self.w(ind, f"{name}_pos = 0")
        elif k == "fetch":
            _, name, names = s
            if name not in self.cursors:
                self.rep.todo(loc, f"FETCH {name}: cursor not declared in this procedure")
                self.helpers.add("todo")
                self.w(ind, f"_todo('FETCH {name}: cursor not declared')")
                return
            for n in names:
                self.vars.setdefault(n.lower(), n)
            self.w(ind, f"if {name}_pos < len({name}_rows):")
            for i, n in enumerate(names):
                self.w(ind + 1, f"{n} = {name}_rows[{name}_pos][{i}]")
            self.w(ind + 1, f"{name}_pos += 1")
            self.w(ind, "else:")
            if self.not_found:
                self.body(self.not_found, ind + 1)
            else:
                self.rep.warn(loc, f"FETCH {name} past the last row: no NOT FOUND handler, so the variables keep their old values")
                self.w(ind + 1, "pass")
        elif k == "close":
            self.w(ind, f"{s[1]}_rows = None")
        elif k == "reraise":
            self.w(ind, "raise")
        elif k == "try":
            _, body, handler = s
            self.w(ind, "try:")
            self.body(body, ind + 1)
            self.w(ind, "except Exception as err:")
            self.body(handler, ind + 1)
        elif k == "call":
            _, name, args, out = s
            a = ", ".join(self.safe_expr(x.split("=", 1)[1] if re.match(r"(?s)\s*@\w+\s*=[^=]", x) else x, loc) for x in args)
            call = f"{name}(spark{', ' + a if a else ''})"
            self.w(ind, f"{out} = {call}" if out else call)
            self.rep.note(f"{loc}: calls {name}(): convert it too (same file or module); OUTPUT parameters come back as a dict")
        elif k == "return":
            e = s[1]
            if e and re.match(r"(?is)^\s*\(?\s*SELECT\b", e):
                self.w(ind, f"return {self.sql_call(e.strip().strip('()') if e.strip().startswith('(') else e, loc)}")
            elif e:
                self.w(ind, f"return {self.safe_expr(e, loc + ' RETURN')}")
            else:
                self.w(ind, self.ret())
        elif k == "print":
            self.w(ind, f"print({self.safe_expr(s[1], loc + ' PRINT')})")
        elif k == "raise":
            msg = s[1]
            parts = _split_args(msg)
            text = next((p for p in parts if p.startswith("'") and p.endswith("'") and len(p) >= 2), "'error'")
            text = repr(text[1:-1].replace("''", "'"))
            self.w(ind, f"raise RuntimeError({text})")
        elif k == "exit":
            if s[1]:
                self.w(ind, f"if {self.safe_expr(s[1], loc + ' EXIT')}:")
                self.w(ind + 1, "break")
            else:
                self.w(ind, "break")
        elif k == "continue":
            if s[1]:
                self.w(ind, f"if {self.safe_expr(s[1], loc)}:")
                self.w(ind + 1, "continue")
            else:
                self.w(ind, "continue")
        elif k == "noop":
            self.w(ind, f"# {cm(' '.join(s[1].split())[:100])}  (not needed: Delta statements are atomic / session settings do not apply)")
        elif k == "todo":
            self.rep.todo(loc, f"{s[2]}: `{' '.join(s[1].split())[:90]}`")
            self.helpers.add("todo")
            self.w(ind, f"# TODO(sparqix): {cm(' '.join(s[1].split())[:140])}")
            self.w(ind, f"_todo({s[2]!r})")

    def apply_handlers(self, body, loc):
        """SPL DECLARE ... HANDLER: NOT FOUND runs after a FETCH that finds no row; SQLEXCEPTION wraps the body in try/except"""
        handlers = [d for d in body if d[0] == "handler"]
        if not handlers:
            return body
        rest = [d for d in body if d[0] != "handler"]
        decl_n = 0
        while decl_n < len(rest) and rest[decl_n][0] in ("declare", "cursor"):
            decl_n += 1
        head, tail = rest[:decl_n], rest[decl_n:]
        for _, kind, cond, hbody in handlers:
            if cond == "NOTFOUND":
                self.not_found = hbody
            elif cond == "SQLWARNING":
                self.rep.warn(loc, "SQLWARNING handler dropped: Spark SQL raises no warnings to handle")
            elif cond == "SQLEXCEPTION":
                if kind == "UNDO":
                    self.rep.warn(loc, "UNDO HANDLER: statements already run are NOT rolled back (Delta statements are individually atomic; there is no multi-statement transaction)")
                if kind == "CONTINUE":
                    self.rep.warn(loc, "CONTINUE HANDLER FOR SQLEXCEPTION: each top-level statement is wrapped in its own try/except")
                    tail = [("try", [st], hbody) for st in tail]
                else:
                    tail = [("try", tail, hbody + [("return", None)])]
            else:
                self.rep.todo(loc, f"handler for condition {cond} (SQLSTATE / named condition) has no automatic conversion: catch the exception by hand")
                self.helpers.add("todo")
        return head + tail

    def ret(self):
        if self.outs:
            return "return {" + ", ".join(f"{o!r}: {o}" for o in self.outs) + "}"
        return "return result" if self.has_result else "return"

    def emit(self):
        r = self.r
        sig = ["spark"] + [p[0] + (f"={self.safe_expr(p[1], r.name)}" if p[1] else "") for p in r.params if p[0]]
        # params without defaults after defaults would be invalid: move defaults to keyword-only style
        seen_default, fixed = False, []
        for s in sig:
            if "=" in s:
                seen_default = True
            elif seen_default and s != "spark":
                s = s + "=None"
            fixed.append(s)
        self.L.append(f"def {r.name}({', '.join(fixed)}):")
        self.w(1, f'"""Converted from {"function" if r.is_function else "procedure"} {dq(r.name)} ({dq(r.dialect)})"""')
        for p in r.params:
            self.vars[p[0].lower()] = p[0]
        mark = len(self.L)
        self.body(self.apply_handlers(r.body, r.name), 1)
        last = self.L[-1].strip() if len(self.L) > mark else ""
        if not last.startswith("return"):
            if self.outs or self.has_result:
                self.w(1, self.ret())
        elif self.has_result and self.L[-1].strip() == "return":
            self.L[-1] = "    " + self.ret()
        return "\n".join(self.L)


_SPL_HEAD = re.compile(r"(?is)\b(?:REPLACE|CREATE)[ \t\r\n]+PROCEDURE[ \t\r\n]+([\w.\"]+)[ \t\r\n]*\(")


def looks_like_spl(source: str) -> bool:
    """Teradata SPL: REPLACE PROCEDURE, or CREATE PROCEDURE using SPL-only syntax"""
    if re.search(r"(?im)^[ \t]*REPLACE[ \t]+PROCEDURE\b", source):
        return True
    return bool(re.search(r"(?im)^[ \t]*CREATE[ \t]+PROCEDURE\b", source)) and bool(
        re.search(r"(?i)\bEND[ \t]+(WHILE|FOR|LOOP|REPEAT)\b|\bELSEIF\b|\bDECLARE\b[^;\n]*\bDEFAULT\b|\bCONTINUE[ \t]+HANDLER\b|\bEXIT[ \t]+HANDLER\b", source))


def spl_to_plsql(source: str, report: Report) -> str:
    """Normalize Teradata SPL into the PL/SQL-like text the PL/SQL parser understands; SPL-only features become TODOs"""
    out, pos = [], 0
    for m in _SPL_HEAD.finditer(source):
        if m.start() < pos:
            continue
        o = m.end() - 1
        depth, j = 0, o
        while j < len(source):
            depth += source[j] == "("
            depth -= source[j] == ")"
            if depth == 0:
                break
            j += 1
        params = []
        for part in _split_args(source[o + 1:j]):
            pm = re.fullmatch(r"(?is)(?:(IN|OUT|INOUT)[ \t\r\n]+)?(\w+)[ \t\r\n]+(.+)", part.strip())
            if pm:
                params.append(f"{pm.group(2)} {(pm.group(1) or 'IN').upper()} {' '.join(pm.group(3).split())}")
        bm = re.search(r"(?i)\bBEGIN\b", source[j:])
        if not bm:
            continue
        b0 = j + bm.start()
        # the body runs to the final END; (procedures in one file are separated by their own END;)
        em = re.search(r"(?is)\bEND[ \t]*;", source[b0:])
        nxt = _SPL_HEAD.search(source, b0)
        end = (nxt.start() if nxt else len(source))
        body = source[b0 + 5:end]
        body = body.rstrip()
        body = re.sub(r"(?is)[ \t\r\n]*;?[ \t\r\n]*$", "", body)
        body = re.sub(r"(?is)[ \t\r\n]*\bEND\b$", "", body)
        decls = []
        body = re.sub(r"(?im)^([ \t]*)\w+[ \t]*:[ \t]*(?=(?:LOOP|WHILE|FOR|REPEAT|BEGIN)\b)", r"\1", body)
        body = re.sub(r"(?i)\b(END[ \t]+(?:LOOP|WHILE|FOR|REPEAT))[ \t]+\w+[ \t]*(?=;)", r"\1", body)
        body = re.sub(r"(?i)\bLEAVE\b[^;]*;", "EXIT;", body)
        body = re.sub(r"(?i)\bITERATE\b[^;]*;", "CONTINUE;", body)
        body = re.sub(r"(?i)(;|\bTHEN|\bELSE|\bDO|\bBEGIN)([ \t\r\n]*)SET[ \t]+(\w+)[ \t]*=(?!=)[ \t]*", r"\1\2\3 := ", body)

        def set_assign(t):
            return re.sub(r"(?is)^\s*SET\s+(\w+)\s*=\s*", r"\1 := ", t)

        def cond_kinds(c):
            kinds = []
            for one in re.split(r"[ \t\r\n]*,[ \t\r\n]*", c.strip()):
                u = " ".join(one.upper().split())
                kinds.append("NOTFOUND" if u == "NOT FOUND" or re.fullmatch(r"SQLSTATE(?: VALUE)? '02000'", u)
                             else "SQLEXCEPTION" if u == "SQLEXCEPTION" else "SQLWARNING" if u == "SQLWARNING"
                             else re.sub(r"\W+", "_", u)[:40] or "OTHER")
            return kinds

        def add_handler(kind, cond, stmt):
            for k in cond_kinds(cond):
                decls.append(f"HANDLER_DECL {kind.upper()} {k} DO {stmt};")

        def hoist_block(dm):
            add_handler(dm.group(1), dm.group(2), "BEGIN " + dm.group(3).strip() + " END")
            return ""
        body = re.sub(r"(?is)\bDECLARE[ \t\r\n]+(CONTINUE|EXIT|UNDO)[ \t\r\n]+HANDLER[ \t\r\n]+FOR[ \t\r\n]+([^;]*?)[ \t\r\n]+BEGIN\b(.*?)\bEND[ \t]*;", hoist_block, body)

        def hoist(dm):
            txt = " ".join(dm.group(1).split())
            hm = re.match(r"(?is)(CONTINUE|EXIT|UNDO)[ \t]+HANDLER[ \t]+FOR[ \t]+(.+?)[ \t]+((?:SET|SIGNAL|RESIGNAL|LEAVE|ITERATE|CALL|INSERT|INS|UPDATE|UPD|DELETE|DEL)\b.*)$", txt)
            if hm:
                stmt = set_assign(hm.group(3))
                stmt = re.sub(r"(?i)^(LEAVE|ITERATE)\b.*", "NULL", stmt)
                add_handler(hm.group(1), hm.group(2), stmt)
                return ""
            cm_ = re.match(r"(?is)(\w+)[ \t]+CURSOR(?:[ \t]+WITH[ \t]+RETURN(?:[ \t]+(?:ONLY|TO[ \t]+(?:CALLER|CLIENT)))?)?(?:[ \t]+FOR)?[ \t]+((?:SEL|SELECT|WITH)\b.*?)(?:[ \t]+FOR[ \t]+(?:READ[ \t]+ONLY|UPDATE))?$", txt)
            if cm_:
                q = re.sub(r"(?i)^SEL\b", "SELECT", cm_.group(2))
                ret = bool(re.search(r"(?i)\bWITH[ \t]+RETURN\b", txt[:cm_.start(2)]))
                decls.append(f"CURSOR {cm_.group(1)} {'WITH_RETURN ' if ret else ''}IS {q};")
                return ""
            if re.match(r"(?i)(CONTINUE|EXIT|UNDO)[ \t]+HANDLER|CURSOR\b|\w+[ \t]+CONDITION|\w+[ \t]+CURSOR\b", txt):
                report.todo("proc", "SPL " + txt.split()[0].lower() + " declaration (handler/cursor/condition) is not recognised: handle errors with try/except by hand")
                return "-- TODO: " + txt[:80].replace("\n", " ") + ";"
            dm2 = re.fullmatch(r"(?is)(\w+(?:[ \t]*,[ \t]*\w+)*)[ \t]+(.+?)(?:[ \t]+DEFAULT[ \t]+(.+))?", txt)
            if not dm2:
                return dm.group(0)
            for v in re.split(r"[ \t]*,[ \t]*", dm2.group(1)):
                decls.append(f"{v} {dm2.group(2)}" + (f" := {dm2.group(3)}" if dm2.group(3) else "") + ";")
            return ""
        body = re.sub(r"(?is)\bDECLARE[ \t]+([^;]*);", hoist, body)
        body = re.sub(r"(?is)\bFOR[ \t\r\n]+(\w+)[ \t\r\n]+AS[ \t\r\n]+(?:\w+[ \t\r\n]+CURSOR[ \t\r\n]+FOR[ \t\r\n]+)?(.*?)[ \t\r\n]+DO\b",
                      lambda fm: f"FOR {fm.group(1)} IN ({re.sub(r'(?i)^SEL\b', 'SELECT', fm.group(2).strip())}) LOOP", body)
        body = re.sub(r"(?i)\bEND[ \t]+FOR\b", "END LOOP", body)
        body = re.sub(r"(?im)^([ \t]*)SET[ \t]+(\w+)[ \t]*=[ \t]*", r"\1\2 := ", body)
        for ab, full in (("SEL", "SELECT"), ("INS", "INSERT"), ("UPD", "UPDATE"), ("DEL", "DELETE")):
            body = re.sub(rf"(?i)(;|\bTHEN|\bELSE|\bDO|\bLOOP|^)([ \t\r\n]*){ab}\b", rf"\1\2{full}", body)
        body = re.sub(r"(?i)\bELSEIF\b", "ELSIF", body)
        body = re.sub(r"(?i)\bEND[ \t]+WHILE\b", "END LOOP", body)
        body = re.sub(r"(?i)\bEND[ \t]+REPEAT\b", "END LOOP", body)
        body = re.sub(r"(?i)\bWHILE\b([^;]*?)\bDO\b", r"WHILE \1 LOOP", body)
        body = re.sub(r"(?im)^([ \t]*)LEAVE\b[^;]*;", r"\1EXIT;", body)
        if re.search(r"(?i)\bREPEAT\b|\bCALL[ \t]+DBC\.", body):
            report.todo("proc", "SPL REPEAT / DBC.* calls are not translated automatically")
        out.append(source[pos:m.start()])
        out.append(f"CREATE OR REPLACE PROCEDURE {m.group(1)} ({', '.join(params)}) AS\n" + "\n".join(decls) + "\nBEGIN" + body + "\nEND;\n")
        pos = end
    out.append(source[pos:])
    return "".join(out)


def convert_proc(source: str, report: Report, dialect: str = "auto") -> str:
    if dialect == "teradata" or (dialect in ("auto", "generic") and looks_like_spl(source)):
        source = spl_to_plsql(source, report)
        dialect = "teradata"
    toks = tokenize(source)
    if dialect in ("auto", "generic"):
        dialect = "oracle" if re.search(r"(?i):=|\bPLS_INTEGER\b|\bVARCHAR2\b|\bNUMBER\b|\bDBMS_OUTPUT\b", source) else "tsql"
    report.note(f"stored procedure dialect: {dialect}")
    parser = (PLSQL if dialect in ("oracle", "plsql", "teradata") else TSQL)(source, toks, report)
    routines = parser.routines()
    if dialect == "teradata":
        for rt in routines:
            rt.dialect = "teradata"
    if not routines:
        report.todo("file", "no CREATE PROCEDURE / FUNCTION found")
        return ""
    funcs, helpers = [], set()
    for rt in routines:
        em = Emitter(rt, report)
        funcs.append(em.emit())
        helpers |= em.helpers
    head = ["from pyspark.sql import SparkSession, functions as F", ""]
    if "datetime" in helpers:
        head.insert(0, "from datetime import datetime")
    if "coalesce" in helpers:
        head += ["", "def _coalesce(a, b):", "    return a if a is not None else b"]
    if "todo" in helpers:
        head += ["", "def _todo(msg):", "    raise NotImplementedError('TODO(sparqix): ' + msg)"]
    report.note("variables reach SQL as named parameters (spark.sql(..., args={...}), Spark 3.4+): they work for values, not for table/column names")
    code = "\n".join(head) + "\n\n\n" + "\n\n\n".join(funcs) + "\n"
    try:
        ast.parse(code)
    except SyntaxError as e:
        report.todo("file", f"generated code failed to parse ({e.msg} at line {e.lineno}): review the output")
    return code
