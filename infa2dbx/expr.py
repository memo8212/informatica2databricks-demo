"""Translate Informatica PowerCenter expressions to PySpark column expressions.

Canonical implementation used by the `infa2dbx` app and the `converter` package.

    UPPER(DEPT_CODE)                  -> F.upper(F.col("DEPT_CODE"))
    ROUND(AMOUNT * FX_RATE, 2)        -> F.round((F.col("AMOUNT") * F.col("FX_RATE")), 2)
    IIF(STATUS = 'REVERSAL', 'Y','N') -> F.when(F.col("STATUS") == "REVERSAL", "Y").otherwise("N")
    IS_REV = 'N' AND AMOUNT_BASE <> 0 -> (F.col("IS_REV") == "N") & (F.col("AMOUNT_BASE") != 0)
    DECODE(x, 'A',1,'B',2, 0)         -> nested when/otherwise
"""
from __future__ import annotations

import re

_SIMPLE = {
    "UPPER": "F.upper", "LOWER": "F.lower", "LTRIM": "F.ltrim", "RTRIM": "F.rtrim",
    "TRIM": "F.trim", "ABS": "F.abs", "ROUND": "F.round", "SUM": "F.sum",
    "AVG": "F.avg", "MIN": "F.min", "MAX": "F.max", "COUNT": "F.count",
    "SUBSTR": "F.substring", "LENGTH": "F.length", "INSTR": "F.instr",
    "COALESCE": "F.coalesce", "NVL": "F.coalesce", "CONCAT": "F.concat",
    "REPLACE": "F.regexp_replace", "LPAD": "F.lpad", "RPAD": "F.rpad",
    "SQRT": "F.sqrt", "POWER": "F.pow", "MOD": "F.expr", "CEIL": "F.ceil",
    "FLOOR": "F.floor", "GREATEST": "F.greatest", "LEAST": "F.least",
}

TOKEN_RE = re.compile(r"""
    '(?:[^']*)'                     |
    \d+\.\d+ | \d+                  |
    [A-Za-z_][A-Za-z0-9_]*          |
    <>|<=|>=|!=|==|=|<|>|\|\||[+\-*/(),]
""", re.VERBOSE)


def _tokenize(s: str) -> list[str]:
    return TOKEN_RE.findall(s)


class _P:
    def __init__(self, toks):
        self.t = toks
        self.i = 0

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else None

    def next(self):
        v = self.peek()
        self.i += 1
        return v

    def expr(self):
        return self.or_()

    def or_(self):
        node = self.and_()
        while (self.peek() or "").upper() == "OR":
            self.next()
            node = f"({node} | {self.and_()})"
        return node

    def and_(self):
        node = self.not_()
        while (self.peek() or "").upper() == "AND":
            self.next()
            node = f"({node} & {self.not_()})"
        return node

    def not_(self):
        if (self.peek() or "").upper() == "NOT":
            self.next()
            return f"(~{self.not_()})"
        return self.cmp()

    def cmp(self):
        node = self.add()
        op = self.peek()
        if op in ("=", "==", "<>", "!=", "<", ">", "<=", ">="):
            self.next()
            r = self.add()
            sop = {"=": "==", "==": "==", "<>": "!=", "!=": "!="}.get(op, op)
            return f"({node} {sop} {r})"
        return node

    def add(self):
        node = self.mul()
        while self.peek() in ("+", "-", "||"):
            op = self.next()
            r = self.mul()
            node = f"F.concat({node}, {r})" if op == "||" else f"({node} {op} {r})"
        return node

    def mul(self):
        node = self.unary()
        while self.peek() in ("*", "/"):
            op = self.next()
            node = f"({node} {op} {self.unary()})"
        return node

    def unary(self):
        if self.peek() == "-":
            self.next()
            return f"(-{self.unary()})"
        return self.primary()

    def primary(self):
        tok = self.next()
        if tok == "(":
            node = self.expr()
            if self.peek() == ")":
                self.next()
            return f"({node})"
        if tok and tok[0] == "'":
            return '"' + tok[1:-1] + '"'
        if re.fullmatch(r"\d+(\.\d+)?", tok or ""):
            return tok
        up = (tok or "").upper()
        if self.peek() == "(":
            self.next()
            args = []
            if self.peek() != ")":
                args.append(self.expr())
                while self.peek() == ",":
                    self.next()
                    args.append(self.expr())
            if self.peek() == ")":
                self.next()
            return _render_func(up, args)
        if up == "NULL":
            return "F.lit(None)"
        if up in ("TRUE", "FALSE"):
            return f"F.lit({up.title()})"
        return f'F.col("{tok}")'


def _render_func(name: str, args: list[str]) -> str:
    if name == "IIF":
        return f"F.when({args[0]}, {args[1]}).otherwise({args[2]})"
    if name == "DECODE":
        subject, rest = args[0], args[1:]
        pairs, default = rest[:-1], rest[-1]
        expr = None
        for k, v in zip(pairs[0::2], pairs[1::2]):
            expr = f"F.when({subject} == {k}, {v})" if expr is None else f"{expr}.when({subject} == {k}, {v})"
        return f"{expr}.otherwise({default})"
    if name == "SYSDATE":
        return "F.current_date()"
    if name == "ISNULL":
        return f"({args[0]}.isNull())"
    if name == "ADD_TO_DATE":
        return f"F.date_add({args[0]}, {args[2]})"
    if name == "TRUNC":
        return f"F.date_trunc('day', {args[0]})"
    if name == "TO_DECIMAL":
        return f"F.round({args[0]}, {args[2]})"
    if name == "TO_DATE":
        return f"F.to_date({args[0]})"
    if name == "TO_CHAR":
        return f"F.date_format({args[0]}, {args[1]})"
    if name == "REG_EXTRACT":
        return f"F.regexp_extract({args[0]}, {args[1]}, {args[2] if len(args) > 2 else '1'})"
    if name == "REG_MATCH":
        return f"({args[0]}.rlike({args[1]}))"
    if name == "IN":
        return f"({args[0]}.isin({', '.join(args[1:])}))"
    if name in _SIMPLE:
        return f"{_SIMPLE[name]}({', '.join(args)})"
    return f"F.{name.lower()}({', '.join(args)})"


def translate_expression(expr: str) -> str:
    expr = (expr or "").strip()
    if not expr:
        return ""
    return _P(_tokenize(expr)).expr()


UNSUPPORTED = ("ABORT", "ERROR", "SESSTARTTIME", "WORKFLOWVAR")
