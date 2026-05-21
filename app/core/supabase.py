import os
from pathlib import Path
import asyncio
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from sqlalchemy import and_, delete, insert, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.db import SessionLocal, tables

env_path = Path(__file__).parent.parent.parent / ".env"
root_env_path = env_path.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv(dotenv_path=root_env_path, override=False)

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip() or None
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip() or None
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip() or None


class _DBResponse:
    def __init__(self, data: Any):
        self.data = data


def _parse_value(raw: str) -> Any:
    v = raw
    if v.lower() == "null":
        return None
    try:
        if "." in v:
            return float(v)
        return int(v)
    except Exception:
        return v


def _split_csv(raw: str) -> list[str]:
    s = str(raw or "").strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def _parse_or_expr(expr: str, table) -> Any:
    s = str(expr or "").strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            continue
        buf.append(ch)
    last = "".join(buf).strip()
    if last:
        parts.append(last)

    conds: list[Any] = []
    for part in parts:
        if ".eq." in part:
            col, val = part.split(".eq.", 1)
            conds.append(table.c[col] == _parse_value(val))
            continue
        if ".neq." in part:
            col, val = part.split(".neq.", 1)
            conds.append(table.c[col] != _parse_value(val))
            continue
        if ".gte." in part:
            col, val = part.split(".gte.", 1)
            conds.append(table.c[col] >= _parse_value(val))
            continue
        if ".gt." in part:
            col, val = part.split(".gt.", 1)
            conds.append(table.c[col] > _parse_value(val))
            continue
        if ".lte." in part:
            col, val = part.split(".lte.", 1)
            conds.append(table.c[col] <= _parse_value(val))
            continue
        if ".lt." in part:
            col, val = part.split(".lt.", 1)
            conds.append(table.c[col] < _parse_value(val))
            continue
        if ".ilike." in part:
            col, val = part.split(".ilike.", 1)
            conds.append(table.c[col].ilike(str(val)))
            continue
        if ".in.(" in part and part.endswith(")"):
            col, rest = part.split(".in.(", 1)
            raw_vals = rest[:-1]
            vals = [_parse_value(x) for x in _split_csv(raw_vals)]
            conds.append(table.c[col].in_(vals))
            continue

    if not conds:
        return None
    if len(conds) == 1:
        return conds[0]
    return or_(*conds)


class _DBQuery:
    def __init__(self, table_name: str):
        if table_name not in tables:
            raise RuntimeError(f"Table not found: {table_name}")
        self._table = tables[table_name]
        self._method: str = "GET"
        self._select_cols: Optional[list[str]] = None
        self._filters: list[Any] = []
        self._payload: Any = None
        self._single: bool = False
        self._maybe_single: bool = False
        self._order: list[Any] = []
        self._limit: Optional[int] = None
        self._on_conflict: Optional[str] = None

    def select(self, cols: str):
        self._method = "GET"
        if cols.strip() == "*" or ":" in cols or "(" in cols:
            self._select_cols = None
        else:
            self._select_cols = _split_csv(cols)
        return self

    def eq(self, col: str, val: Any):
        self._filters.append(self._table.c[col] == val)
        return self

    def neq(self, col: str, val: Any):
        self._filters.append(self._table.c[col] != val)
        return self

    def gte(self, col: str, val: Any):
        self._filters.append(self._table.c[col] >= val)
        return self

    def gt(self, col: str, val: Any):
        self._filters.append(self._table.c[col] > val)
        return self

    def lte(self, col: str, val: Any):
        self._filters.append(self._table.c[col] <= val)
        return self

    def lt(self, col: str, val: Any):
        self._filters.append(self._table.c[col] < val)
        return self

    def ilike(self, col: str, pattern: str):
        self._filters.append(self._table.c[col].ilike(pattern))
        return self

    def in_(self, col: str, values: List[Any]):
        self._filters.append(self._table.c[col].in_(values))
        return self

    def or_(self, expr: str):
        cond = _parse_or_expr(expr, self._table)
        if cond is not None:
            self._filters.append(cond)
        return self

    def order(self, col: str, desc: bool = False, nullsfirst: bool | None = None):
        c = self._table.c[col]
        o = c.desc() if desc else c.asc()
        if nullsfirst is True:
            o = o.nullsfirst()
        elif nullsfirst is False:
            o = o.nullslast()
        self._order.append(o)
        return self

    def limit(self, n: int):
        self._limit = int(n)
        return self

    def single(self):
        self._single = True
        self._maybe_single = False
        return self

    def maybe_single(self):
        self._maybe_single = True
        self._single = False
        return self

    def insert(self, payload: Any):
        self._method = "POST"
        self._payload = payload
        return self

    def update(self, payload: Any):
        self._method = "PATCH"
        self._payload = payload
        return self

    def delete(self):
        self._method = "DELETE"
        return self

    def upsert(self, payload: Any, on_conflict: str | None = None):
        self._method = "UPSERT"
        self._payload = payload
        self._on_conflict = on_conflict
        return self

    async def execute_async(self) -> _DBResponse:
        return await self._execute_async()

    def execute(self) -> _DBResponse:
        try:
            asyncio.get_running_loop()
            raise RuntimeError("Use await query.execute_async() for DB queries inside async context")
        except RuntimeError as e:
            if "no running event loop" not in str(e).lower():
                raise
        return asyncio.run(self._execute_async())

    async def _execute_async(self) -> _DBResponse:
        async with SessionLocal() as session:
            if self._method == "GET":
                cols = [self._table.c[c] for c in self._select_cols] if self._select_cols else [self._table]
                stmt = select(*cols)
                if self._filters:
                    stmt = stmt.where(and_(*self._filters))
                if self._order:
                    stmt = stmt.order_by(*self._order)
                if self._limit is not None:
                    stmt = stmt.limit(self._limit)
                res = await session.execute(stmt)
                rows = res.mappings().all()
                data = [dict(r) for r in rows]
                if self._single or self._maybe_single:
                    return _DBResponse(data[0] if data else None)
                return _DBResponse(data)

            if self._method == "POST":
                stmt = insert(self._table).values(self._payload).returning(self._table)
                res = await session.execute(stmt)
                await session.commit()
                data = [dict(r) for r in res.mappings().all()]
                return _DBResponse(data)

            if self._method == "PATCH":
                stmt = update(self._table).values(self._payload)
                if self._filters:
                    stmt = stmt.where(and_(*self._filters))
                stmt = stmt.returning(self._table)
                res = await session.execute(stmt)
                await session.commit()
                data = [dict(r) for r in res.mappings().all()]
                return _DBResponse(data)

            if self._method == "DELETE":
                stmt = delete(self._table)
                if self._filters:
                    stmt = stmt.where(and_(*self._filters))
                stmt = stmt.returning(self._table)
                res = await session.execute(stmt)
                await session.commit()
                data = [dict(r) for r in res.mappings().all()]
                return _DBResponse(data)

            if self._method == "UPSERT":
                conflict = self._on_conflict or ""
                cols = _split_csv(conflict)
                if not cols:
                    raise RuntimeError("on_conflict must be set for upsert")
                payload = self._payload
                stmt = pg_insert(self._table).values(payload)
                excluded = stmt.excluded
                set_map = {c.name: getattr(excluded, c.name) for c in self._table.c if c.name not in cols}
                stmt = stmt.on_conflict_do_update(index_elements=[self._table.c[c] for c in cols], set_=set_map).returning(self._table)
                res = await session.execute(stmt)
                await session.commit()
                data = [dict(r) for r in res.mappings().all()]
                return _DBResponse(data)

            raise RuntimeError(f"Unsupported method: {self._method}")


class _SupabaseCompat:
    def table(self, name: str) -> _DBQuery:
        return _DBQuery(name)

    def from_(self, name: str) -> _DBQuery:
        return self.table(name)


supabase = _SupabaseCompat()
admin_supabase = _SupabaseCompat()
