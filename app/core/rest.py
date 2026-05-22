from typing import Any, Dict, Optional

from sqlalchemy import and_, delete, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.db import SessionLocal, tables


class DBResponse:
    def __init__(self, status_code: int, data: Any = None, text: str = ""):
        self.status_code = status_code
        self._data = data
        self.text = text

    def json(self) -> Any:
        return self._data


def _parse_param(table, key: str, raw: Any):
    s = str(raw)
    if s.startswith("eq."):
        return table.c[key] == s[3:]
    if s.startswith("neq."):
        return table.c[key] != s[4:]
    if s.startswith("gte."):
        # Cast to datetime if the column is a datetime type to avoid asyncpg type errors
        val = s[4:]
        if "time" in str(table.c[key].type).lower():
            try:
                from datetime import datetime
                val = datetime.fromisoformat(val.replace("Z", "+00:00"))
            except:
                pass
        return table.c[key] >= val
    if s.startswith("gt."):
        val = s[3:]
        if "time" in str(table.c[key].type).lower():
            try:
                from datetime import datetime
                val = datetime.fromisoformat(val.replace("Z", "+00:00"))
            except:
                pass
        return table.c[key] > val
    if s.startswith("lte."):
        val = s[4:]
        if "time" in str(table.c[key].type).lower():
            try:
                from datetime import datetime
                val = datetime.fromisoformat(val.replace("Z", "+00:00"))
            except:
                pass
        return table.c[key] <= val
    if s.startswith("lt."):
        val = s[3:]
        if "time" in str(table.c[key].type).lower():
            try:
                from datetime import datetime
                val = datetime.fromisoformat(val.replace("Z", "+00:00"))
            except:
                pass
        return table.c[key] < val
    if s.startswith("ilike."):
        return table.c[key].ilike(s[6:])
    if s.startswith("in.(") and s.endswith(")"):
        vals = [v.strip() for v in s[4:-1].split(",") if v.strip()]
        return table.c[key].in_(vals)
    return None


def _split_csv(raw: str) -> list[str]:
    s = str(raw or "").strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


async def rest_get(path: str, params: Dict[str, Any], *, write: bool = False) -> DBResponse:
    table_name = path.strip().lstrip("/").split("/", 1)[0]
    if table_name not in tables:
        return DBResponse(404, [], f"Table not found: {table_name}")
    table = tables[table_name]

    select_raw = str(params.get("select") or "*")
    cols = None
    if select_raw != "*" and ":" not in select_raw and "(" not in select_raw:
        cols = [table.c[c] for c in _split_csv(select_raw)]

    stmt = select(*(cols or [table]))
    where_parts = []
    order_raw = params.get("order")
    limit_raw = params.get("limit")

    for k, v in params.items():
        if k in ("select", "order", "limit", "on_conflict"):
            continue
        if k not in table.c:
            continue
        cond = _parse_param(table, k, v)
        if cond is not None:
            where_parts.append(cond)

    if where_parts:
        stmt = stmt.where(and_(*where_parts))

    if order_raw:
        for part in _split_csv(str(order_raw)):
            col, dir_ = part.split(".", 1) if "." in part else (part, "asc")
            if col in table.c:
                stmt = stmt.order_by(table.c[col].desc() if dir_ == "desc" else table.c[col].asc())

    if limit_raw:
        try:
            stmt = stmt.limit(int(limit_raw))
        except Exception:
            pass

    async with SessionLocal() as session:
        res = await session.execute(stmt)
        rows = [dict(r) for r in res.mappings().all()]

        if table_name == "competitions" and ("categories:competition_categories" in select_raw):
            ids = [str(r.get("id")) for r in rows if r.get("id")]
            cats_table = tables.get("competition_categories")
            if ids and cats_table is not None:
                cats_res = await session.execute(select(cats_table).where(cats_table.c.competition_id.in_(ids)))
                cats = [dict(r) for r in cats_res.mappings().all()]
                by_comp: dict[str, list[dict]] = {}
                for c in cats:
                    by_comp.setdefault(str(c.get("competition_id")), []).append(c)
                for r in rows:
                    r["categories"] = by_comp.get(str(r.get("id")), [])

        if table_name == "competitions" and ("locations(" in select_raw):
            loc_table = tables.get("locations")
            if loc_table is not None:
                loc_ids = [str(r.get("location_id")) for r in rows if r.get("location_id")]
                if loc_ids:
                    loc_res = await session.execute(select(loc_table).where(loc_table.c.id.in_(loc_ids)))
                    locs = {str(r.get("id")): dict(r) for r in loc_res.mappings().all()}
                    for r in rows:
                        lid = str(r.get("location_id")) if r.get("location_id") else None
                        if lid and lid in locs:
                            r["locations"] = {"name": locs[lid].get("name")}

    return DBResponse(200, rows, "")


async def rest_post(path: str, params: Dict[str, Any], json: Any, *, prefer: Optional[str] = None) -> DBResponse:
    table_name = path.strip().lstrip("/").split("/", 1)[0]
    if table_name not in tables:
        return DBResponse(404, [], f"Table not found: {table_name}")
    table = tables[table_name]
    async with SessionLocal() as session:
        stmt = insert(table).values(json).returning(table)
        res = await session.execute(stmt)
        await session.commit()
        rows = [dict(r) for r in res.mappings().all()]
        return DBResponse(201, rows, "")


async def rest_delete(path: str, params: Dict[str, Any]) -> DBResponse:
    table_name = path.strip().lstrip("/").split("/", 1)[0]
    if table_name not in tables:
        return DBResponse(404, [], f"Table not found: {table_name}")
    table = tables[table_name]
    where_parts = []
    for k, v in params.items():
        if k not in table.c:
            continue
        cond = _parse_param(table, k, v)
        if cond is not None:
            where_parts.append(cond)
    stmt = delete(table)
    if where_parts:
        stmt = stmt.where(and_(*where_parts))
    stmt = stmt.returning(table)
    async with SessionLocal() as session:
        res = await session.execute(stmt)
        await session.commit()
        rows = [dict(r) for r in res.mappings().all()]
        return DBResponse(200, rows, "")


async def rest_patch(path: str, params: Dict[str, Any], json: Any, *, prefer: Optional[str] = None) -> DBResponse:
    table_name = path.strip().lstrip("/").split("/", 1)[0]
    if table_name not in tables:
        return DBResponse(404, [], f"Table not found: {table_name}")
    table = tables[table_name]
    where_parts = []
    for k, v in params.items():
        if k not in table.c:
            continue
        cond = _parse_param(table, k, v)
        if cond is not None:
            where_parts.append(cond)
    stmt = update(table).values(json)
    if where_parts:
        stmt = stmt.where(and_(*where_parts))
    stmt = stmt.returning(table)
    async with SessionLocal() as session:
        res = await session.execute(stmt)
        await session.commit()
        rows = [dict(r) for r in res.mappings().all()]
        return DBResponse(200, rows, "")


async def rest_upsert(table_name: str, payload: Dict[str, Any], *, on_conflict: Optional[str] = None) -> None:
    if table_name not in tables:
        raise RuntimeError(f"Table not found: {table_name}")
    table = tables[table_name]
    conflict_cols = _split_csv(on_conflict or "")
    if not conflict_cols:
        raise RuntimeError("on_conflict must be provided")
    stmt = pg_insert(table).values(payload)
    excluded = stmt.excluded
    set_map = {c.name: getattr(excluded, c.name) for c in table.c if c.name not in conflict_cols}
    stmt = stmt.on_conflict_do_update(index_elements=[table.c[c] for c in conflict_cols], set_=set_map)
    async with SessionLocal() as session:
        await session.execute(stmt)
        await session.commit()
