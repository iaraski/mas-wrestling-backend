from sqlalchemy import select as _select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import SessionLocal, tables


async def get_role_codes(
    user_id: str,
    *,
    session: AsyncSession | None = None,
) -> list[str]:
    user_roles_t = tables.get("user_roles")
    roles_t = tables.get("roles")
    if user_roles_t is None or roles_t is None:
        return []

    stmt = (
        _select(roles_t.c.code)
        .select_from(user_roles_t.join(roles_t, roles_t.c.id == user_roles_t.c.role_id))
        .where(user_roles_t.c.user_id == str(user_id))
        .limit(1000)
    )

    if session is not None:
        res = await session.execute(stmt)
        rows = res.mappings().all()
    else:
        async with SessionLocal() as own_session:
            res = await own_session.execute(stmt)
            rows = res.mappings().all()
    return [str(r.get("code")) for r in rows if r.get("code")]
