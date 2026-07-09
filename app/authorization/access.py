from collections.abc import Iterable

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.roles import get_role_codes


STAFF_ROLE_CODES = frozenset(
    {
        "admin",
        "founder",
        "country_admin",
        "region_admin",
        "secretary",
        "country_secretary",
        "region_secretary",
    }
)


def is_staff_role(codes: Iterable[str]) -> bool:
    return any(str(code) in STAFF_ROLE_CODES for code in codes)


async def get_role_codes_safe(
    user_id: str,
    *,
    session: AsyncSession | None = None,
) -> list[str]:
    try:
        return await get_role_codes(user_id, session=session)
    except Exception:
        return []
