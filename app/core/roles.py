from app.core.rest import rest_get


async def get_role_codes(user_id: str) -> list[str]:
    resp = await rest_get(
        "user_roles",
        {"select": "role_id", "user_id": f"eq.{str(user_id)}", "limit": "1000"},
        write=True,
    )
    rows = resp.json()
    if not isinstance(rows, list):
        return []
    role_ids = [str(r.get("role_id")) for r in rows if isinstance(r, dict) and r.get("role_id")]
    if not role_ids:
        return []
    ids_expr = f"in.({','.join(role_ids)})"
    r2 = await rest_get("roles", {"select": "code,id", "id": ids_expr, "limit": "1000"}, write=True)
    roles = r2.json()
    if not isinstance(roles, list):
        return []
    return [str(r.get("code")) for r in roles if isinstance(r, dict) and r.get("code")]

