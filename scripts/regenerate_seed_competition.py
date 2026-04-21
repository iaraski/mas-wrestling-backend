from __future__ import annotations

from collections import defaultdict
from typing import Iterable
from uuid import UUID, uuid4

import anyio

from app.core.supabase import admin_supabase
from app.routers.live import GenerateLiveBoutsRequest, generate_live_bouts

DESIRED_CATEGORY_SIZES = [3, 6, 7, 12, 15]


def _chunked(xs: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(xs), size):
        yield xs[i : i + size]


def _get_category_ids(comp_id: str) -> list[str]:
    rows = (
        admin_supabase.table("competition_categories")
        .select("id,weight_min")
        .eq("competition_id", comp_id)
        .order("weight_min", desc=False)
        .limit(20000)
        .execute()
        .data
        or []
    )
    return [str(r["id"]) for r in rows if r.get("id")]


def _get_category_bounds(comp_id: str) -> dict[str, tuple[float, float]]:
    rows = (
        admin_supabase.table("competition_categories")
        .select("id,weight_min,weight_max")
        .eq("competition_id", comp_id)
        .limit(20000)
        .execute()
        .data
        or []
    )
    out: dict[str, tuple[float, float]] = {}
    for r in rows:
        cid = r.get("id")
        if not cid:
            continue
        min_w = float(r.get("weight_min") or 0.0)
        max_w = float(r.get("weight_max") or 0.0)
        if max_w >= 999:
            max_w = min_w + 5.0
        out[str(cid)] = (min_w, max_w)
    return out


def _weights_for_category(min_w: float, max_w: float, n: int, seed: int) -> list[tuple[float, float]]:
    if max_w <= min_w:
        max_w = min_w + max(0.5, float(n) * 0.2)
    if max_w >= 999:
        max_w = min_w + max(0.5, float(n) * 0.2)
    eps = 0.01
    span = max_w - min_w
    usable = max(span - 2 * eps, eps * (n + 1))
    step = usable / (n + 1)
    actuals = [round(min_w + eps + step * (i + 1), 2) for i in range(n)]
    actuals = [min(w, round(max_w - eps, 2)) for w in actuals]
    offset = seed % max(1, n)
    seen: set[float] = set()
    out: list[tuple[float, float]] = []
    for i in range(n):
        w = actuals[(i + offset) % n]
        ww = w
        while ww in seen and ww + 0.01 <= max_w - eps:
            ww = round(ww + 0.01, 2)
        if ww in seen:
            ww = round(min_w + eps + i * 0.01, 2)
        seen.add(ww)
        declared = max(round(min_w + eps, 2), round(ww - 0.05, 2))
        out.append((declared, ww))
    return out


def _get_region_ids(limit: int = 200) -> list[str]:
    rows = (
        admin_supabase.table("locations")
        .select("id")
        .eq("type", "region")
        .limit(int(limit))
        .execute()
        .data
        or []
    )
    ids = [str(r["id"]) for r in rows if r.get("id")]
    ids = list(dict.fromkeys([x for x in ids if x]))
    return ids


def _ensure_min_categories(comp_id: str, desired_count: int) -> None:
    rows = (
        admin_supabase.table("competition_categories")
        .select("id,competition_id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day")
        .eq("competition_id", comp_id)
        .order("weight_min", desc=False)
        .limit(20000)
        .execute()
        .data
        or []
    )
    existing = [r for r in rows if r.get("id")]
    if len(existing) >= desired_count:
        return
    tmpl = existing[0] if existing else None
    if not tmpl:
        raise SystemExit("no categories to clone from")

    max_w = 0.0
    for r in existing:
        wmax = float(r.get("weight_max") or 0.0)
        wmin = float(r.get("weight_min") or 0.0)
        if wmax >= 999:
            max_w = max(max_w, wmin)
        else:
            max_w = max(max_w, wmax)

    to_create = desired_count - len(existing)
    new_rows: list[dict] = []
    wmin = round(max_w + 0.01, 2)
    for i in range(to_create):
        wmax = round(wmin + 10.0, 2)
        new_rows.append(
            {
                "id": str(uuid4()),
                "competition_id": comp_id,
                "gender": tmpl.get("gender"),
                "age_min": tmpl.get("age_min"),
                "age_max": tmpl.get("age_max"),
                "weight_min": wmin,
                "weight_max": wmax,
                "competition_day": tmpl.get("competition_day"),
                "mandate_day": tmpl.get("mandate_day"),
            }
        )
        wmin = round(wmax + 0.01, 2)

    for chunk in (new_rows[i : i + 200] for i in range(0, len(new_rows), 200)):
        admin_supabase.table("competition_categories").insert(chunk).execute()


def _reset_categories_exact(comp_id: str, desired_count: int) -> None:
    rows = (
        admin_supabase.table("competition_categories")
        .select("id,competition_id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day")
        .eq("competition_id", comp_id)
        .order("weight_min", desc=False)
        .limit(20000)
        .execute()
        .data
        or []
    )
    existing = [r for r in rows if r.get("id")]
    tmpl = existing[0] if existing else None
    if not tmpl:
        raise SystemExit("no categories to clone from")

    admin_supabase.table("competition_categories").delete().eq("competition_id", comp_id).execute()

    wmin = float(tmpl.get("weight_min") or 1.0)
    if wmin <= 0:
        wmin = 1.0

    new_rows: list[dict] = []
    for _ in range(int(desired_count)):
        wmax = round(wmin + 10.0, 2)
        new_rows.append(
            {
                "id": str(uuid4()),
                "competition_id": comp_id,
                "gender": tmpl.get("gender"),
                "age_min": tmpl.get("age_min"),
                "age_max": tmpl.get("age_max"),
                "weight_min": round(wmin, 2),
                "weight_max": wmax,
                "competition_day": tmpl.get("competition_day"),
                "mandate_day": tmpl.get("mandate_day"),
            }
        )
        wmin = round(wmax + 0.01, 2)

    for chunk in (new_rows[i : i + 200] for i in range(0, len(new_rows), 200)):
        admin_supabase.table("competition_categories").insert(chunk).execute()


def _get_target_counts(comp_id: str, cat_ids: list[str]) -> dict[str, int]:
    canonical = DESIRED_CATEGORY_SIZES
    counts: dict[str, int] = defaultdict(int)
    for cid, cnt in zip(cat_ids, canonical):
        counts[cid] = int(cnt)
    return counts


def _collect_user_ids_for_comp(comp_id: str) -> tuple[list[str], list[str]]:
    apps = (
        admin_supabase.table("applications")
        .select("athlete_id")
        .eq("competition_id", comp_id)
        .limit(200000)
        .execute()
        .data
        or []
    )
    athlete_ids = list({str(r["athlete_id"]) for r in apps if r.get("athlete_id")})
    if not athlete_ids:
        return [], []

    athletes = (
        admin_supabase.table("athletes")
        .select("id,user_id")
        .in_("id", athlete_ids)
        .limit(200000)
        .execute()
        .data
        or []
    )
    user_ids = list({str(r["user_id"]) for r in athletes if r.get("user_id")})
    return athlete_ids, user_ids


def _reset_comp(comp_id: str) -> None:
    admin_supabase.table("competition_mats").update({"current_bout_id": None}).eq("competition_id", comp_id).execute()
    admin_supabase.table("competition_category_assignments").delete().eq("competition_id", comp_id).execute()
    admin_supabase.table("competition_bouts").delete().eq("competition_id", comp_id).execute()
    admin_supabase.table("applications").delete().eq("competition_id", comp_id).execute()


def _delete_users(athlete_ids: list[str], user_ids: list[str]) -> None:
    for ch in _chunked(athlete_ids, 200):
        admin_supabase.table("athletes").delete().in_("id", ch).execute()
    for ch in _chunked(user_ids, 200):
        admin_supabase.table("profiles").delete().in_("user_id", ch).execute()
        admin_supabase.table("users").delete().in_("id", ch).execute()


def _seed_apps(
    comp_id: str,
    cat_ids: list[str],
    counts: dict[str, int],
    bounds: dict[str, tuple[float, float]],
    region_ids: list[str],
) -> int:
    users_rows: list[dict] = []
    profiles_rows: list[dict] = []
    athletes_rows: list[dict] = []
    apps_rows: list[dict] = []

    draw = 1
    region_count = len(region_ids)
    for cid in cat_ids:
        cnt = int(counts.get(cid) or 0)
        if cnt <= 0:
            continue
        min_w, max_w = bounds.get(cid, (0.0, 0.0))
        weights = _weights_for_category(min_w, max_w, cnt, draw)
        for _ in range(cnt):
            user_id = str(uuid4())
            athlete_id = str(uuid4())
            users_rows.append({"id": user_id, "email": f"seed_{comp_id[:8]}_{cid[:8]}_{uuid4().hex[:8]}@example.com"})
            profile = {"user_id": user_id, "full_name": f"Тестовый Спортсмен {draw}"}
            if region_count:
                profile["location_id"] = region_ids[draw % region_count]
            profiles_rows.append(profile)
            athletes_rows.append({"id": athlete_id, "user_id": user_id, "coach_name": "Тестовый тренер"})
            declared_w, actual_w = weights.pop(0)
            apps_rows.append(
                {
                    "competition_id": comp_id,
                    "athlete_id": athlete_id,
                    "category_id": cid,
                    "status": "weighed",
                    "declared_weight": declared_w,
                    "actual_weight": actual_w,
                    "draw_number": draw,
                }
            )
            draw += 1

    for chunk in (users_rows[i : i + 200] for i in range(0, len(users_rows), 200)):
        admin_supabase.table("users").insert(chunk).execute()
    for chunk in (profiles_rows[i : i + 200] for i in range(0, len(profiles_rows), 200)):
        admin_supabase.table("profiles").insert(chunk).execute()
    for chunk in (athletes_rows[i : i + 200] for i in range(0, len(athletes_rows), 200)):
        admin_supabase.table("athletes").insert(chunk).execute()
    for chunk in (apps_rows[i : i + 200] for i in range(0, len(apps_rows), 200)):
        admin_supabase.table("applications").insert(chunk).execute()

    return len(apps_rows)


async def _generate(comp_id: str, finals_mat: int, active_mats: list[int]) -> dict:
    body = GenerateLiveBoutsRequest(
        force_regenerate=True,
        rebalance_assignments=True,
        active_mats=active_mats,
        finals_mat=finals_mat,
    )
    return await generate_live_bouts(UUID(comp_id), body)


def main() -> None:
    if not admin_supabase:
        raise SystemExit("admin_supabase not configured")

    comp_id = "5b26da6e-7840-4add-9354-450e4673d7ba"
    region_ids = _get_region_ids()
    athlete_ids, user_ids = _collect_user_ids_for_comp(comp_id)

    _reset_comp(comp_id)
    if athlete_ids or user_ids:
        _delete_users(athlete_ids, user_ids)

    _reset_categories_exact(comp_id, len(DESIRED_CATEGORY_SIZES))
    cat_ids = _get_category_ids(comp_id)
    if not cat_ids:
        raise SystemExit("no categories")

    counts = _get_target_counts(comp_id, cat_ids)
    bounds = _get_category_bounds(comp_id)

    total = _seed_apps(comp_id, cat_ids, counts, bounds, region_ids)
    res = anyio.run(_generate, comp_id, 3, [1, 2, 3])

    print("ok", True)
    print("seeded_apps", total)
    print("generated_bouts", res.get("bouts_created"))
    print("categories_total", res.get("categories"))


if __name__ == "__main__":
    main()
