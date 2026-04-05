from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from uuid import UUID
from datetime import datetime
import anyio
import os
import random
from uuid import uuid4

from app.core.supabase import supabase, admin_supabase


router = APIRouter(prefix="/live", tags=["live"])


class GenerateLiveBoutsRequest(BaseModel):
    force_regenerate: bool = False
    rebalance_assignments: bool = False


class StopLiveCompetitionRequest(BaseModel):
    clear_assignments: bool = True


class RollbackMatRequest(BaseModel):
    mat_number: int
    to_bout_id: UUID | None = None
    last_count: int = 1


class WithdrawAthleteRequest(BaseModel):
    athlete_id: UUID
    reason: str


class MoveCategoryRequest(BaseModel):
    competition_id: UUID
    to_mat_number: int


class MoveBoutsRequest(BaseModel):
    competition_id: UUID
    bout_ids: list[UUID]
    to_mat_number: int


class FinishBoutRequest(BaseModel):
    winner_athlete_id: UUID


class SeedWeighedApplicationsRequest(BaseModel):
    category_id: UUID
    count: int = 8
    start_draw_number: int = 1


class CleanupSeedUsersRequest(BaseModel):
    dry_run: bool = True


class SeedFillRoundRobinRequest(BaseModel):
    min_per_category: int = 3
    max_per_category: int = 6
    start_draw_number: int = 10000


@router.get("/competitions/{comp_id}/categories/{category_id}/standings")
async def get_round_robin_standings(comp_id: UUID, category_id: UUID):
    comp_id_str = str(comp_id)
    cat_id_str = str(category_id)

    db = admin_supabase or supabase
    has_scores = await _competition_bouts_has_score_columns()

    bouts_res = await _execute(
        db.table("competition_bouts")
        .select(
            "id,athlete_red_id,athlete_blue_id,winner_athlete_id,status"
            + (",red_wins,blue_wins,wins_to" if has_scores else "")
        )
        .eq("competition_id", comp_id_str)
        .eq("category_id", cat_id_str)
        .eq("bracket_type", "round_robin")
    )
    bouts = bouts_res.data or []
    done = [b for b in bouts if b.get("status") == "done" and b.get("winner_athlete_id")]

    stats: dict[str, dict] = {}
    def ensure(a_id: str):
        if a_id not in stats:
            stats[a_id] = {
                "athlete_id": a_id,
                "wins": 0,
                "losses": 0,
                "played": 0,
                "points": 0,
                "points_against": 0,
            }

    head_to_head: dict[tuple[str, str], int] = {}

    for b in done:
        red = str(b.get("athlete_red_id"))
        blue = str(b.get("athlete_blue_id"))
        winner = str(b.get("winner_athlete_id"))
        if not red or not blue or not winner:
            continue
        ensure(red)
        ensure(blue)
        stats[red]["played"] += 1
        stats[blue]["played"] += 1
        if has_scores:
            rw = int(b.get("red_wins") or 0)
            bw = int(b.get("blue_wins") or 0)
            stats[red]["points"] += rw
            stats[red]["points_against"] += bw
            stats[blue]["points"] += bw
            stats[blue]["points_against"] += rw
        if winner == red:
            stats[red]["wins"] += 1
            stats[blue]["losses"] += 1
            head_to_head[(red, blue)] = 1
            head_to_head[(blue, red)] = 0
        else:
            stats[blue]["wins"] += 1
            stats[red]["losses"] += 1
            head_to_head[(blue, red)] = 1
            head_to_head[(red, blue)] = 0

    rows = list(stats.values())
    rows.sort(
        key=lambda r: (
            -int(r["wins"]),
            -int(r.get("points") or 0),
            int(r.get("points_against") or 0),
            r["athlete_id"],
        )
    )

    athlete_ids = [r["athlete_id"] for r in rows]
    weight_map: dict[str, float] = {}
    if athlete_ids:
        try:
            apps_res = await _execute(
                db.table("applications")
                .select("athlete_id,actual_weight,declared_weight")
                .eq("competition_id", comp_id_str)
                .eq("category_id", cat_id_str)
                .in_("athlete_id", athlete_ids)
            )
            for a in (apps_res.data or []):
                a_id = str(a.get("athlete_id"))
                w = a.get("actual_weight")
                if w is None:
                    w = a.get("declared_weight")
                if w is None:
                    continue
                try:
                    weight_map[a_id] = float(w)
                except Exception:
                    continue
        except Exception:
            weight_map = {}

    if len(rows) == 3:
        w0 = int(rows[0]["wins"])
        p0 = int(rows[0].get("points") or 0)
        if all(int(r["wins"]) == w0 and int(r.get("points") or 0) == p0 for r in rows):
            rows.sort(
                key=lambda r: (
                    int(r.get("points_against") or 0),
                    weight_map.get(r["athlete_id"], 10**9),
                    r["athlete_id"],
                )
            )
    elif 4 <= len(rows) <= 6:
        groups: dict[tuple[int, int], list[dict]] = {}
        for r in rows:
            key = (int(r["wins"]), int(r.get("points") or 0))
            groups.setdefault(key, []).append(r)
        for group in groups.values():
            if len(group) != 2:
                continue
            a_id = group[0]["athlete_id"]
            b_id = group[1]["athlete_id"]
            h = head_to_head.get((a_id, b_id))
            if h is None:
                continue
            if h == 0:
                group[0], group[1] = group[1], group[0]
        ordered: list[dict] = []
        used = set()
        for r in rows:
            if r["athlete_id"] in used:
                continue
            key = (int(r["wins"]), int(r.get("points") or 0))
            group = groups.get(key, [r])
            for g in group:
                if g["athlete_id"] not in used:
                    ordered.append(g)
                    used.add(g["athlete_id"])
        rows = ordered

    names = await _get_athlete_name_map(athlete_ids)
    for r in rows:
        r["name"] = names.get(r["athlete_id"]) or ""

    total_bouts = len([b for b in bouts if b.get("athlete_red_id") and b.get("athlete_blue_id")])
    done_bouts = len(done)
    champion = rows[0] if total_bouts > 0 and done_bouts == total_bouts and rows else None

    return {
        "competition_id": comp_id_str,
        "category_id": cat_id_str,
        "total_bouts": total_bouts,
        "done_bouts": done_bouts,
        "standings": rows,
        "champion": champion,
    }


_bouts_has_name_columns: bool | None = None
_assignments_supports_comp_cat_upsert: bool | None = None
_mats_supports_comp_mat_upsert: bool | None = None
_bouts_has_score_columns: bool | None = None


async def _execute(query, *, retries: int = 2):
    for attempt in range(retries + 1):
        try:
            return await anyio.to_thread.run_sync(query.execute)
        except Exception as e:
            if attempt >= retries:
                raise e
            await anyio.sleep(0.2 * (attempt + 1))


async def _competition_bouts_has_name_columns() -> bool:
    global _bouts_has_name_columns
    if _bouts_has_name_columns is not None:
        return _bouts_has_name_columns

    try:
        q = supabase.table("competition_bouts").select("id,athlete_red_name,athlete_blue_name").limit(1)
        await _execute(q)
        _bouts_has_name_columns = True
        return True
    except Exception:
        _bouts_has_name_columns = False
        return False


async def _competition_bouts_has_score_columns() -> bool:
    global _bouts_has_score_columns
    if _bouts_has_score_columns is not None:
        return _bouts_has_score_columns
    try:
        q = supabase.table("competition_bouts").select("id,red_wins,blue_wins,wins_to").limit(1)
        await _execute(q)
        _bouts_has_score_columns = True
        return True
    except Exception:
        _bouts_has_score_columns = False
        return False


async def _ensure_category_assignments(comp_id_str: str, assignments: dict[str, int]) -> None:
    global _assignments_supports_comp_cat_upsert
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    rows = [
        {"competition_id": comp_id_str, "category_id": cat_id, "mat_number": int(mat)}
        for cat_id, mat in assignments.items()
    ]
    if not rows:
        return

    if _assignments_supports_comp_cat_upsert is not False:
        try:
            await _execute(
                admin_supabase.table("competition_category_assignments").upsert(
                    rows, on_conflict="competition_id,category_id"
                )
            )
            _assignments_supports_comp_cat_upsert = True
            return
        except Exception:
            _assignments_supports_comp_cat_upsert = False

    existing_res = await _execute(
        admin_supabase.table("competition_category_assignments")
        .select("id, category_id")
        .eq("competition_id", comp_id_str)
    )
    cat_to_row_id = {
        str(r["category_id"]): str(r["id"])
        for r in (existing_res.data or [])
        if r.get("category_id") and r.get("id")
    }

    for row in rows:
        cat_id = row["category_id"]
        row_id = cat_to_row_id.get(cat_id)
        if row_id:
            await _execute(
                admin_supabase.table("competition_category_assignments")
                .update({"mat_number": row["mat_number"]})
                .eq("id", row_id)
            )
        else:
            await _execute(admin_supabase.table("competition_category_assignments").insert(row))


async def _ensure_competition_mats(comp_id_str: str, mats_count: int) -> None:
    global _mats_supports_comp_mat_upsert
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    mats_count = max(1, int(mats_count))
    rows = [{"competition_id": comp_id_str, "mat_number": m} for m in range(1, mats_count + 1)]

    if _mats_supports_comp_mat_upsert is not False:
        try:
            await _execute(admin_supabase.table("competition_mats").upsert(rows, on_conflict="competition_id,mat_number"))
            _mats_supports_comp_mat_upsert = True
            return
        except Exception:
            _mats_supports_comp_mat_upsert = False

    existing_res = await _execute(
        admin_supabase.table("competition_mats").select("id, mat_number").eq("competition_id", comp_id_str)
    )
    existing = {int(r["mat_number"]): str(r["id"]) for r in (existing_res.data or []) if r.get("mat_number") and r.get("id")}

    for row in rows:
        mat_number = int(row["mat_number"])
        if mat_number in existing:
            continue
        await _execute(admin_supabase.table("competition_mats").insert(row))


def _round_robin_rounds_with_bye_priority(athlete_ids: list[str]):
    participants = list(athlete_ids)
    n = len(participants)
    if n < 2:
        return []

    bye_added = False
    if n % 2 != 0:
        participants.append(None)
        n += 1
        bye_added = True

    rounds: list[list[tuple[str, str]]] = []
    last_round_bye: str | None = None

    for round_idx in range(n - 1):
        pairs: list[tuple[str | None, str | None]] = []
        bye_athlete: str | None = None

        for j in range(n // 2):
            a = participants[j]
            b = participants[n - 1 - j]
            if a is None and b is None:
                continue
            if a is None or b is None:
                bye_athlete = a if a is not None else b
                continue
            pairs.append((a, b))

        if last_round_bye:
            for i, (a, b) in enumerate(pairs):
                if a == last_round_bye or b == last_round_bye:
                    if b == last_round_bye:
                        pairs[i] = (b, a)
                    pairs.insert(0, pairs.pop(i))
                    break

        rounds.append([(a, b) for a, b in pairs])
        last_round_bye = bye_athlete if bye_added else None

        participants = [participants[0]] + [participants[-1]] + participants[1:-1]

    return rounds


def _round_robin_rounds_with_bye_priority_from_participants(participants: list[str | None]):
    n = len(participants)
    if n < 2:
        return []

    bye_added = any(p is None for p in participants)
    rounds: list[list[tuple[str, str]]] = []
    last_round_bye: str | None = None

    for _round_idx in range(n - 1):
        pairs: list[tuple[str | None, str | None]] = []
        bye_athlete: str | None = None

        for j in range(n // 2):
            a = participants[j]
            b = participants[n - 1 - j]
            if a is None and b is None:
                continue
            if a is None or b is None:
                bye_athlete = a if a is not None else b
                continue
            pairs.append((a, b))

        if last_round_bye:
            for i, (a, b) in enumerate(pairs):
                if a == last_round_bye or b == last_round_bye:
                    if b == last_round_bye:
                        pairs[i] = (b, a)
                    pairs.insert(0, pairs.pop(i))
                    break

        rounds.append([(a, b) for a, b in pairs])
        last_round_bye = bye_athlete if bye_added else None

        participants = [participants[0]] + [participants[-1]] + participants[1:-1]

    return rounds


def _best_pairs_avoiding_same_region(athlete_ids: list[str], region_by_athlete: dict[str, str]) -> list[tuple[str, str]]:
    if len(athlete_ids) < 2:
        return []

    best_pairs: list[tuple[str, str]] = []
    best_same = 10**9

    for _ in range(250):
        remaining = list(athlete_ids)
        random.shuffle(remaining)
        pairs: list[tuple[str, str]] = []
        while len(remaining) >= 2:
            a = remaining.pop(0)
            ra = region_by_athlete.get(a)
            idx = None
            if ra:
                for i, b in enumerate(remaining):
                    rb = region_by_athlete.get(b)
                    if not rb or rb != ra:
                        idx = i
                        break
            if idx is None:
                idx = 0
            b = remaining.pop(idx)
            pairs.append((a, b))

        same = 0
        for a, b in pairs:
            ra = region_by_athlete.get(a)
            rb = region_by_athlete.get(b)
            if ra and rb and ra == rb:
                same += 1
        if same < best_same:
            best_same = same
            best_pairs = pairs
            if best_same == 0:
                break

    return best_pairs


def _seed_round_robin_participants(athlete_ids: list[str], region_by_athlete: dict[str, str]) -> list[str | None]:
    ids = list(athlete_ids)
    if len(ids) < 2:
        return ids

    if len(ids) % 2 == 0:
        pairs = _best_pairs_avoiding_same_region(ids, region_by_athlete)
        n = len(ids)
        participants: list[str | None] = [None] * n
        for i, (a, b) in enumerate(pairs):
            participants[i] = a
            participants[n - 1 - i] = b
        return participants

    best_participants: list[str | None] | None = None
    best_same = 10**9
    for _ in range(200):
        random.shuffle(ids)
        bye = ids[0]
        rest = ids[1:]
        pairs = _best_pairs_avoiding_same_region(rest, region_by_athlete)
        n = len(ids) + 1
        participants = [None] * n
        participants[0] = bye
        participants[n - 1] = None
        for i, (a, b) in enumerate(pairs, start=1):
            participants[i] = a
            participants[n - 1 - i] = b

        same = 0
        for i in range(1, n // 2):
            a = participants[i]
            b = participants[n - 1 - i]
            if a is None or b is None:
                continue
            ra = region_by_athlete.get(str(a))
            rb = region_by_athlete.get(str(b))
            if ra and rb and ra == rb:
                same += 1
        if same < best_same:
            best_same = same
            best_participants = participants
            if best_same == 0:
                break
    return best_participants or (list(athlete_ids) + [None])


async def _get_athlete_region_map(athlete_ids: list[str]) -> dict[str, str]:
    if not athlete_ids:
        return {}
    db = admin_supabase or supabase
    res = await _execute(db.table("athletes").select("id,user_id").in_("id", athlete_ids))
    rows = res.data or []
    athlete_to_user = {str(r["id"]): str(r["user_id"]) for r in rows if r.get("id") and r.get("user_id")}
    user_ids = list(dict.fromkeys(athlete_to_user.values()))
    if not user_ids:
        return {}
    pres = await _execute(db.table("profiles").select("user_id,location_id").in_("user_id", user_ids))
    prows = pres.data or []
    user_to_region = {str(r["user_id"]): str(r["location_id"]) for r in prows if r.get("user_id") and r.get("location_id")}
    out: dict[str, str] = {}
    for a_id, u_id in athlete_to_user.items():
        reg = user_to_region.get(u_id)
        if reg:
            out[a_id] = reg
    return out


async def _get_athlete_name_map(athlete_ids: list[str]) -> dict[str, str]:
    if not athlete_ids:
        return {}

    athletes_res = await _execute(
        supabase.table("athletes").select("id, user_id").in_("id", athlete_ids)
    )
    athletes_rows = athletes_res.data or []
    athlete_to_user: dict[str, str] = {}
    user_ids: list[str] = []
    for row in athletes_rows:
        a_id = str(row.get("id"))
        u_id = row.get("user_id")
        if a_id and u_id:
            athlete_to_user[a_id] = str(u_id)
            user_ids.append(str(u_id))

    profiles_res = await _execute(
        supabase.table("profiles").select("user_id, full_name").in_("user_id", list(set(user_ids)))
    )
    profiles_rows = profiles_res.data or []
    user_to_name = {str(p["user_id"]): str(p.get("full_name") or "") for p in profiles_rows if p.get("user_id")}

    athlete_to_name: dict[str, str] = {}
    for a_id, u_id in athlete_to_user.items():
        name = user_to_name.get(u_id) or ""
        athlete_to_name[a_id] = name
    return athlete_to_name


def _category_label(cat: dict) -> str:
    gender = cat.get("gender")
    g = "Мужчины" if gender == "male" else "Женщины" if gender == "female" else "Категория"
    age_min = cat.get("age_min")
    age_max = cat.get("age_max")
    w_min = cat.get("weight_min")
    w_max = cat.get("weight_max")
    age_part = f"{age_min}-{age_max}" if age_min is not None and age_max is not None else ""
    def _fmt_num(x):
        try:
            xi = int(x)
            if float(x) == float(xi):
                return str(xi)
        except Exception:
            pass
        return str(x)
    if w_max is not None:
        try:
            if float(w_max) >= 999 and w_min is not None:
                weight_part = f"{_fmt_num(w_min)}+"
            else:
                weight_part = f"до {_fmt_num(w_max)} кг"
        except Exception:
            weight_part = f"до {_fmt_num(w_max)} кг"
    else:
        weight_part = f"{_fmt_num(w_min)}+" if w_min is not None else ""
    parts = [p for p in [g, age_part, weight_part] if p]
    return " ".join(parts)


def _balanced_assignments(
    categories: list[dict],
    weighed_counts: dict[str, int],
    mats_count: int,
    existing_assignments: dict[str, int],
) -> dict[str, int]:
    mats_count = max(1, int(mats_count))
    mats_load = {m: 0 for m in range(1, mats_count + 1)}
    result = dict(existing_assignments)

    for cat_id, mat in existing_assignments.items():
        mats_load[int(mat)] = mats_load.get(int(mat), 0) + int(weighed_counts.get(cat_id, 0))

    sorted_cats = sorted(
        categories,
        key=lambda c: int(weighed_counts.get(str(c["id"]), 0)),
        reverse=True,
    )

    for cat in sorted_cats:
        cat_id = str(cat["id"])
        if cat_id in result:
            continue
        min_mat = min(mats_load, key=mats_load.get)
        result[cat_id] = int(min_mat)
        mats_load[min_mat] += int(weighed_counts.get(cat_id, 0))

    return result


async def _select_competition_bouts_for_comp(comp_id_str: str):
    cols = (
        "id,competition_id,category_id,athlete_red_id,athlete_blue_id,bracket_type,round_index,stage,"
        "status,winner_athlete_id,mat_number,order_in_mat,updated_at"
    )
    if await _competition_bouts_has_name_columns():
        cols += ",athlete_red_name,athlete_blue_name"
    if await _competition_bouts_has_score_columns():
        cols += ",red_wins,blue_wins,wins_to"
    q = (
        admin_supabase.table("competition_bouts")
        .select(cols)
        .eq("competition_id", comp_id_str)
        .order("mat_number", desc=False)
        .order("order_in_mat", desc=False)
    )
    return await _execute(q)


async def _materialize_names_for_bouts(bouts: list[dict]) -> list[dict]:
    if not bouts:
        return bouts
    if await _competition_bouts_has_name_columns():
        return bouts
    athlete_ids = []
    for b in bouts:
        if b.get("athlete_red_id"):
            athlete_ids.append(str(b["athlete_red_id"]))
        if b.get("athlete_blue_id"):
            athlete_ids.append(str(b["athlete_blue_id"]))
    athlete_ids = list(dict.fromkeys(athlete_ids))
    name_map = await _get_athlete_name_map(athlete_ids)
    for b in bouts:
        b["athlete_red_name"] = name_map.get(str(b.get("athlete_red_id") or "")) or ""
        b["athlete_blue_name"] = name_map.get(str(b.get("athlete_blue_id") or "")) or ""
    return bouts


async def _get_mat_round(comp_id_str: str, mat_number: int) -> int | None:
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("round_index")
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
        .in_("status", ["queued", "next", "running"])
        .order("round_index", desc=False)
        .limit(1)
    )
    if not res.data:
        return None
    val = res.data[0].get("round_index")
    return int(val) if val is not None else None


async def _get_mats_count(comp_id_str: str) -> int:
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    res = await _execute(
        admin_supabase.table("competitions").select("mats_count").eq("id", comp_id_str).single()
    )
    return int((res.data or {}).get("mats_count") or 1)


async def _set_next_for_mat(comp_id_str: str, mat_number: int):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id,status,order_in_mat")
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
        .in_("status", ["queued", "next", "running"])
        .order("order_in_mat", desc=False)
    )
    rows = res.data or []
    running = [r for r in rows if r.get("status") == "running"]
    if running:
        rid = str(running[0]["id"])
        await _execute(
            admin_supabase.table("competition_mats")
            .update({"current_bout_id": rid})
            .eq("competition_id", comp_id_str)
            .eq("mat_number", mat_number)
        )
        return {"current_bout_id": rid, "next_bout_id": None}

    qn = [r for r in rows if r.get("status") in ("queued", "next")]
    if not qn:
        await _execute(
            admin_supabase.table("competition_mats")
            .update({"current_bout_id": None})
            .eq("competition_id", comp_id_str)
            .eq("mat_number", mat_number)
        )
        return {"current_bout_id": None, "next_bout_id": None}

    head_id = str(qn[0]["id"])
    await _execute(
        admin_supabase.table("competition_bouts")
        .update({"status": "queued"})
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
        .in_("status", ["queued", "next"])
    )
    await _execute(
        admin_supabase.table("competition_bouts")
        .update({"status": "next"})
        .eq("id", head_id)
    )
    await _execute(
        admin_supabase.table("competition_mats")
        .update({"current_bout_id": None})
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
    )
    return {"current_bout_id": None, "next_bout_id": head_id}


@router.post("/competitions/{comp_id}/generate")
async def generate_live_bouts(comp_id: UUID, body: GenerateLiveBoutsRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    comp_id_str = str(comp_id)
    mats_res = await _execute(admin_supabase.table("competitions").select("mats_count").eq("id", comp_id_str).single())
    mats_count = int((mats_res.data or {}).get("mats_count") or 1)

    started_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id")
        .eq("competition_id", comp_id_str)
        .in_("status", ["running", "done"])
        .limit(1)
    )
    if started_res.data:
        raise HTTPException(status_code=409, detail="Competition already started. Stop it to reset.")

    categories_res = await _execute(
        admin_supabase.table("competition_categories").select("*").eq("competition_id", comp_id_str)
    )
    categories = categories_res.data or []

    if body.force_regenerate:
        await _execute(
            admin_supabase.table("competition_mats")
            .update({"current_bout_id": None})
            .eq("competition_id", comp_id_str)
        )
        await _execute(admin_supabase.table("competition_bouts").delete().eq("competition_id", comp_id_str))
    else:
        existing_bouts_res = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id")
            .eq("competition_id", comp_id_str)
            .limit(1)
        )
        if existing_bouts_res.data:
            raise HTTPException(status_code=409, detail="Bouts already generated. Use force_regenerate=true")

    weighed_counts: dict[str, int] = {}
    cat_to_athletes: dict[str, list[str]] = {}
    cat_to_withdrawn: dict[str, list[str]] = {}

    for cat in categories:
        cat_id = str(cat["id"])
        apps_res = await _execute(
            admin_supabase.table("applications")
            .select("athlete_id, created_at, comment")
            .eq("competition_id", comp_id_str)
            .eq("category_id", cat_id)
            .eq("status", "weighed")
            .order("created_at", desc=False)
        )
        rows = apps_res.data or []
        athlete_ids = []
        withdrawn_ids = []
        for r in rows:
            a_id = r.get("athlete_id")
            if not a_id:
                continue
            a_id_str = str(a_id)
            c = str(r.get("comment") or "")
            if c.startswith("[WITHDRAWN:"):
                withdrawn_ids.append(a_id_str)
                continue
            athlete_ids.append(a_id_str)
        cat_to_athletes[cat_id] = athlete_ids
        cat_to_withdrawn[cat_id] = withdrawn_ids
        weighed_counts[cat_id] = len(athlete_ids)

    existing_assignments_res = await _execute(
        admin_supabase.table("competition_category_assignments")
        .select("category_id, mat_number")
        .eq("competition_id", comp_id_str)
    )
    existing_assignments = {
        str(r["category_id"]): int(r["mat_number"]) for r in (existing_assignments_res.data or []) if r.get("category_id")
    }
    assignments = _balanced_assignments(
        categories,
        weighed_counts,
        mats_count,
        {} if body.rebalance_assignments else existing_assignments,
    )

    await _ensure_category_assignments(comp_id_str, assignments)
    await _ensure_competition_mats(comp_id_str, mats_count)

    all_athlete_ids = []
    for ids in cat_to_athletes.values():
        all_athlete_ids.extend(ids)
    all_athlete_ids = list(dict.fromkeys(all_athlete_ids))

    name_map = await _get_athlete_name_map(all_athlete_ids)
    region_map = await _get_athlete_region_map(all_athlete_ids)
    has_name_cols = await _competition_bouts_has_name_columns()

    bouts_to_insert: list[dict] = []
    sortable_bouts: list[tuple[int, int, str, int, dict]] = []
    cat_label_by_id = {str(c["id"]): _category_label(c) for c in categories if c.get("id")}
    seq = 0

    for cat in categories:
        cat_id = str(cat["id"])
        athlete_ids = cat_to_athletes.get(cat_id, [])
        if len(athlete_ids) < 2:
            continue

        mat_number = int(assignments.get(cat_id) or 1)
        cat_label = cat_label_by_id.get(cat_id) or cat_id

        score_cols = await _competition_bouts_has_score_columns()

        if len(athlete_ids) <= 6:
            seeded = _seed_round_robin_participants(athlete_ids, region_map)
            rounds = _round_robin_rounds_with_bye_priority_from_participants(seeded)
            for r_idx, matches in enumerate(rounds, start=1):
                for a_id, b_id in matches:
                    row = {
                        "competition_id": comp_id_str,
                        "category_id": cat_id,
                        "athlete_red_id": a_id,
                        "athlete_blue_id": b_id,
                        "bracket_type": "round_robin",
                        "round_index": int(r_idx),
                        "stage": None,
                        "status": "queued",
                        "winner_athlete_id": None,
                        "mat_number": mat_number,
                        "order_in_mat": 0,
                    }
                    if score_cols:
                        row["red_wins"] = 0
                        row["blue_wins"] = 0
                        row["wins_to"] = 2
                    if has_name_cols:
                        row["athlete_red_name"] = name_map.get(a_id) or ""
                        row["athlete_blue_name"] = name_map.get(b_id) or ""
                    bouts_to_insert.append(row)
                    sortable_bouts.append((mat_number, int(r_idx), cat_label, seq, row))
                    seq += 1
        else:
            shuffled = list(athlete_ids)
            random.shuffle(shuffled)
            bye = None
            if len(shuffled) % 2 != 0:
                bye = shuffled.pop()

            pairs = _best_pairs_avoiding_same_region(shuffled, region_map)
            if bye is not None:
                bye_row = {
                    "competition_id": comp_id_str,
                    "category_id": cat_id,
                    "athlete_red_id": bye,
                    "athlete_blue_id": bye,
                    "bracket_type": "double_elim",
                    "round_index": 1,
                    "stage": "bye",
                    "status": "done",
                    "winner_athlete_id": bye,
                    "mat_number": mat_number,
                    "order_in_mat": 0,
                }
                if has_name_cols:
                    bye_row["athlete_red_name"] = name_map.get(bye) or ""
                    bye_row["athlete_blue_name"] = name_map.get(bye) or ""
                bouts_to_insert.append(bye_row)

            for a_id, b_id in pairs:
                row = {
                    "competition_id": comp_id_str,
                    "category_id": cat_id,
                    "athlete_red_id": a_id,
                    "athlete_blue_id": b_id,
                    "bracket_type": "double_elim",
                    "round_index": 1,
                    "stage": "wb",
                    "status": "queued",
                    "winner_athlete_id": None,
                    "mat_number": mat_number,
                    "order_in_mat": 0,
                }
                if score_cols:
                    row["red_wins"] = 0
                    row["blue_wins"] = 0
                    row["wins_to"] = 2
                if has_name_cols:
                    row["athlete_red_name"] = name_map.get(a_id) or ""
                    row["athlete_blue_name"] = name_map.get(b_id) or ""
                bouts_to_insert.append(row)
                sortable_bouts.append((mat_number, 1, cat_label, seq, row))
                seq += 1

    if sortable_bouts:
        sortable_bouts.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        order_by_mat: dict[int, int] = {}
        for mat_number, _round_idx, _cat_label, _seq, row in sortable_bouts:
            nxt = order_by_mat.get(mat_number, 0) + 1
            order_by_mat[mat_number] = nxt
            row["order_in_mat"] = nxt

    for i in range(0, len(bouts_to_insert), 200):
        chunk = bouts_to_insert[i : i + 200]
        await _execute(admin_supabase.table("competition_bouts").insert(chunk))

    for m in range(1, mats_count + 1):
        await _set_next_for_mat(comp_id_str, m)

    return {
        "status": "ok",
        "competition_id": comp_id_str,
        "mats_count": mats_count,
        "categories": len(categories),
        "bouts_created": len(bouts_to_insert),
        "generated_at": datetime.now().isoformat(),
    }


@router.post("/competitions/{comp_id}/stop")
async def stop_live_competition(comp_id: UUID, body: StopLiveCompetitionRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")

    comp_id_str = str(comp_id)
    await _execute(
        admin_supabase.table("competition_mats")
        .update({"current_bout_id": None})
        .eq("competition_id", comp_id_str)
    )
    await _execute(admin_supabase.table("competition_bouts").delete().eq("competition_id", comp_id_str))
    if body.clear_assignments:
        await _execute(
            admin_supabase.table("competition_category_assignments")
            .delete()
            .eq("competition_id", comp_id_str)
        )
    return {"ok": True, "competition_id": comp_id_str, "deleted_bouts": True, "cleared_assignments": body.clear_assignments}


@router.post("/competitions/{comp_id}/withdraw")
async def withdraw_athlete(comp_id: UUID, body: WithdrawAthleteRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    comp_id_str = str(comp_id)
    athlete_id_str = str(body.athlete_id)
    reason = str(body.reason or "").strip().lower()
    if reason not in ("medical", "disciplinary"):
        raise HTTPException(status_code=400, detail="reason must be medical or disciplinary")

    bouts_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id,mat_number,status")
        .eq("competition_id", comp_id_str)
        .or_(f"athlete_red_id.eq.{athlete_id_str},athlete_blue_id.eq.{athlete_id_str}")
        .limit(2000)
    )
    bouts = bouts_res.data or []
    if not bouts:
        return {"ok": True, "withdrawn": True, "affected_bouts": 0}

    update = {"status": "cancelled", "winner_athlete_id": None, "stage": f"withdrawn_{reason}"}
    if await _competition_bouts_has_score_columns():
        update["red_wins"] = 0
        update["blue_wins"] = 0

    ids = [str(b["id"]) for b in bouts if b.get("id")]
    for i in range(0, len(ids), 200):
        chunk = ids[i : i + 200]
        await _execute(admin_supabase.table("competition_bouts").update(update).in_("id", chunk))

    mats = {int(b.get("mat_number") or 0) for b in bouts}
    mats = {m for m in mats if m > 0}

    if mats:
        await _execute(
            admin_supabase.table("competition_mats")
            .update({"current_bout_id": None})
            .eq("competition_id", comp_id_str)
            .in_("mat_number", list(mats))
        )
        for m in sorted(mats):
            await _set_next_for_mat(comp_id_str, m)

    apps_res = await _execute(
        admin_supabase.table("applications")
        .select("id,comment")
        .eq("competition_id", comp_id_str)
        .eq("athlete_id", athlete_id_str)
        .limit(50)
    )
    app_rows = apps_res.data or []
    if app_rows:
        for r in app_rows:
            app_id = r.get("id")
            if not app_id:
                continue
            prev = str(r.get("comment") or "")
            marker = f"[WITHDRAWN:{reason}]"
            if prev.startswith("[WITHDRAWN:"):
                new_comment = prev
            else:
                new_comment = (marker + (" " + prev if prev else "")).strip()
            await _execute(admin_supabase.table("applications").update({"comment": new_comment}).eq("id", str(app_id)))

    return {"ok": True, "withdrawn": True, "affected_bouts": len(ids), "mats": sorted(mats)}


@router.get("/competitions/{comp_id}/state")
async def get_live_state(comp_id: UUID):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    comp_id_str = str(comp_id)
    comp_res = await _execute(
        admin_supabase.table("competitions").select("id,mats_count,name").eq("id", comp_id_str).single()
    )
    comp = comp_res.data or {}
    mats_count = int(comp.get("mats_count") or 1)

    cats_res = await _execute(
        admin_supabase.table("competition_categories").select("id,gender,age_min,age_max,weight_min,weight_max").eq("competition_id", comp_id_str)
    )
    categories = {str(c["id"]): c for c in (cats_res.data or []) if c.get("id")}

    assigns_res = await _execute(
        admin_supabase.table("competition_category_assignments").select("category_id,mat_number").eq("competition_id", comp_id_str)
    )
    assigns = assigns_res.data or []

    cats_by_mat: dict[int, list[dict]] = {m: [] for m in range(1, mats_count + 1)}
    for a in assigns:
        cat_id = str(a.get("category_id") or "")
        mat = int(a.get("mat_number") or 0)
        if mat < 1 or mat > mats_count:
            continue
        cat = categories.get(cat_id)
        if not cat:
            continue
        cats_by_mat[mat].append({"id": cat_id, "label": _category_label(cat)})

    mats_res = await _execute(
        admin_supabase.table("competition_mats").select("mat_number,current_bout_id").eq("competition_id", comp_id_str)
    )
    mats_rows = mats_res.data or []

    bouts_res = await _select_competition_bouts_for_comp(comp_id_str)
    bouts = bouts_res.data or []
    bouts = [b for b in bouts if b.get("status") in ("queued", "next", "running")]
    bouts = await _materialize_names_for_bouts(bouts)
    has_bouts = bool(bouts)
    started_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id")
        .eq("competition_id", comp_id_str)
        .in_("status", ["running", "done"])
        .limit(1)
    )
    has_started = bool(started_res.data)
    bouts_by_mat: dict[int, list[dict]] = {m: [] for m in range(1, mats_count + 1)}
    for b in bouts:
        mat = int(b.get("mat_number") or 0)
        if mat < 1 or mat > mats_count:
            continue
        bouts_by_mat[mat].append(b)

    mats_out = []
    for m in range(1, mats_count + 1):
        mat_bouts = sorted(bouts_by_mat.get(m, []), key=lambda x: int(x.get("order_in_mat") or 0))
        running_bout = next((b for b in mat_bouts if b.get("status") == "running"), None)
        next_marked = next((b for b in mat_bouts if b.get("status") == "next"), None)
        current_bout = running_bout or next_marked
        next_bout = None
        if current_bout:
            cur_order = int(current_bout.get("order_in_mat") or 0)
            next_bout = next(
                (b for b in mat_bouts if b.get("status") == "queued" and int(b.get("order_in_mat") or 0) > cur_order),
                None,
            )
        if not next_bout:
            next_bout = next((b for b in mat_bouts if b.get("status") == "queued"), None)

        rounds = sorted({int(b.get("round_index") or 0) for b in mat_bouts if b.get("round_index") is not None})
        rounds = [r for r in rounds if r > 0]
        rounds_window = set(rounds[:2]) if rounds else set()
        if rounds_window:
            queue_bouts = [b for b in mat_bouts if int(b.get("round_index") or 0) in rounds_window]
            queue_bouts = queue_bouts[:400]
        else:
            queue_bouts = mat_bouts[:100]
        pin_ids = {str(x.get("id")) for x in [current_bout, next_bout] if x}
        pin = [b for b in mat_bouts if str(b.get("id")) in pin_ids]
        rest = [b for b in queue_bouts if str(b.get("id")) not in pin_ids]
        queue_bouts = pin + rest

        cols_hist = (
            "id,competition_id,category_id,athlete_red_id,athlete_blue_id,bracket_type,round_index,stage,"
            "status,winner_athlete_id,mat_number,order_in_mat,updated_at"
        )
        if await _competition_bouts_has_name_columns():
            cols_hist += ",athlete_red_name,athlete_blue_name"
        if await _competition_bouts_has_score_columns():
            cols_hist += ",red_wins,blue_wins,wins_to"
        hist_res = await _execute(
            admin_supabase.table("competition_bouts")
            .select(cols_hist)
            .eq("competition_id", comp_id_str)
            .eq("mat_number", m)
            .eq("status", "done")
            .order("updated_at", desc=True)
            .limit(30)
        )
        history_bouts = await _materialize_names_for_bouts(hist_res.data or [])

        mat_current_round = None
        for b in mat_bouts:
            r = b.get("round_index")
            if r is None:
                continue
            r_int = int(r)
            if mat_current_round is None or r_int < mat_current_round:
                mat_current_round = r_int

        mats_out.append(
            {
                "mat_number": m,
                "categories": sorted(cats_by_mat.get(m, []), key=lambda x: x["label"]),
                "current_bout": current_bout,
                "next_bout": next_bout,
                "queue": queue_bouts,
                "history": history_bouts,
                "current_round": mat_current_round,
            }
        )

    return {
        "competition": {"id": comp_id_str, "name": comp.get("name"), "mats_count": mats_count, "has_bouts": has_bouts, "has_started": has_started},
        "mats": mats_out,
    }


@router.post("/bouts/{bout_id}/start")
async def start_bout(bout_id: UUID):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    bout_id_str = str(bout_id)
    bout_res = await _execute(
        admin_supabase.table("competition_bouts").select("id,competition_id,mat_number,status,order_in_mat").eq("id", bout_id_str).single()
    )
    bout = bout_res.data
    if not bout:
        raise HTTPException(status_code=404, detail="Bout not found")

    comp_id_str = str(bout["competition_id"])
    mat_number = int(bout.get("mat_number") or 0)
    if mat_number < 1:
        raise HTTPException(status_code=400, detail="Bout has no mat_number")

    running_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id")
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
        .eq("status", "running")
        .limit(1)
    )
    if running_res.data and str(running_res.data[0]["id"]) != bout_id_str:
        raise HTTPException(status_code=409, detail="Another bout is already running on this mat")

    if bout.get("status") not in ("next", "queued"):
        raise HTTPException(status_code=409, detail="Bout is not ready to start")

    if bout.get("status") == "queued":
        head_res = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id,order_in_mat")
            .eq("competition_id", comp_id_str)
            .eq("mat_number", mat_number)
            .in_("status", ["queued", "next"])
            .order("order_in_mat", desc=False)
            .limit(1)
        )
        if head_res.data and str(head_res.data[0]["id"]) != bout_id_str:
            raise HTTPException(status_code=409, detail="Only the next bout can be started")

    await _execute(
        admin_supabase.table("competition_bouts")
        .update({"status": "queued"})
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
        .eq("status", "next")
        .neq("id", bout_id_str)
    )
    await _execute(admin_supabase.table("competition_bouts").update({"status": "running"}).eq("id", bout_id_str))
    await _execute(
        admin_supabase.table("competition_mats")
        .update({"current_bout_id": bout_id_str})
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
    )
    return {"ok": True, "bout_id": bout_id_str, "status": "running"}



@router.post("/bouts/{bout_id}/finish")
async def finish_bout(bout_id: UUID, body: FinishBoutRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    bout_id_str = str(bout_id)
    bout_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id,competition_id,mat_number,status,athlete_red_id,athlete_blue_id,red_wins,blue_wins,wins_to")
        .eq("id", bout_id_str)
        .single()
    )
    bout = bout_res.data
    if not bout:
        raise HTTPException(status_code=404, detail="Bout not found")

    if bout.get("status") == "done":
        return {
            "ok": True,
            "bout_id": bout_id_str,
            "status": "done",
            "red_wins": int(bout.get("red_wins") or 0),
            "blue_wins": int(bout.get("blue_wins") or 0),
            "winner_athlete_id": bout.get("winner_athlete_id"),
        }
    if bout.get("status") != "running":
        raise HTTPException(status_code=409, detail="Bout is not running")

    winner_id = str(body.winner_athlete_id)
    red_id = str(bout.get("athlete_red_id"))
    blue_id = str(bout.get("athlete_blue_id"))
    if winner_id not in (red_id, blue_id):
        raise HTTPException(status_code=400, detail="Winner must be one of the bout athletes")

    comp_id_str = str(bout["competition_id"])
    mat_number = int(bout.get("mat_number") or 0)

    has_scores = await _competition_bouts_has_score_columns()
    if has_scores:
        red_wins = int(bout.get("red_wins") or 0)
        blue_wins = int(bout.get("blue_wins") or 0)
        wins_to = int(bout.get("wins_to") or 2)

        if winner_id == red_id:
            red_wins += 1
        else:
            blue_wins += 1

        if red_wins >= wins_to or blue_wins >= wins_to:
            await _execute(
                admin_supabase.table("competition_bouts")
                .update({"status": "done", "winner_athlete_id": winner_id, "red_wins": red_wins, "blue_wins": blue_wins})
                .eq("id", bout_id_str)
            )
            await _set_next_for_mat(comp_id_str, mat_number)
            return {"ok": True, "bout_id": bout_id_str, "status": "done", "red_wins": red_wins, "blue_wins": blue_wins}
        else:
            await _execute(
                admin_supabase.table("competition_bouts")
                .update({"red_wins": red_wins, "blue_wins": blue_wins})
                .eq("id", bout_id_str)
            )
            return {"ok": True, "bout_id": bout_id_str, "status": "running", "red_wins": red_wins, "blue_wins": blue_wins}
    else:
        await _execute(
            admin_supabase.table("competition_bouts")
            .update({"status": "done", "winner_athlete_id": winner_id})
            .eq("id", bout_id_str)
        )
        await _set_next_for_mat(comp_id_str, mat_number)
        return {"ok": True, "bout_id": bout_id_str, "status": "done"}


@router.post("/bouts/{bout_id}/cancel")
async def cancel_bout(bout_id: UUID):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    bout_id_str = str(bout_id)
    bout_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id,competition_id,mat_number,status")
        .eq("id", bout_id_str)
        .single()
    )
    bout = bout_res.data
    if not bout:
        raise HTTPException(status_code=404, detail="Bout not found")

    status = bout.get("status")
    if status not in ("running", "next", "queued"):
        raise HTTPException(status_code=409, detail="Bout cannot be cancelled in its current status")

    comp_id_str = str(bout["competition_id"])
    mat_number = int(bout.get("mat_number") or 0)
    if mat_number < 1:
        raise HTTPException(status_code=400, detail="Bout has no mat_number")

    update = {"status": "cancelled", "winner_athlete_id": None}
    if await _competition_bouts_has_score_columns():
        update["red_wins"] = 0
        update["blue_wins"] = 0
    await _execute(admin_supabase.table("competition_bouts").update(update).eq("id", bout_id_str))

    if status == "running":
        await _execute(
            admin_supabase.table("competition_mats")
            .update({"current_bout_id": None})
            .eq("competition_id", comp_id_str)
            .eq("mat_number", mat_number)
        )

    await _set_next_for_mat(comp_id_str, mat_number)
    return {"ok": True, "bout_id": bout_id_str, "status": "cancelled"}


@router.post("/competitions/{comp_id}/rollback")
async def rollback_mat(comp_id: UUID, body: RollbackMatRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    comp_id_str = str(comp_id)
    mat_number = int(body.mat_number)
    if mat_number < 1:
        raise HTTPException(status_code=400, detail="mat_number must be >= 1")

    running_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id")
        .eq("competition_id", comp_id_str)
        .eq("mat_number", mat_number)
        .eq("status", "running")
        .limit(1)
    )
    if running_res.data:
        raise HTTPException(status_code=409, detail="Stop the running bout before rollback")

    ids_to_rollback: list[str] = []
    if body.to_bout_id:
        target_id = str(body.to_bout_id)
        target_res = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id,updated_at,status")
            .eq("id", target_id)
            .eq("competition_id", comp_id_str)
            .eq("mat_number", mat_number)
            .single()
        )
        target = target_res.data
        if not target:
            raise HTTPException(status_code=404, detail="Target bout not found on this mat")
        if target.get("status") != "done":
            raise HTTPException(status_code=409, detail="Target bout is not done")
        target_updated = target.get("updated_at")
        sel = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id")
            .eq("competition_id", comp_id_str)
            .eq("mat_number", mat_number)
            .eq("status", "done")
            .gte("updated_at", target_updated)
            .order("updated_at", desc=True)
            .limit(200)
        )
        ids_to_rollback = [str(r["id"]) for r in (sel.data or []) if r.get("id")]
    else:
        last_count = max(1, int(body.last_count))
        sel = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id")
            .eq("competition_id", comp_id_str)
            .eq("mat_number", mat_number)
            .eq("status", "done")
            .order("updated_at", desc=True)
            .limit(last_count)
        )
        ids_to_rollback = [str(r["id"]) for r in (sel.data or []) if r.get("id")]

    if not ids_to_rollback:
        return {"ok": True, "rolled_back": 0, "mat_number": mat_number}

    update = {"status": "queued", "winner_athlete_id": None}
    if await _competition_bouts_has_score_columns():
        update["red_wins"] = 0
        update["blue_wins"] = 0

    for i in range(0, len(ids_to_rollback), 200):
        chunk = ids_to_rollback[i : i + 200]
        await _execute(admin_supabase.table("competition_bouts").update(update).in_("id", chunk))

    await _set_next_for_mat(comp_id_str, mat_number)
    return {"ok": True, "rolled_back": len(ids_to_rollback), "mat_number": mat_number}


@router.post("/competitions/{comp_id}/seed-weighed")
async def seed_weighed_applications(comp_id: UUID, body: SeedWeighedApplicationsRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    count = int(body.count)
    if count < 2 or count > 32:
        raise HTTPException(status_code=400, detail="count must be between 2 and 32")

    comp_id_str = str(comp_id)
    cat_id_str = str(body.category_id)

    cat_res = await _execute(
        admin_supabase.table("competition_categories")
        .select("id,competition_id")
        .eq("id", cat_id_str)
        .single()
    )
    if not cat_res.data or str(cat_res.data.get("competition_id")) != comp_id_str:
        raise HTTPException(status_code=404, detail="Category not found for this competition")

    existing_res = await _execute(
        admin_supabase.table("applications")
        .select("draw_number")
        .eq("competition_id", comp_id_str)
        .order("draw_number", desc=True, nullsfirst=False)
        .limit(1)
    )
    max_draw = int((existing_res.data or [{}])[0].get("draw_number") or 0)
    draw = max(max_draw + 1, int(body.start_draw_number))

    users = []
    athletes = []
    profiles = []
    applications = []

    for i in range(count):
        user_id = str(uuid4())
        athlete_id = str(uuid4())
        email = f"seed_{comp_id_str[:8]}_{cat_id_str[:8]}_{uuid4().hex[:8]}@example.com"
        full_name = f"Тестовый Спортсмен {draw}"

        users.append({"id": user_id, "email": email})
        profiles.append({"user_id": user_id, "full_name": full_name})
        athletes.append({"id": athlete_id, "user_id": user_id, "coach_name": "Тестовый тренер"})
        applications.append(
            {
                "competition_id": comp_id_str,
                "athlete_id": athlete_id,
                "category_id": cat_id_str,
                "status": "weighed",
                "declared_weight": 60,
                "actual_weight": 60,
                "draw_number": draw,
            }
        )
        draw += 1

    await _execute(admin_supabase.table("users").insert(users))
    await _execute(admin_supabase.table("profiles").insert(profiles))
    await _execute(admin_supabase.table("athletes").insert(athletes))
    await _execute(admin_supabase.table("applications").insert(applications))

    return {"ok": True, "created": count, "category_id": cat_id_str, "competition_id": comp_id_str}


@router.post("/competitions/{comp_id}/seed-fill-round-robin")
async def seed_fill_round_robin(comp_id: UUID, body: SeedFillRoundRobinRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")

    comp_id_str = str(comp_id)
    min_n = int(body.min_per_category)
    max_n = int(body.max_per_category)
    if min_n < 0 or max_n < min_n or max_n > 6:
        raise HTTPException(status_code=400, detail="min/max must be within 0..6 and max>=min")

    cats_res = await _execute(
        admin_supabase.table("competition_categories")
        .select("id")
        .eq("competition_id", comp_id_str)
    )
    category_ids = [str(c["id"]) for c in (cats_res.data or []) if c.get("id")]
    if not category_ids:
        raise HTTPException(status_code=404, detail="No categories found for competition")

    existing_draw_res = await _execute(
        admin_supabase.table("applications")
        .select("draw_number")
        .eq("competition_id", comp_id_str)
        .order("draw_number", desc=True, nullsfirst=False)
        .limit(1)
    )
    max_draw = int((existing_draw_res.data or [{}])[0].get("draw_number") or 0)
    draw = max(max_draw + 1, int(body.start_draw_number))

    created_users = 0
    created_athletes = 0
    created_profiles = 0
    created_apps = 0

    for cat_id_str in category_ids:
        existing_cat_res = await _execute(
            admin_supabase.table("applications")
            .select("id")
            .eq("competition_id", comp_id_str)
            .eq("category_id", cat_id_str)
            .eq("status", "weighed")
            .limit(1)
        )
        if existing_cat_res.data:
            continue

        count = max_n if max_n == min_n else (min_n + (draw % (max_n - min_n + 1)))
        if count == 0:
            continue

        users = []
        athletes = []
        profiles = []
        applications = []

        for _ in range(count):
            user_id = str(uuid4())
            athlete_id = str(uuid4())
            email = f"seed_{comp_id_str[:8]}_{cat_id_str[:8]}_{uuid4().hex[:8]}@example.com"
            full_name = f"Тестовый Спортсмен {draw}"

            users.append({"id": user_id, "email": email})
            profiles.append({"user_id": user_id, "full_name": full_name})
            athletes.append({"id": athlete_id, "user_id": user_id, "coach_name": "Тестовый тренер"})
            applications.append(
                {
                    "competition_id": comp_id_str,
                    "athlete_id": athlete_id,
                    "category_id": cat_id_str,
                    "status": "weighed",
                    "declared_weight": 60,
                    "actual_weight": 60,
                    "draw_number": draw,
                }
            )
            draw += 1

        await _execute(admin_supabase.table("users").insert(users))
        await _execute(admin_supabase.table("profiles").insert(profiles))
        await _execute(admin_supabase.table("athletes").insert(athletes))
        await _execute(admin_supabase.table("applications").insert(applications))

        created_users += len(users)
        created_profiles += len(profiles)
        created_athletes += len(athletes)
        created_apps += len(applications)

    return {
        "ok": True,
        "competition_id": comp_id_str,
        "created": {
            "users": created_users,
            "profiles": created_profiles,
            "athletes": created_athletes,
            "applications": created_apps,
        },
        "min_per_category": min_n,
        "max_per_category": max_n,
    }


@router.post("/competitions/{comp_id}/seed-cleanup")
async def cleanup_seed_users(comp_id: UUID, body: CleanupSeedUsersRequest):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")

    if os.getenv("APP_DEBUG") != "1":
        raise HTTPException(status_code=404, detail="Not Found")

    comp_id_str = str(comp_id)
    comp_prefix = comp_id_str[:8]
    email_pattern = f"seed_{comp_prefix}_%@example.com"

    users_res = await _execute(
        admin_supabase.table("users").select("id,email").ilike("email", email_pattern)
    )
    users_rows = users_res.data or []
    user_ids = [str(r["id"]) for r in users_rows if r.get("id")]
    if not user_ids:
        return {"ok": True, "deleted": False, "users": 0, "athletes": 0, "applications": 0, "profiles": 0}

    athletes_res = await _execute(
        admin_supabase.table("athletes").select("id,user_id").in_("user_id", user_ids)
    )
    athletes_rows = athletes_res.data or []
    athlete_ids = [str(r["id"]) for r in athletes_rows if r.get("id")]

    bout_ids: list[str] = []
    if athlete_ids:
        red_res = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id")
            .eq("competition_id", comp_id_str)
            .in_("athlete_red_id", athlete_ids)
        )
        blue_res = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id")
            .eq("competition_id", comp_id_str)
            .in_("athlete_blue_id", athlete_ids)
        )
        winner_res = await _execute(
            admin_supabase.table("competition_bouts")
            .select("id")
            .eq("competition_id", comp_id_str)
            .in_("winner_athlete_id", athlete_ids)
        )
        for res in (red_res, blue_res, winner_res):
            for row in (res.data or []):
                if row.get("id"):
                    bout_ids.append(str(row["id"]))
        bout_ids = list(dict.fromkeys(bout_ids))

    apps_res = await _execute(
        admin_supabase.table("applications")
        .select("id")
        .eq("competition_id", comp_id_str)
        .in_("athlete_id", athlete_ids or ["00000000-0000-0000-0000-000000000000"])
    )
    app_ids = [str(r["id"]) for r in (apps_res.data or []) if r.get("id")]

    counts = {
        "users": len(user_ids),
        "athletes": len(athlete_ids),
        "applications": len(app_ids),
        "profiles": len(user_ids),
        "competition_bouts": len(bout_ids),
    }

    if body.dry_run:
        return {"ok": True, "deleted": False, "email_pattern": email_pattern, **counts}

    if bout_ids:
        for i in range(0, len(bout_ids), 200):
            chunk = bout_ids[i : i + 200]
            await _execute(
                admin_supabase.table("competition_mats")
                .update({"current_bout_id": None})
                .eq("competition_id", comp_id_str)
                .in_("current_bout_id", chunk)
            )
            await _execute(admin_supabase.table("competition_bouts").delete().in_("id", chunk))

    if app_ids:
        for i in range(0, len(app_ids), 200):
            chunk = app_ids[i : i + 200]
            await _execute(admin_supabase.table("applications").delete().in_("id", chunk))

    if athlete_ids:
        for i in range(0, len(athlete_ids), 200):
            chunk = athlete_ids[i : i + 200]
            await _execute(admin_supabase.table("athletes").delete().in_("id", chunk))

    for i in range(0, len(user_ids), 200):
        chunk = user_ids[i : i + 200]
        await _execute(admin_supabase.table("profiles").delete().in_("user_id", chunk))

    for i in range(0, len(user_ids), 200):
        chunk = user_ids[i : i + 200]
        await _execute(admin_supabase.table("user_roles").delete().in_("user_id", chunk))
        await _execute(admin_supabase.table("staff_locations").delete().in_("user_id", chunk))
        await _execute(admin_supabase.table("competition_secretaries").delete().in_("user_id", chunk))

    for i in range(0, len(user_ids), 200):
        chunk = user_ids[i : i + 200]
        await _execute(admin_supabase.table("users").delete().in_("id", chunk))

    return {"ok": True, "deleted": True, "email_pattern": email_pattern, **counts}


@router.post("/categories/{category_id}/move")
async def move_category(category_id: UUID, body: MoveCategoryRequest):
    comp_id_str = str(body.competition_id)
    cat_id_str = str(category_id)
    to_mat = int(body.to_mat_number)
    if to_mat < 1:
        raise HTTPException(status_code=400, detail="Invalid mat number")

    await _ensure_category_assignments(comp_id_str, {cat_id_str: to_mat})

    existing = await _execute(
        supabase.table("competition_bouts")
        .select("id, mat_number, order_in_mat, status")
        .eq("competition_id", comp_id_str)
        .eq("category_id", cat_id_str)
        .in_("status", ["queued", "next"])
        .order("order_in_mat", desc=False)
    )
    bouts = existing.data or []
    if not bouts:
        return {"ok": True, "moved": 0}

    max_on_target = await _execute(
        supabase.table("competition_bouts")
        .select("order_in_mat")
        .eq("competition_id", comp_id_str)
        .eq("mat_number", to_mat)
        .order("order_in_mat", desc=True)
        .limit(1)
    )
    base = int((max_on_target.data or [{}])[0].get("order_in_mat") or 0)
    updates = []
    order = base + 1
    for b in bouts:
        updates.append({"id": str(b["id"]), "mat_number": to_mat, "order_in_mat": order})
        order += 1

    for i in range(0, len(updates), 200):
        chunk = updates[i : i + 200]
        for row in chunk:
            await _execute(
                supabase.table("competition_bouts")
                .update({"mat_number": row["mat_number"], "order_in_mat": row["order_in_mat"]})
                .eq("id", row["id"])
            )

    return {"ok": True, "moved": len(updates), "to_mat_number": to_mat}


@router.post("/bouts/move")
async def move_bouts(body: MoveBoutsRequest):
    comp_id_str = str(body.competition_id)
    to_mat = int(body.to_mat_number)
    if to_mat < 1:
        raise HTTPException(status_code=400, detail="Invalid mat number")
    bout_ids = [str(b) for b in body.bout_ids]
    if not bout_ids:
        return {"ok": True, "moved": 0}

    res = await _execute(
        supabase.table("competition_bouts")
        .select("id, category_id, status")
        .eq("competition_id", comp_id_str)
        .in_("id", bout_ids)
    )
    rows = res.data or []
    if not rows:
        return {"ok": True, "moved": 0}

    categories = {str(r["category_id"]) for r in rows if r.get("category_id")}
    if len(categories) != 1:
        raise HTTPException(status_code=400, detail="Bouts must be from a single category")
    cat_id = next(iter(categories))

    return await move_category(UUID(cat_id), MoveCategoryRequest(competition_id=body.competition_id, to_mat_number=to_mat))
