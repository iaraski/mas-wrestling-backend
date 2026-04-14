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
    active_mats: list[int] | None = None
    finals_mat: int | None = None


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


def _double_elim_is_lb_stage(stage: str | None) -> bool:
    s = str(stage or "").lower()
    return s.startswith("lb") or s.startswith("bye_lb")


def _double_elim_is_wb_stage(stage: str | None) -> bool:
    s = str(stage or "").lower()
    return s == "wb" or s.startswith("bye_wb") or s == "bye"


def _double_elim_round_done(bouts: list[dict], *, stage: str, round_index: int) -> bool:
    for b in bouts:
        if str(b.get("stage") or "") != stage:
            continue
        if int(b.get("round_index") or 0) != int(round_index):
            continue
        if str(b.get("status") or "") != "done":
            return False
    return True


def _double_elim_any_round_exists(bouts: list[dict], *, stage: str, round_index: int) -> bool:
    for b in bouts:
        s = str(b.get("stage") or "")
        if s == stage and int(b.get("round_index") or 0) == int(round_index):
            return True
        if stage == "lb_new" and s == "bye_lb_new" and int(b.get("round_index") or 0) == int(round_index):
            return True
        if stage == "lb_old" and s == "bye_lb_old" and int(b.get("round_index") or 0) == int(round_index):
            return True
        if stage == "wb" and s in ("bye", "bye_wb") and int(b.get("round_index") or 0) == int(round_index):
            return True
    return False


async def _append_competition_bouts(
    *,
    comp_id_str: str,
    mat_number: int,
    rows: list[dict],
) -> int:
    if not rows:
        return 0
    if not admin_supabase:
        return 0
    max_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("order_in_mat")
        .eq("competition_id", comp_id_str)
        .eq("mat_number", int(mat_number))
        .order("order_in_mat", desc=True, nullsfirst=False)
        .limit(1)
    )
    start = int((max_res.data or [{}])[0].get("order_in_mat") or 0)
    order = start
    for r in rows:
        order += 1
        r["order_in_mat"] = order
        r["mat_number"] = int(mat_number)
    for i in range(0, len(rows), 200):
        await _execute(admin_supabase.table("competition_bouts").insert(rows[i : i + 200]))
    return len(rows)


async def _advance_double_elim_for_category(
    *,
    comp_id_str: str,
    cat_id_str: str,
    mat_number: int,
) -> int:
    if not admin_supabase:
        return 0

    cols = "id,athlete_red_id,athlete_blue_id,winner_athlete_id,status,stage,round_index,mat_number,order_in_mat"
    if await _competition_bouts_has_score_columns():
        cols += ",red_wins,blue_wins,wins_to"
    if await _competition_bouts_has_name_columns():
        cols += ",athlete_red_name,athlete_blue_name"
    res = await _execute(
        admin_supabase.table("competition_bouts")
        .select(cols)
        .eq("competition_id", comp_id_str)
        .eq("category_id", cat_id_str)
        .eq("bracket_type", "double_elim")
        .limit(5000)
    )
    bouts: list[dict] = res.data or []
    if not bouts:
        return 0

    withdrawn: set[str] = set()
    for b in bouts:
        stg = str(b.get("stage") or "")
        if stg.startswith("withdrawn_"):
            a = b.get("athlete_red_id")
            c = b.get("athlete_blue_id")
            if a:
                withdrawn.add(str(a))
            if c:
                withdrawn.add(str(c))

    participants: list[str] = []
    for b in bouts:
        a = b.get("athlete_red_id")
        c = b.get("athlete_blue_id")
        if a:
            a_id = str(a)
            if a_id not in withdrawn:
                participants.append(a_id)
        if c:
            c_id = str(c)
            if c_id not in withdrawn:
                participants.append(c_id)
    participants = list(dict.fromkeys([p for p in participants if p]))
    if len(participants) < 2:
        return 0

    region_map = await _get_athlete_region_map(participants)
    name_map = await _get_athlete_name_map(participants)
    has_name_cols = await _competition_bouts_has_name_columns()
    has_scores = await _competition_bouts_has_score_columns()

    def _next_pow2(n: int) -> int:
        p = 1
        while p < n:
            p *= 2
        return p

    def _wb_rounds_total(n: int) -> int:
        p = _next_pow2(n)
        r = 0
        while p > 1:
            p //= 2
            r += 1
        return max(1, r)

    def _stage_str(b: dict) -> str:
        return str(b.get("stage") or "")

    def _is_wb(b: dict) -> bool:
        s = _stage_str(b).lower()
        return s == "wb" or s == "bye" or s.startswith("bye_wb")

    def _wb_round_of(b: dict) -> int:
        s = _stage_str(b).lower()
        if s == "bye":
            return 1
        if s.startswith("bye_wb"):
            tail = s[len("bye_wb") :]
            if tail.isdigit():
                return int(tail)
            return 1
        return int(b.get("round_index") or 1)

    def _lb_round_of(b: dict) -> int | None:
        s = _stage_str(b).lower()
        if s.startswith("bye_lb"):
            tail = s[len("bye_lb") :]
            return int(tail) if tail.isdigit() else None
        if s.startswith("lb"):
            tail = s[len("lb") :]
            return int(tail) if tail.isdigit() else None
        return None

    def _overall_round_for_lb(lb_round: int) -> int:
        return int(lb_round) + 1

    new_rows: list[dict] = []

    def add_bye(stage: str, round_index: int, athlete_id: str):
        row = {
            "competition_id": comp_id_str,
            "category_id": cat_id_str,
            "athlete_red_id": athlete_id,
            "athlete_blue_id": athlete_id,
            "bracket_type": "double_elim",
            "round_index": int(round_index),
            "stage": stage,
            "status": "done",
            "winner_athlete_id": athlete_id,
            "mat_number": int(mat_number),
            "order_in_mat": 0,
        }
        if has_scores:
            row["red_wins"] = 0
            row["blue_wins"] = 0
            row["wins_to"] = 2
        if has_name_cols:
            row["athlete_red_name"] = name_map.get(athlete_id) or ""
            row["athlete_blue_name"] = name_map.get(athlete_id) or ""
        new_rows.append(row)

    def add_bout(stage: str, round_index: int, a_id: str, b_id: str):
        row = {
            "competition_id": comp_id_str,
            "category_id": cat_id_str,
            "athlete_red_id": a_id,
            "athlete_blue_id": b_id,
            "bracket_type": "double_elim",
            "round_index": int(round_index),
            "stage": stage,
            "status": "queued",
            "winner_athlete_id": None,
            "mat_number": int(mat_number),
            "order_in_mat": 0,
        }
        if has_scores:
            row["red_wins"] = 0
            row["blue_wins"] = 0
            row["wins_to"] = 2
        if has_name_cols:
            row["athlete_red_name"] = name_map.get(a_id) or ""
            row["athlete_blue_name"] = name_map.get(b_id) or ""
        new_rows.append(row)

    def _best_cross_pairs(left: list[str], right: list[str], forbidden: set[tuple[str, str]]) -> list[tuple[str, str]]:
        l = [str(x) for x in left if x]
        r = [str(x) for x in right if x]
        if not l or not r:
            return []
        remaining = sorted(r)
        pairs: list[tuple[str, str]] = []
        for a in l:
            if not remaining:
                break
            ra = region_map.get(a)
            candidates = [b for b in remaining if _pair_key(a, b) not in forbidden]
            if not candidates:
                candidates = list(remaining)
            candidates.sort(key=lambda b: (0 if (ra and region_map.get(b) and region_map.get(b) != ra) else 1, b))
            b = candidates[0]
            pairs.append((a, b))
            remaining.remove(b)
        return pairs

    wb_total = _wb_rounds_total(len(participants))

    changed = True
    loops = 0
    while changed and loops < 20:
        loops += 1
        changed = False

        bouts_all = bouts + new_rows

        forbidden: set[tuple[str, str]] = set()
        for b in bouts_all:
            a = str(b.get("athlete_red_id") or "")
            c = str(b.get("athlete_blue_id") or "")
            if not a or not c or a == c:
                continue
            if a in withdrawn or c in withdrawn:
                continue
            forbidden.add(_pair_key(a, c))

        busy_now: set[str] = set()
        for b in bouts_all:
            st = str(b.get("status") or "")
            if st not in ("queued", "next", "running"):
                continue
            a = str(b.get("athlete_red_id") or "")
            c = str(b.get("athlete_blue_id") or "")
            if not a or not c or a == c:
                continue
            busy_now.add(a)
            busy_now.add(c)

        losses: dict[str, int] = {p: 0 for p in participants}
        for b in bouts_all:
            if str(b.get("status") or "") != "done":
                continue
            a = str(b.get("athlete_red_id") or "")
            c = str(b.get("athlete_blue_id") or "")
            if not a or not c or a == c:
                continue
            if a in withdrawn or c in withdrawn:
                continue
            w = str(b.get("winner_athlete_id") or "")
            if not w:
                continue
            loser = c if w == a else a
            losses[loser] = int(losses.get(loser, 0)) + 1

        wb_bouts_by_round: dict[int, list[dict]] = {}
        for b in bouts_all:
            if not _is_wb(b):
                continue
            rr = _wb_round_of(b)
            wb_bouts_by_round.setdefault(rr, []).append(b)

        def wb_exists(r: int) -> bool:
            return bool(wb_bouts_by_round.get(int(r)) or [])

        def wb_done(r: int) -> bool:
            rr = int(r)
            xs = wb_bouts_by_round.get(rr) or []
            if not xs:
                return False
            return all(str(b.get("status") or "") == "done" for b in xs)

        def wb_losers(r: int) -> list[str]:
            rr = int(r)
            out: list[str] = []
            for b in wb_bouts_by_round.get(rr) or []:
                if str(b.get("status") or "") != "done":
                    continue
                a = str(b.get("athlete_red_id") or "")
                c = str(b.get("athlete_blue_id") or "")
                if not a or not c or a == c:
                    continue
                w = str(b.get("winner_athlete_id") or "")
                if not w:
                    continue
                out.append(c if w == a else a)
            return out

        def wb_winners(r: int) -> list[str]:
            rr = int(r)
            out: list[str] = []
            for b in wb_bouts_by_round.get(rr) or []:
                if str(b.get("status") or "") != "done":
                    continue
                w = b.get("winner_athlete_id")
                if w:
                    out.append(str(w))
            return out

        lb_bouts_by_round: dict[int, list[dict]] = {}
        for b in bouts_all:
            lr = _lb_round_of(b)
            if lr is None:
                continue
            lb_bouts_by_round.setdefault(int(lr), []).append(b)

        def lb_exists(lr: int) -> bool:
            return bool(lb_bouts_by_round.get(int(lr)) or [])

        def lb_done(lr: int) -> bool:
            xs = lb_bouts_by_round.get(int(lr)) or []
            if not xs:
                return False
            return all(str(b.get("status") or "") == "done" for b in xs)

        def lb_winners(lr: int) -> list[str]:
            out: list[str] = []
            for b in lb_bouts_by_round.get(int(lr)) or []:
                if str(b.get("status") or "") != "done":
                    continue
                w = b.get("winner_athlete_id")
                if w:
                    out.append(str(w))
            return out

        wb_round_generated = max(wb_bouts_by_round.keys() or [1])
        if wb_round_generated < wb_total and wb_exists(wb_round_generated) and wb_done(wb_round_generated):
            nxt = wb_round_generated + 1
            if not wb_exists(nxt):
                pool = [p for p in participants if losses.get(p, 0) == 0 and p not in busy_now]
                if len(pool) >= 2:
                    pairs = _best_pairs_no_repeat(pool, region_map, forbidden)
                    if len(pairs) * 2 == len(pool):
                        for a_id, b_id in pairs:
                            add_bout("wb", nxt, a_id, b_id)
                            forbidden.add(_pair_key(a_id, b_id))
                        changed = True

        max_lb_generated = max(lb_bouts_by_round.keys() or [0])
        next_lb = max_lb_generated + 1
        if next_lb >= 1 and next_lb <= 2 * max(0, wb_total - 1) and not lb_exists(next_lb):
            if next_lb == 1:
                if wb_total >= 2 and wb_done(2):
                    pool = [a for a in wb_losers(1) if a not in withdrawn and a not in busy_now and losses.get(a, 0) == 1]
                    if pool:
                        bye = None
                        if len(pool) % 2 == 1:
                            bye = pool.pop()
                            add_bye("bye_lb1", _overall_round_for_lb(1), bye)
                        pairs = _best_pairs_no_repeat(pool, region_map, forbidden)
                        if len(pairs) * 2 == len(pool):
                            for a_id, b_id in pairs:
                                add_bout("lb1", _overall_round_for_lb(1), a_id, b_id)
                                forbidden.add(_pair_key(a_id, b_id))
                            changed = True
            elif next_lb % 2 == 1:
                if lb_done(next_lb - 1):
                    pool = [a for a in lb_winners(next_lb - 1) if a not in withdrawn and a not in busy_now and losses.get(a, 0) == 1]
                    if pool:
                        bye = None
                        if len(pool) % 2 == 1:
                            bye = pool.pop()
                            add_bye(f"bye_lb{next_lb}", _overall_round_for_lb(next_lb), bye)
                        pairs = _best_pairs_no_repeat(pool, region_map, forbidden)
                        if len(pairs) * 2 == len(pool):
                            for a_id, b_id in pairs:
                                add_bout(f"lb{next_lb}", _overall_round_for_lb(next_lb), a_id, b_id)
                                forbidden.add(_pair_key(a_id, b_id))
                            changed = True
            else:
                k = next_lb // 2
                if lb_done(next_lb - 1) and wb_done(k + 1):
                    left = [a for a in lb_winners(next_lb - 1) if a not in withdrawn and a not in busy_now and losses.get(a, 0) == 1]
                    right = [a for a in wb_losers(k + 1) if a not in withdrawn and a not in busy_now and losses.get(a, 0) == 1]
                    if left and right:
                        pairs = _best_cross_pairs(left, right, forbidden)
                        used_left = {a for a, _ in pairs}
                        used_right = {b for _, b in pairs}
                        if pairs:
                            for a_id, b_id in pairs:
                                add_bout(f"lb{next_lb}", _overall_round_for_lb(next_lb), a_id, b_id)
                                forbidden.add(_pair_key(a_id, b_id))
                            extras = [a for a in left if a not in used_left] + [b for b in right if b not in used_right]
                            if extras:
                                add_bye(f"bye_lb{next_lb}", _overall_round_for_lb(next_lb), extras[-1])
                            changed = True

        if wb_total >= 2:
            final_exists = any(str(_stage_str(b) or "").lower() == "final" for b in bouts_all)
            last_lb = 2 * max(0, wb_total - 1)
            if not final_exists and wb_done(wb_total) and (last_lb == 0 or lb_done(last_lb)):
                wb_champs = wb_winners(wb_total)
                lb_champs = lb_winners(last_lb) if last_lb > 0 else []
                if wb_champs and lb_champs:
                    add_bout("final", 2 * wb_total, wb_champs[0], lb_champs[0])
                    changed = True

    return await _append_competition_bouts(comp_id_str=comp_id_str, mat_number=mat_number, rows=new_rows)

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


async def _get_weight_map_for_category(db, *, comp_id_str: str, cat_id_str: str) -> dict[str, float]:
    try:
        apps_res = await _execute(
            db.table("applications")
            .select("athlete_id,actual_weight,declared_weight")
            .eq("competition_id", comp_id_str)
            .eq("category_id", cat_id_str)
            .limit(10000)
        )
        weight_map: dict[str, float] = {}
        for a in (apps_res.data or []):
            a_id = a.get("athlete_id")
            if not a_id:
                continue
            w = a.get("actual_weight")
            if w is None:
                w = a.get("declared_weight")
            if w is None:
                continue
            try:
                weight_map[str(a_id)] = float(w)
            except Exception:
                continue
        return weight_map
    except Exception:
        return {}


def _round_robin_rank_from_bouts(
    *,
    bouts: list[dict],
    has_scores: bool,
    weight_map: dict[str, float],
) -> list[dict]:
    done = [b for b in bouts if b.get("status") == "done" and b.get("winner_athlete_id")]

    stats: dict[str, dict] = {}

    def ensure(a_id: str):
        if a_id not in stats:
            stats[a_id] = {
                "athlete_id": a_id,
                "wins": 0,
                "losses": 0,
                "played": 0,
                "match_points": 0,
                "clean_wins": 0,
                "points": 0,
                "points_against": 0,
            }

    head_to_head: dict[tuple[str, str], int] = {}

    for b in done:
        red = str(b.get("athlete_red_id") or "")
        blue = str(b.get("athlete_blue_id") or "")
        winner = str(b.get("winner_athlete_id") or "")
        if not red or not blue or not winner:
            continue
        ensure(red)
        ensure(blue)
        stats[red]["played"] += 1
        stats[blue]["played"] += 1
        if has_scores:
            rw = int(b.get("red_wins") or 0)
            bw = int(b.get("blue_wins") or 0)
            wins_to = int(b.get("wins_to") or 2)
            stats[red]["points"] += rw
            stats[red]["points_against"] += bw
            stats[blue]["points"] += bw
            stats[blue]["points_against"] += rw
        if winner == red:
            stats[red]["wins"] += 1
            stats[blue]["losses"] += 1
            head_to_head[(red, blue)] = 1
            head_to_head[(blue, red)] = 0
            if has_scores:
                winner_rounds = int(rw)
                loser_rounds = int(bw)
                mp = 2 if (winner_rounds >= wins_to and loser_rounds == 0) else 1
                stats[red]["match_points"] += mp
                if mp == 2:
                    stats[red]["clean_wins"] += 1
        else:
            stats[blue]["wins"] += 1
            stats[red]["losses"] += 1
            head_to_head[(blue, red)] = 1
            head_to_head[(red, blue)] = 0
            if has_scores:
                winner_rounds = int(bw)
                loser_rounds = int(rw)
                mp = 2 if (winner_rounds >= wins_to and loser_rounds == 0) else 1
                stats[blue]["match_points"] += mp
                if mp == 2:
                    stats[blue]["clean_wins"] += 1

    rows = list(stats.values())
    rows.sort(
        key=lambda r: (
            -int(r["wins"]),
            -int(r.get("match_points") or 0),
            -int(r.get("clean_wins") or 0),
            -int(r.get("points") or 0),
            int(r.get("points_against") or 0),
            r["athlete_id"],
        )
    )

    if len(rows) == 3:
        w0 = int(rows[0]["wins"])
        mp0 = int(rows[0].get("match_points") or 0)
        if all(int(r["wins"]) == w0 and int(r.get("match_points") or 0) == mp0 for r in rows):
            rows.sort(
                key=lambda r: (
                    weight_map.get(r["athlete_id"], 10**9),
                    r["athlete_id"],
                )
            )
    elif 4 <= len(rows) <= 6:
        groups: dict[tuple[int, int], list[dict]] = {}
        for r in rows:
            key = (int(r["wins"]), int(r.get("match_points") or 0))
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
            key = (int(r["wins"]), int(r.get("match_points") or 0))
            group = groups.get(key, [r])
            for g in group:
                if g["athlete_id"] not in used:
                    ordered.append(g)
                    used.add(g["athlete_id"])
        rows = ordered

    return rows


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
    weight_map = await _get_weight_map_for_category(db, comp_id_str=comp_id_str, cat_id_str=cat_id_str)
    rows = _round_robin_rank_from_bouts(bouts=bouts, has_scores=has_scores, weight_map=weight_map)
    athlete_ids = [r["athlete_id"] for r in rows]

    names = await _get_athlete_name_map(athlete_ids)
    for r in rows:
        r["name"] = names.get(r["athlete_id"]) or ""

    total_bouts = len(
        [
            b
            for b in bouts
            if b.get("athlete_red_id")
            and b.get("athlete_blue_id")
            and str(b.get("status") or "") != "cancelled"
            and not str(b.get("stage") or "").startswith("withdrawn_")
        ]
    )
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


def _category_stats_is_in_scope(b: dict) -> bool:
    if not b.get("athlete_red_id") or not b.get("athlete_blue_id"):
        return False
    if str(b.get("athlete_red_id")) == str(b.get("athlete_blue_id")):
        return False
    if str(b.get("status") or "") == "cancelled":
        return False
    if str(b.get("stage") or "").startswith("withdrawn_"):
        return False
    return True


def _double_elim_rank_from_bouts(bouts: list[dict]) -> list[dict]:
    participants: set[str] = set()
    withdrawn: set[str] = set()
    for b in bouts:
        stg = str(b.get("stage") or "")
        if stg.startswith("withdrawn_"):
            a = b.get("athlete_red_id")
            c = b.get("athlete_blue_id")
            if a:
                withdrawn.add(str(a))
            if c:
                withdrawn.add(str(c))
    for b in bouts:
        if not _category_stats_is_in_scope(b):
            continue
        a = str(b.get("athlete_red_id") or "")
        c = str(b.get("athlete_blue_id") or "")
        if not a or not c:
            continue
        if a in withdrawn or c in withdrawn:
            continue
        participants.add(a)
        participants.add(c)

    losses: dict[str, int] = {a: 0 for a in participants}
    wins: dict[str, int] = {a: 0 for a in participants}
    played: dict[str, int] = {a: 0 for a in participants}
    for b in bouts:
        if not _category_stats_is_in_scope(b):
            continue
        if str(b.get("status") or "") != "done":
            continue
        a = str(b.get("athlete_red_id") or "")
        c = str(b.get("athlete_blue_id") or "")
        if not a or not c or a == c:
            continue
        if a not in participants or c not in participants:
            continue
        w = str(b.get("winner_athlete_id") or "")
        if not w:
            continue
        played[a] += 1
        played[c] += 1
        loser = c if w == a else a
        losses[loser] = int(losses.get(loser, 0)) + 1
        wins[w] = int(wins.get(w, 0)) + 1

    rows = []
    for a in participants:
        rows.append(
            {
                "athlete_id": a,
                "losses": int(losses.get(a, 0)),
                "wins": int(wins.get(a, 0)),
                "played": int(played.get(a, 0)),
            }
        )
    rows.sort(key=lambda r: (int(r["losses"]), -int(r["wins"]), -int(r["played"]), r["athlete_id"]))
    return rows


@router.get("/competitions/{comp_id}/results")
async def get_competition_results(comp_id: UUID):
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    comp_id_str = str(comp_id)

    comp_res = await _execute(
        admin_supabase.table("competitions").select("id,name,start_date").eq("id", comp_id_str).single()
    )
    comp = comp_res.data or {}
    comp_start = comp.get("start_date")

    cats_res = await _execute(
        admin_supabase.table("competition_categories")
        .select("id,gender,age_min,age_max,weight_min,weight_max")
        .eq("competition_id", comp_id_str)
        .limit(10000)
    )
    categories = {str(c["id"]): c for c in (cats_res.data or []) if c.get("id")}

    bouts_res = await _select_competition_bouts_for_comp(comp_id_str)
    bouts_all = bouts_res.data or []

    by_cat: dict[str, list[dict]] = {}
    for b in bouts_all:
        cat_id = b.get("category_id")
        if not cat_id:
            continue
        by_cat.setdefault(str(cat_id), []).append(b)

    has_scores = await _competition_bouts_has_score_columns()

    categories_out: list[dict] = []
    champions_out: list[dict] = []

    total_all = 0
    done_all = 0
    remaining_all = 0

    for cat_id, bouts in by_cat.items():
        scoped = [b for b in bouts if _category_stats_is_in_scope(b)]
        total_bouts = len(scoped)
        done_bouts = len([b for b in scoped if str(b.get("status") or "") == "done" and b.get("winner_athlete_id")])
        remaining = len([b for b in scoped if str(b.get("status") or "") in ("queued", "next", "running")])

        total_all += total_bouts
        done_all += done_bouts
        remaining_all += remaining

        bracket_types = {str(b.get("bracket_type") or "") for b in scoped if b.get("bracket_type")}
        bracket_type = sorted(bracket_types)[0] if bracket_types else None

        winners: list[dict] = []
        is_finished = bool(total_bouts > 0 and done_bouts == total_bouts and remaining == 0)

        if is_finished and bracket_type == "round_robin":
            weight_map = await _get_weight_map_for_category(
                admin_supabase, comp_id_str=comp_id_str, cat_id_str=cat_id
            )
            ranked = _round_robin_rank_from_bouts(bouts=scoped, has_scores=has_scores, weight_map=weight_map)
            top = ranked[:3]
            athlete_ids = [r["athlete_id"] for r in top]
            names = await _get_athlete_name_map(athlete_ids)
            for idx, r in enumerate(top, start=1):
                winners.append({"place": idx, "athlete_id": r["athlete_id"], "name": names.get(r["athlete_id"]) or ""})

        elif is_finished and bracket_type == "double_elim":
            ranked = _double_elim_rank_from_bouts(scoped)
            top = ranked[:3]
            athlete_ids = [r["athlete_id"] for r in top]
            names = await _get_athlete_name_map(athlete_ids)
            for idx, r in enumerate(top, start=1):
                winners.append({"place": idx, "athlete_id": r["athlete_id"], "name": names.get(r["athlete_id"]) or ""})

        label = ""
        cat = categories.get(cat_id)
        if cat:
            label = _category_label(cat, at_date=comp_start)

        cat_out = {
            "category_id": cat_id,
            "label": label,
            "bracket_type": bracket_type,
            "total_bouts": total_bouts,
            "done_bouts": done_bouts,
            "is_finished": is_finished,
            "winners": winners,
        }
        categories_out.append(cat_out)

        if winners:
            champ = winners[0]
            champions_out.append(
                {
                    "category_id": cat_id,
                    "category_label": label,
                    "athlete_id": champ["athlete_id"],
                    "name": champ.get("name") or "",
                }
            )

    categories_out.sort(key=lambda x: x.get("label") or x.get("category_id") or "")
    champions_out.sort(key=lambda x: x.get("category_label") or x.get("category_id") or "")

    has_started_res = await _execute(
        admin_supabase.table("competition_bouts").select("id").eq("competition_id", comp_id_str).in_("status", ["running", "done"]).limit(1)
    )
    has_started = bool(has_started_res.data)
    is_finished = bool(has_started and remaining_all == 0 and total_all > 0 and done_all == total_all)

    return {
        "competition": {"id": comp_id_str, "name": comp.get("name"), "is_finished": is_finished},
        "totals": {"total_bouts": total_all, "done_bouts": done_all, "remaining_bouts": remaining_all},
        "categories": categories_out,
        "champions": champions_out,
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


def _pair_key(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a <= b else (b, a)


def _pair_same_region_count(pairs: list[tuple[str, str]], region_by_athlete: dict[str, str]) -> int:
    same = 0
    for a, b in pairs:
        ra = region_by_athlete.get(a)
        rb = region_by_athlete.get(b)
        if ra and rb and ra == rb:
            same += 1
    return same


def _best_pairs_no_repeat(
    athlete_ids: list[str],
    region_by_athlete: dict[str, str],
    forbidden: set[tuple[str, str]],
) -> list[tuple[str, str]]:
    ids = [str(x) for x in athlete_ids if x]
    if len(ids) < 2:
        return []

    if len(ids) > 16:
        best: list[tuple[str, str]] = []
        best_same = 10**9
        for _ in range(3000):
            remaining = list(ids)
            random.shuffle(remaining)
            pairs: list[tuple[str, str]] = []
            ok = True
            while len(remaining) >= 2:
                a = remaining.pop(0)
                candidates = list(remaining)
                random.shuffle(candidates)
                chosen = None
                for b in candidates:
                    if _pair_key(a, b) in forbidden:
                        continue
                    chosen = b
                    break
                if chosen is None:
                    ok = False
                    break
                remaining.remove(chosen)
                pairs.append((a, chosen))
            if not ok:
                continue
            same = _pair_same_region_count(pairs, region_by_athlete)
            if same < best_same:
                best_same = same
                best = pairs
                if best_same == 0:
                    break
        return best

    best_solution: list[tuple[str, str]] | None = None
    best_same = 10**9

    def backtrack(remaining: list[str], acc: list[tuple[str, str]]):
        nonlocal best_solution, best_same
        if not remaining:
            same = _pair_same_region_count(acc, region_by_athlete)
            if same < best_same:
                best_same = same
                best_solution = list(acc)
            return

        if best_same == 0:
            return

        a = remaining[0]
        ra = region_by_athlete.get(a)
        options = remaining[1:]
        options.sort(key=lambda b: 0 if (ra and region_by_athlete.get(b) and region_by_athlete.get(b) != ra) else 1)
        for b in options:
            if _pair_key(a, b) in forbidden:
                continue
            nxt = [x for x in remaining if x not in (a, b)]
            acc.append((a, b))
            backtrack(nxt, acc)
            acc.pop()

    backtrack(sorted(ids), [])
    return best_solution or []


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


def _category_label(cat: dict, *, at_date: str | None = None) -> str:
    gender = str(cat.get("gender") or "").lower()
    is_male = gender in ("male", "m")
    is_female = gender in ("female", "f")

    age_min = cat.get("age_min")
    age_max = cat.get("age_max")
    w_min = cat.get("weight_min")
    w_max = cat.get("weight_max")

    group = "Мужчины" if is_male else "Женщины" if is_female else "Мужчины"
    if age_min == 18 and age_max == 21:
        group = "Юниоры" if is_male else "Юниорки" if is_female else "Юниоры"
    elif isinstance(age_max, int) and age_max < 18:
        group = "Юноши" if is_male else "Девушки" if is_female else "Юноши"

    year = datetime.now().year
    if at_date:
        try:
            year = datetime.fromisoformat(str(at_date).replace("Z", "+00:00")).year
        except Exception:
            year = datetime.now().year
    years_part = ""
    if isinstance(age_min, int) and isinstance(age_max, int):
        years_part = f"{year - age_max}-{year - age_min} г.р."

    def _fmt_num(x):
        try:
            xi = int(x)
            if float(x) == float(xi):
                return str(xi)
        except Exception:
            pass
        return str(x)

    if w_max is None or (isinstance(w_max, (int, float)) and float(w_max) >= 999):
        minv = float(w_min) if w_min is not None else 0.0
        if minv <= 0:
            weight_part = "абсолютная"
        else:
            weight_part = f"{_fmt_num(int(minv))}+ кг"
    else:
        weight_part = f"до {_fmt_num(w_max)} кг"

    if years_part:
        return f"{group} {years_part}, {weight_part}"
    return f"{group}, {weight_part}"


def _balanced_assignments(
    categories: list[dict],
    weighed_counts: dict[str, int],
    mats_count: int,
    existing_assignments: dict[str, int],
    allowed_mats: list[int] | None = None,
) -> dict[str, int]:
    mats_count = max(1, int(mats_count))
    allowed = [int(m) for m in (allowed_mats or []) if int(m) >= 1 and int(m) <= mats_count]
    allowed = list(dict.fromkeys(allowed))
    if not allowed:
        allowed = list(range(1, mats_count + 1))

    mats_load = {m: 0 for m in allowed}
    result: dict[str, int] = {}

    for cat_id, mat in existing_assignments.items():
        m = int(mat)
        if m not in mats_load:
            continue
        result[str(cat_id)] = m
        mats_load[m] = mats_load.get(m, 0) + int(weighed_counts.get(str(cat_id), 0))

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
    rows = [r for r in rows if r.get("athlete_red_id") != r.get("athlete_blue_id")]
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
    mats_res = await _execute(admin_supabase.table("competitions").select("mats_count,start_date").eq("id", comp_id_str).single())
    mats_count = int((mats_res.data or {}).get("mats_count") or 1)
    comp_start = (mats_res.data or {}).get("start_date")

    started_res = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id, athlete_red_id, athlete_blue_id")
        .eq("competition_id", comp_id_str)
        .in_("status", ["running", "done"])
        .limit(1000)
    )
    real_started = [r for r in (started_res.data or []) if r.get("athlete_red_id") != r.get("athlete_blue_id")]
    if real_started and not body.force_regenerate:
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

    active_categories = []
    for cat in categories:
        cat_id = str(cat["id"])
        if len(cat_to_athletes.get(cat_id) or []) >= 2:
            active_categories.append(cat)

    existing_assignments_res = await _execute(
        admin_supabase.table("competition_category_assignments")
        .select("category_id, mat_number")
        .eq("competition_id", comp_id_str)
    )
    existing_assignments = {
        str(r["category_id"]): int(r["mat_number"]) for r in (existing_assignments_res.data or []) if r.get("category_id")
    }

    allowed_mats = body.active_mats
    finals_mat = body.finals_mat
    if allowed_mats is not None:
        allowed_mats = [int(m) for m in allowed_mats if int(m) >= 1 and int(m) <= mats_count]
        allowed_mats = list(dict.fromkeys(allowed_mats))
    if finals_mat is not None:
        fm = int(finals_mat)
        if fm < 1 or fm > mats_count:
            raise HTTPException(status_code=400, detail="finals_mat must be between 1 and mats_count")
        if allowed_mats:
            allowed_mats = [int(m) for m in allowed_mats if int(m) != fm]
    if body.active_mats is not None and not allowed_mats:
        raise HTTPException(status_code=400, detail="active_mats must include at least one mat")
    assignments = _balanced_assignments(
        active_categories,
        weighed_counts,
        mats_count,
        {} if body.rebalance_assignments else existing_assignments,
        allowed_mats=allowed_mats,
    )

    if assignments:
        await _ensure_category_assignments(comp_id_str, assignments)
        keep = set(assignments.keys())
        if existing_assignments:
            to_delete = [cid for cid in existing_assignments.keys() if cid not in keep]
            for i in range(0, len(to_delete), 200):
                chunk = to_delete[i : i + 200]
                await _execute(
                    admin_supabase.table("competition_category_assignments")
                    .delete()
                    .eq("competition_id", comp_id_str)
                    .in_("category_id", chunk)
                )
    else:
        if existing_assignments:
            await _execute(
                admin_supabase.table("competition_category_assignments")
                .delete()
                .eq("competition_id", comp_id_str)
            )
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
    cat_label_by_id = {str(c["id"]): _category_label(c, at_date=comp_start) for c in active_categories if c.get("id")}
    seq = 0

    for cat in active_categories:
        cat_id = str(cat["id"])
        athlete_ids = cat_to_athletes.get(cat_id, [])
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
                if score_cols:
                    bye_row["red_wins"] = 0
                    bye_row["blue_wins"] = 0
                    bye_row["wins_to"] = 2
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
        admin_supabase.table("competitions").select("id,mats_count,name,start_date").eq("id", comp_id_str).single()
    )
    comp = comp_res.data or {}
    mats_count = int(comp.get("mats_count") or 1)
    comp_start = comp.get("start_date")

    cats_res = await _execute(
        admin_supabase.table("competition_categories").select("id,gender,age_min,age_max,weight_min,weight_max").eq("competition_id", comp_id_str)
    )
    categories = {str(c["id"]): c for c in (cats_res.data or []) if c.get("id")}

    assigns_res = await _execute(
        admin_supabase.table("competition_category_assignments").select("category_id,mat_number").eq("competition_id", comp_id_str)
    )
    assigns = assigns_res.data or []

    bouts_res = await _select_competition_bouts_for_comp(comp_id_str)
    bouts_all = bouts_res.data or []
    cats_with_bouts = {str(b.get("category_id")) for b in bouts_all if b.get("category_id")}

    cats_by_mat: dict[int, list[dict]] = {m: [] for m in range(1, mats_count + 1)}
    for a in assigns:
        cat_id = str(a.get("category_id") or "")
        mat = int(a.get("mat_number") or 0)
        if mat < 1 or mat > mats_count:
            continue
        if cat_id and cat_id not in cats_with_bouts:
            continue
        cat = categories.get(cat_id)
        if not cat:
            continue
        cats_by_mat[mat].append({"id": cat_id, "label": _category_label(cat, at_date=comp_start)})

    mats_res = await _execute(
        admin_supabase.table("competition_mats").select("mat_number,current_bout_id").eq("competition_id", comp_id_str)
    )
    mats_rows = mats_res.data or []

    active_bouts = [
        b
        for b in bouts_all
        if b.get("status") in ("queued", "next", "running")
        and b.get("athlete_red_id") != b.get("athlete_blue_id")
    ]
    bye_bouts = [
        b
        for b in bouts_all
        if str(b.get("status") or "") == "done"
        and b.get("athlete_red_id") == b.get("athlete_blue_id")
        and str(b.get("stage") or "").startswith("bye")
    ]
    display_bouts = await _materialize_names_for_bouts(active_bouts + bye_bouts)
    has_bouts = bool(active_bouts)
    started_bouts = [
        b
        for b in bouts_all
        if b.get("status") in ("running", "done")
        and b.get("athlete_red_id") != b.get("athlete_blue_id")
    ]
    has_started = bool(started_bouts)

    scoped_all = [b for b in bouts_all if _category_stats_is_in_scope(b)]
    total_bouts = len(scoped_all)
    done_bouts = len([b for b in scoped_all if str(b.get("status") or "") == "done" and b.get("winner_athlete_id")])
    remaining_bouts = len([b for b in scoped_all if str(b.get("status") or "") in ("queued", "next", "running")])
    is_finished = bool(has_started and total_bouts > 0 and remaining_bouts == 0 and done_bouts == total_bouts)
    bouts_by_mat: dict[int, list[dict]] = {m: [] for m in range(1, mats_count + 1)}
    byes_by_mat: dict[int, list[dict]] = {m: [] for m in range(1, mats_count + 1)}
    for b in display_bouts:
        mat = int(b.get("mat_number") or 0)
        if mat < 1 or mat > mats_count:
            continue
        if b.get("athlete_red_id") == b.get("athlete_blue_id") and str(b.get("stage") or "").startswith("bye"):
            byes_by_mat[mat].append(b)
        elif str(b.get("status") or "") in ("queued", "next", "running"):
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
        byes_for_mat = byes_by_mat.get(m, [])
        if rounds_window:
            byes_for_mat = [b for b in byes_for_mat if int(b.get("round_index") or 0) in rounds_window]
        byes_for_mat = sorted(byes_for_mat, key=lambda x: (int(x.get("round_index") or 0), int(x.get("order_in_mat") or 0)))
        if byes_for_mat:
            queue_bouts = queue_bouts + byes_for_mat

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
        history_bouts = [
            b
            for b in history_bouts
            if b.get("athlete_red_id") != b.get("athlete_blue_id")
            and not str(b.get("stage") or "").startswith("bye")
        ]

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
        "competition": {
            "id": comp_id_str,
            "name": comp.get("name"),
            "mats_count": mats_count,
            "has_bouts": has_bouts,
            "has_started": has_started,
            "is_finished": is_finished,
            "total_bouts": total_bouts,
            "done_bouts": done_bouts,
            "remaining_bouts": remaining_bouts,
            "results_path": f"/api/v1/live/competitions/{comp_id_str}/results",
        },
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
        .select("id,competition_id,category_id,bracket_type,stage,round_index,mat_number,status,athlete_red_id,athlete_blue_id,winner_athlete_id,red_wins,blue_wins,wins_to")
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
    cat_id_str = str(bout.get("category_id") or "")
    bracket_type = str(bout.get("bracket_type") or "")
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
            if bracket_type == "double_elim" and cat_id_str and mat_number > 0:
                await _advance_double_elim_for_category(comp_id_str=comp_id_str, cat_id_str=cat_id_str, mat_number=mat_number)
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
        if bracket_type == "double_elim" and cat_id_str and mat_number > 0:
            await _advance_double_elim_for_category(comp_id_str=comp_id_str, cat_id_str=cat_id_str, mat_number=mat_number)
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
    if not admin_supabase:
        raise HTTPException(status_code=500, detail="Service role not configured")
    comp_id_str = str(body.competition_id)
    cat_id_str = str(category_id)
    to_mat = int(body.to_mat_number)
    mats_count = await _get_mats_count(comp_id_str)
    if to_mat < 1 or to_mat > mats_count:
        raise HTTPException(status_code=400, detail="Invalid mat number")

    await _ensure_category_assignments(comp_id_str, {cat_id_str: to_mat})

    existing = await _execute(
        admin_supabase.table("competition_bouts")
        .select("id, mat_number, order_in_mat, status")
        .eq("competition_id", comp_id_str)
        .eq("category_id", cat_id_str)
        .in_("status", ["queued", "next", "running"])
        .order("order_in_mat", desc=False)
    )
    bouts = existing.data or []
    if not bouts:
        return {"ok": True, "moved": 0}

    max_on_target = await _execute(
        admin_supabase.table("competition_bouts")
        .select("order_in_mat")
        .eq("competition_id", comp_id_str)
        .eq("mat_number", to_mat)
        .in_("status", ["queued", "next", "running"])
        .order("order_in_mat", desc=True)
        .limit(1)
    )
    base = int((max_on_target.data or [{}])[0].get("order_in_mat") or 0)
    affected_mats = {int(b.get("mat_number") or 0) for b in bouts if int(b.get("mat_number") or 0) > 0}
    affected_mats.add(int(to_mat))

    running_bout_id: str | None = None
    running_from_mat: int | None = None
    for b in bouts:
        if str(b.get("status") or "") == "running":
            running_bout_id = str(b.get("id"))
            running_from_mat = int(b.get("mat_number") or 0) or None
            break

    priority = {"running": 0, "next": 1, "queued": 2}
    bouts_sorted = sorted(
        bouts,
        key=lambda x: (
            priority.get(str(x.get("status") or ""), 9),
            int(x.get("order_in_mat") or 0),
        ),
    )
    updates = []
    order = base + 1
    for b in bouts_sorted:
        updates.append({"id": str(b["id"]), "mat_number": to_mat, "order_in_mat": order})
        order += 1

    for i in range(0, len(updates), 200):
        chunk = updates[i : i + 200]
        for row in chunk:
            await _execute(
                admin_supabase.table("competition_bouts")
                .update({"mat_number": row["mat_number"], "order_in_mat": row["order_in_mat"]})
                .eq("id", row["id"])
            )

    if running_bout_id and running_from_mat and running_from_mat != int(to_mat):
        await _execute(
            admin_supabase.table("competition_mats")
            .update({"current_bout_id": None})
            .eq("competition_id", comp_id_str)
            .eq("mat_number", int(running_from_mat))
            .eq("current_bout_id", running_bout_id)
        )
        await _execute(
            admin_supabase.table("competition_mats")
            .update({"current_bout_id": running_bout_id})
            .eq("competition_id", comp_id_str)
            .eq("mat_number", int(to_mat))
        )

    for m in sorted(affected_mats):
        await _set_next_for_mat(comp_id_str, int(m))

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
