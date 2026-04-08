import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import anyio

from app.core.supabase import admin_supabase


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _date_only(s: str | None) -> str | None:
    dt = _parse_dt(s)
    return dt.date().isoformat() if dt else None


def _fmt_num(value) -> str:
    try:
        if value is None:
            return ""
        if float(value).is_integer():
            return str(int(float(value)))
        return str(value)
    except Exception:
        return str(value)


def _format_category_label(cat: dict, *, competition_start_date: str | None) -> str:
    gender = str(cat.get("gender") or "").lower()
    is_male = gender in ("male", "m")
    is_female = gender in ("female", "f")
    age_min = cat.get("age_min")
    age_max = cat.get("age_max")

    group = "Мужчины" if is_male else "Женщины" if is_female else "Мужчины"
    if age_min == 18 and age_max == 21:
        group = "Юниоры" if is_male else "Юниорки" if is_female else "Юниоры"
    elif isinstance(age_max, int) and age_max < 18:
        group = "Юноши" if is_male else "Девушки" if is_female else "Юноши"

    year = datetime.now().year
    if competition_start_date:
        dt = _parse_dt(competition_start_date)
        if dt:
            year = dt.year

    years = None
    if isinstance(age_min, int) and isinstance(age_max, int):
        years = f"{year - age_max}-{year - age_min} г.р."

    w_min = cat.get("weight_min")
    w_max = cat.get("weight_max")
    if w_max is None or (isinstance(w_max, (int, float)) and float(w_max) >= 999):
        try:
            minv = float(w_min) if w_min is not None else 0.0
        except Exception:
            minv = 0.0
        if minv <= 0:
            weight = "абсолютная"
        else:
            weight = f"{_fmt_num(int(minv))}+ кг"
    else:
        weight = f"до {_fmt_num(w_max)} кг"

    if years:
        return f"{group} {years}, {weight}"
    return f"{group}, {weight}"


@dataclass(frozen=True)
class CatRefCounts:
    applications: int
    bouts: int
    assignments: int


async def _count_refs(comp_id: str, cat_id: str) -> CatRefCounts:
    if not admin_supabase:
        raise RuntimeError("admin_supabase is not configured")
    apps = await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("applications").select("id").eq("competition_id", comp_id).eq("category_id", cat_id).limit(1).execute()
    )
    bouts = await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("competition_bouts").select("id").eq("competition_id", comp_id).eq("category_id", cat_id).limit(1).execute()
    )
    assigns = await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("competition_category_assignments").select("id").eq("competition_id", comp_id).eq("category_id", cat_id).limit(1).execute()
    )
    return CatRefCounts(
        applications=len(apps.data or []),
        bouts=len(bouts.data or []),
        assignments=len(assigns.data or []),
    )


async def _merge_category(comp_id: str, keep_id: str, drop_id: str) -> None:
    if not admin_supabase:
        raise RuntimeError("admin_supabase is not configured")
    await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("applications")
        .update({"category_id": keep_id})
        .eq("competition_id", comp_id)
        .eq("category_id", drop_id)
        .execute()
    )
    await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("competition_bouts")
        .update({"category_id": keep_id})
        .eq("competition_id", comp_id)
        .eq("category_id", drop_id)
        .execute()
    )
    await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("competition_category_assignments")
        .update({"category_id": keep_id})
        .eq("competition_id", comp_id)
        .eq("category_id", drop_id)
        .execute()
    )
    await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("competition_categories").delete().eq("id", drop_id).execute()
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--competition-id", dest="competition_id", default=None)
    parser.add_argument("--apply", action="store_true", default=False)
    parser.add_argument("--force-across-days", action="store_true", default=False)
    args = parser.parse_args()

    if not admin_supabase:
        raise RuntimeError("admin_supabase is not configured (SUPABASE_SERVICE_ROLE_KEY)")

    comps = await anyio.to_thread.run_sync(
        lambda: admin_supabase.table("competitions")
        .select("id,start_date")
        .execute()
    )
    comp_rows = comps.data or []
    comp_start_by_id = {str(c.get("id")): c.get("start_date") for c in comp_rows if c.get("id")}
    comp_ids = list(comp_start_by_id.keys())
    if args.competition_id:
        comp_ids = [str(args.competition_id)]

    total_merged = 0
    total_groups = 0

    for comp_id in comp_ids:
        cats_res = await anyio.to_thread.run_sync(
            lambda: admin_supabase.table("competition_categories")
            .select("id,competition_id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day")
            .eq("competition_id", comp_id)
            .limit(10000)
            .execute()
        )
        cats = cats_res.data or []
        if not cats:
            continue

        comp_start = comp_start_by_id.get(comp_id)
        groups: dict[str, list[dict]] = {}
        for c in cats:
            label = _format_category_label(c, competition_start_date=comp_start)
            groups.setdefault(label, []).append(c)

        dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
        if not dup_groups:
            continue

        print(f"\ncompetition_id={comp_id} duplicate_label_groups={len(dup_groups)}")
        for label, items in sorted(dup_groups.items(), key=lambda kv: len(kv[1]), reverse=True):
            total_groups += 1
            day_set = sorted({(_date_only(i.get("competition_day")), _date_only(i.get("mandate_day"))) for i in items})
            if not args.force_across_days and len(day_set) > 1:
                ids = [str(i.get("id")) for i in items if i.get("id")]
                print(f"  SKIP days_mismatch label={label} ids={ids} days={day_set}")
                continue

            scored: list[tuple[int, str, dict, CatRefCounts]] = []
            for it in items:
                cid = str(it.get("id"))
                refs = await _count_refs(comp_id, cid)
                score = refs.applications * 100 + refs.bouts * 10 + refs.assignments
                scored.append((score, cid, it, refs))
            scored.sort(key=lambda x: (-x[0], x[1]))

            keep_id = scored[0][1]
            print(f"  label={label}")
            print(f"    keep={keep_id} refs={scored[0][3]}")
            for _, cid, _, refs in scored[1:]:
                print(f"    drop={cid} refs={refs}")
                if args.apply:
                    await _merge_category(comp_id, keep_id, cid)
                    total_merged += 1

    print("\nSummary")
    print(f"  groups_seen={total_groups}")
    print(f"  categories_merged={total_merged}")
    if not args.apply:
        print("  mode=dry_run (add --apply to perform cleanup)")


if __name__ == "__main__":
    anyio.run(main)

