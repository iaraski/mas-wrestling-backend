import argparse
import datetime

from app.core.supabase import admin_supabase


def _parse_year(d: object) -> int:
    if not d:
        return datetime.date.today().year
    s = str(d)
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).year
    except Exception:
        try:
            return datetime.date.fromisoformat(s[:10]).year
        except Exception:
            return datetime.date.today().year


def _in_range(age: int, cat: dict) -> bool:
    amin = cat.get("age_min")
    amax = cat.get("age_max")
    if isinstance(amin, (int, float)) and age < int(amin):
        return False
    if isinstance(amax, (int, float)) and age > int(amax):
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--competition-id", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--only-status", default="pending")
    args = parser.parse_args()

    if not admin_supabase:
        raise SystemExit("admin_supabase not configured")

    comp_id = str(args.competition_id)
    comp = (
        admin_supabase.table("competitions")
        .select("id,name,start_date")
        .eq("id", comp_id)
        .maybe_single()
        .execute()
        .data
        or {}
    )
    year = _parse_year(comp.get("start_date"))

    cats = (
        admin_supabase.table("competition_categories")
        .select("id,competition_id,gender,age_min,age_max,weight_min,weight_max")
        .eq("competition_id", comp_id)
        .limit(10000)
        .execute()
        .data
        or []
    )
    cat_by_id = {str(c["id"]): c for c in cats if c.get("id")}

    apps_q = (
        admin_supabase.table("applications")
        .select("id,athlete_id,category_id,status")
        .eq("competition_id", comp_id)
        .limit(50000)
    )
    if args.only_status:
        apps_q = apps_q.eq("status", str(args.only_status))
    apps = apps_q.execute().data or []

    athlete_ids = sorted({str(a.get("athlete_id")) for a in apps if a.get("athlete_id")})
    passports: list[dict] = []
    for i in range(0, len(athlete_ids), 200):
        passports += (
            admin_supabase.table("passports")
            .select("athlete_id,birth_date")
            .in_("athlete_id", athlete_ids[i : i + 200])
            .execute()
            .data
            or []
        )
    birth_year_by_athlete: dict[str, int] = {}
    for p in passports:
        aid = p.get("athlete_id")
        b = p.get("birth_date")
        if not aid or not b:
            continue
        byear = _parse_year(b)
        birth_year_by_athlete[str(aid)] = byear

    key_to_cats: dict[tuple[object, object, object], list[dict]] = {}
    for c in cats:
        key = (c.get("gender"), c.get("weight_min"), c.get("weight_max"))
        key_to_cats.setdefault(key, []).append(c)

    planned: list[dict] = []
    for a in apps:
        aid = a.get("athlete_id")
        cid = a.get("category_id")
        if not aid or not cid:
            continue
        byear = birth_year_by_athlete.get(str(aid))
        if not byear:
            continue
        age = year - int(byear)
        cur = cat_by_id.get(str(cid))
        if not cur:
            continue
        if _in_range(age, cur):
            continue

        key = (cur.get("gender"), cur.get("weight_min"), cur.get("weight_max"))
        candidates = [c for c in key_to_cats.get(key, []) if _in_range(age, c)]
        candidates.sort(
            key=lambda c: (
                int(c.get("age_max") or 9999) - int(c.get("age_min") or 0),
                str(c.get("id") or ""),
            )
        )
        if not candidates:
            planned.append(
                {
                    "application_id": str(a["id"]),
                    "athlete_id": str(aid),
                    "age": age,
                    "from": f"{cur.get('age_min')}-{cur.get('age_max')}",
                    "to": None,
                    "reason": "no_matching_category_same_weight",
                }
            )
            continue

        tgt = candidates[0]
        planned.append(
            {
                "application_id": str(a["id"]),
                "athlete_id": str(aid),
                "age": age,
                "from": f"{cur.get('age_min')}-{cur.get('age_max')}",
                "to": f"{tgt.get('age_min')}-{tgt.get('age_max')}",
                "from_category_id": str(cur.get("id")),
                "to_category_id": str(tgt.get("id")),
                "weight_min": cur.get("weight_min"),
                "weight_max": cur.get("weight_max"),
                "gender": cur.get("gender"),
            }
        )

    print("competition:", comp.get("name"))
    print("competition_id:", comp_id)
    print("year:", year)
    print("applications_checked:", len(apps))
    print("planned_changes:", len([p for p in planned if p.get('to_category_id')]))
    print("unresolved:", len([p for p in planned if not p.get('to_category_id')]))
    for p in planned[:50]:
        if p.get("to_category_id"):
            print(
                "-",
                p["application_id"],
                "age",
                p["age"],
                "weight",
                p.get("weight_min"),
                p.get("weight_max"),
                p.get("gender"),
                "from",
                p["from"],
                "to",
                p["to"],
            )
        else:
            print("-", p["application_id"], "age", p["age"], "from", p["from"], "unresolved", p["reason"])
    if len(planned) > 50:
        print("... more:", len(planned) - 50)

    if not args.apply:
        return 0

    applied = 0
    for p in planned:
        to_cid = p.get("to_category_id")
        if not to_cid:
            continue
        admin_supabase.table("applications").update({"category_id": to_cid}).eq("id", p["application_id"]).execute()
        applied += 1
    print("applied:", applied)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

