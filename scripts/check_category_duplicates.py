import argparse
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.rest import rest_get


def _norm_iso(dt: str | None) -> str | None:
    if not dt:
        return None
    try:
        parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        return parsed.isoformat().replace("+00:00", "Z")
    except Exception:
        return str(dt)


def _cat_key(cat: dict) -> tuple:
    gender = str(cat.get("gender") or "").lower()
    age_min = int(cat.get("age_min")) if cat.get("age_min") is not None else None
    age_max = int(cat.get("age_max")) if cat.get("age_max") is not None else None
    wmin = float(cat.get("weight_min")) if cat.get("weight_min") is not None else None
    wmax = cat.get("weight_max")
    wmax = float(wmax) if wmax is not None else None
    if wmax is not None and abs(wmax - 999.0) < 1e-6:
        wmax = 999.0
    day = _norm_iso(cat.get("competition_day"))
    mandate = _norm_iso(cat.get("mandate_day"))
    return (
        gender,
        age_min,
        age_max,
        None if wmin is None else round(wmin, 6),
        None if wmax is None else round(wmax, 6),
        day,
        mandate,
    )


async def _fetch_all_categories(competition_id: str | None) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        params: dict[str, str] = {
            "select": "id,competition_id,gender,age_min,age_max,weight_min,weight_max,competition_day,mandate_day",
            "limit": "10000",
            "offset": str(offset),
            "order": "competition_id.asc,id.asc",
        }
        if competition_id:
            params["competition_id"] = f"eq.{competition_id}"
        resp = await rest_get("competition_categories", params, write=True)
        batch = resp.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < 10000:
            break
    return rows


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--competition-id", dest="competition_id", default=None)
    args = parser.parse_args()

    rows = await _fetch_all_categories(args.competition_id)
    by_comp: dict[str, list[dict]] = {}
    for r in rows:
        cid = str(r.get("competition_id") or "")
        if not cid:
            continue
        by_comp.setdefault(cid, []).append(r)

    total_groups = 0
    total_dup_groups = 0
    total_dup_rows = 0

    for comp_id, cats in by_comp.items():
        groups: dict[tuple, list[dict]] = {}
        for c in cats:
            groups.setdefault(_cat_key(c), []).append(c)
        dups = {k: v for k, v in groups.items() if len(v) > 1}
        total_groups += len(groups)
        total_dup_groups += len(dups)
        total_dup_rows += sum(len(v) - 1 for v in dups.values())
        if not dups:
            continue
        print(f"\ncompetition_id={comp_id} duplicates={len(dups)}")
        for k, v in sorted(dups.items(), key=lambda item: len(item[1]), reverse=True):
            ids = [str(x.get("id")) for x in v if x.get("id")]
            print(f"  count={len(v)} ids={ids} key={k}")

    print("\nSummary")
    print(f"  competitions_scanned={len(by_comp)}")
    print(f"  unique_groups={total_groups}")
    print(f"  duplicate_groups={total_dup_groups}")
    print(f"  duplicate_rows_excess={total_dup_rows}")


if __name__ == "__main__":
    import anyio

    anyio.run(main)
