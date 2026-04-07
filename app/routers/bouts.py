from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from uuid import UUID
from app.core.rest import rest_get, rest_post, rest_patch, rest_delete

router = APIRouter(prefix="/bouts", tags=["bouts"])

@router.get("/competition/{comp_id}")
async def get_competition_bouts(comp_id: UUID):
    try:
        # Fetch bouts with athlete names and category info
        resp = await rest_get(
            "bouts",
            {
                "select": "*,red_athlete:applications!red_athlete_id(athlete_name),blue_athlete:applications!blue_athlete_id(athlete_name),category:competition_categories(*)",
                "competition_id": f"eq.{str(comp_id)}",
                "order": "mat_number.asc,bout_order.asc",
                "limit": "10000",
            },
            write=True,
        )
        rows = resp.json()
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"Error fetching bouts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.patch("/{bout_id}")
async def update_bout(bout_id: UUID, data: Dict[str, Any]):
    try:
        # Prevent updating to invalid statuses
        if "status" in data and data["status"] not in ["pending", "active", "completed"]:
            raise HTTPException(status_code=400, detail="Invalid status")
            
        upd = await rest_patch(
            "bouts",
            {"id": f"eq.{str(bout_id)}"},
            data,
            prefer="return=representation",
        )
        upd_rows = upd.json()
        if not isinstance(upd_rows, list) or not upd_rows:
            raise HTTPException(status_code=404, detail="Bout not found")
            
        bout = upd_rows[0]
        
        # If bout is completed and has a winner, we might need to advance them to the next bout
        if bout.get("status") == "completed" and bout.get("winner_id"):
            winner_id = bout["winner_id"]
            loser_id = bout["red_athlete_id"] if winner_id == bout["blue_athlete_id"] else bout["blue_athlete_id"]
            
            # Advance winner
            if bout.get("next_bout_id_winner"):
                next_resp = await rest_get(
                    "bouts",
                    {"select": "*", "id": f"eq.{str(bout['next_bout_id_winner'])}", "limit": "1"},
                    write=True,
                )
                next_rows = next_resp.json()
                next_bout_w = next_rows[0] if isinstance(next_rows, list) and next_rows else {}
                # Place in empty slot
                if not next_bout_w.get("red_athlete_id"):
                    await rest_patch(
                        "bouts",
                        {"id": f"eq.{str(bout['next_bout_id_winner'])}"},
                        {"red_athlete_id": winner_id},
                        prefer="return=minimal",
                    )
                elif not next_bout_w.get("blue_athlete_id"):
                    await rest_patch(
                        "bouts",
                        {"id": f"eq.{str(bout['next_bout_id_winner'])}"},
                        {"blue_athlete_id": winner_id},
                        prefer="return=minimal",
                    )
                    
            # Advance loser
            if bout.get("next_bout_id_loser"):
                next_resp = await rest_get(
                    "bouts",
                    {"select": "*", "id": f"eq.{str(bout['next_bout_id_loser'])}", "limit": "1"},
                    write=True,
                )
                next_rows = next_resp.json()
                next_bout_l = next_rows[0] if isinstance(next_rows, list) and next_rows else {}
                if not next_bout_l.get("red_athlete_id"):
                    await rest_patch(
                        "bouts",
                        {"id": f"eq.{str(bout['next_bout_id_loser'])}"},
                        {"red_athlete_id": loser_id},
                        prefer="return=minimal",
                    )
                elif not next_bout_l.get("blue_athlete_id"):
                    await rest_patch(
                        "bouts",
                        {"id": f"eq.{str(bout['next_bout_id_loser'])}"},
                        {"blue_athlete_id": loser_id},
                        prefer="return=minimal",
                    )
                    
        return bout
    except Exception as e:
        print(f"Error updating bout: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/competition/{comp_id}/generate")
async def generate_brackets(comp_id: UUID):
    try:
        # This will contain the complex logic for bracket generation.
        # Currently, it acts as a placeholder to delete old bouts and trigger the generation script.
        
        # 1. Delete old bouts
        await rest_delete("bouts", {"competition_id": f"eq.{str(comp_id)}"})
        
        # 2. Get Competition Data
        comp_resp = await rest_get(
            "competitions",
            {"select": "*", "id": f"eq.{str(comp_id)}", "limit": "1"},
            write=True,
        )
        comp_rows = comp_resp.json()
        comp = comp_rows[0] if isinstance(comp_rows, list) and comp_rows else None
        if not isinstance(comp, dict):
            raise HTTPException(status_code=404, detail="Competition not found")
        mats_count = comp.get("mats_count") or 1
        
        # 3. Get Categories
        cats_resp = await rest_get(
            "competition_categories",
            {"select": "*", "competition_id": f"eq.{str(comp_id)}", "limit": "10000"},
            write=True,
        )
        categories = cats_resp.json()
        if not isinstance(categories, list):
            categories = []
        
        # 4. Fetch participants for each category
        apps_resp = await rest_get(
            "applications",
            {
                "select": "id,category_id,draw_number",
                "competition_id": f"eq.{str(comp_id)}",
                "status": "eq.weighed",
                "order": "draw_number.asc",
                "limit": "10000",
            },
            write=True,
        )
        apps_rows = apps_resp.json()
        cat_participants: dict[str, list[str]] = {}
        if isinstance(apps_rows, list):
            for app in apps_rows:
                if not isinstance(app, dict):
                    continue
                cid = app.get("category_id")
                aid = app.get("id")
                if cid and aid:
                    cat_participants.setdefault(str(cid), []).append(str(aid))
            
        # 5. Simple Mat distribution logic
        # Sort categories by size
        sorted_cats = sorted(categories, key=lambda c: len(cat_participants[c["id"]]), reverse=True)
        mats_load = {i: 0 for i in range(1, mats_count + 1)}
        cat_to_mat = {}
        
        for cat in sorted_cats:
            # Find mat with minimum load
            min_mat = min(mats_load, key=mats_load.get)
            cat_to_mat[cat["id"]] = min_mat
            mats_load[min_mat] += len(cat_participants[cat["id"]])
            
        # 6. Generate bouts (Simplified Round Robin for now)
        from app.services.bracket_generator import generate_bouts_for_competition
        
        bouts_to_insert = generate_bouts_for_competition(str(comp_id), categories, cat_participants, cat_to_mat)
        
        if bouts_to_insert:
            # Batch insert
            # Supabase API has a limit, we might need to chunk it if there are thousands
            for i in range(0, len(bouts_to_insert), 100):
                chunk = bouts_to_insert[i:i+100]
                await rest_post("bouts", {}, chunk, prefer="return=minimal")
                
        return {"status": "success", "message": f"Generated {len(bouts_to_insert)} bouts"}
        
    except Exception as e:
        import traceback
        print(f"Error generating brackets: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
