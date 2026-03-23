from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from uuid import UUID
from app.core.supabase import supabase

router = APIRouter(prefix="/bouts", tags=["bouts"])

@router.get("/competition/{comp_id}")
async def get_competition_bouts(comp_id: UUID):
    try:
        # Fetch bouts with athlete names and category info
        query = supabase.table("bouts").select(
            "*, red_athlete:applications!red_athlete_id(athlete_name), blue_athlete:applications!blue_athlete_id(athlete_name), category:competition_categories(*)"
        ).eq("competition_id", str(comp_id)).order("mat_number").order("bout_order").execute()
        return query.data
    except Exception as e:
        print(f"Error fetching bouts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.patch("/{bout_id}")
async def update_bout(bout_id: UUID, data: Dict[str, Any]):
    try:
        # Prevent updating to invalid statuses
        if "status" in data and data["status"] not in ["pending", "active", "completed"]:
            raise HTTPException(status_code=400, detail="Invalid status")
            
        update_res = supabase.table("bouts").update(data).eq("id", str(bout_id)).execute()
        if not update_res.data:
            raise HTTPException(status_code=404, detail="Bout not found")
            
        bout = update_res.data[0]
        
        # If bout is completed and has a winner, we might need to advance them to the next bout
        if bout.get("status") == "completed" and bout.get("winner_id"):
            winner_id = bout["winner_id"]
            loser_id = bout["red_athlete_id"] if winner_id == bout["blue_athlete_id"] else bout["blue_athlete_id"]
            
            # Advance winner
            if bout.get("next_bout_id_winner"):
                next_bout_w = supabase.table("bouts").select("*").eq("id", bout["next_bout_id_winner"]).single().execute().data
                # Place in empty slot
                if not next_bout_w.get("red_athlete_id"):
                    supabase.table("bouts").update({"red_athlete_id": winner_id}).eq("id", bout["next_bout_id_winner"]).execute()
                elif not next_bout_w.get("blue_athlete_id"):
                    supabase.table("bouts").update({"blue_athlete_id": winner_id}).eq("id", bout["next_bout_id_winner"]).execute()
                    
            # Advance loser
            if bout.get("next_bout_id_loser"):
                next_bout_l = supabase.table("bouts").select("*").eq("id", bout["next_bout_id_loser"]).single().execute().data
                if not next_bout_l.get("red_athlete_id"):
                    supabase.table("bouts").update({"red_athlete_id": loser_id}).eq("id", bout["next_bout_id_loser"]).execute()
                elif not next_bout_l.get("blue_athlete_id"):
                    supabase.table("bouts").update({"blue_athlete_id": loser_id}).eq("id", bout["next_bout_id_loser"]).execute()
                    
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
        supabase.table("bouts").delete().eq("competition_id", str(comp_id)).execute()
        
        # 2. Get Competition Data
        comp_res = supabase.table("competitions").select("*").eq("id", str(comp_id)).single().execute()
        comp = comp_res.data
        mats_count = comp.get("mats_count") or 1
        
        # 3. Get Categories
        cats_res = supabase.table("competition_categories").select("*").eq("competition_id", str(comp_id)).execute()
        categories = cats_res.data
        
        # 4. Fetch participants for each category
        cat_participants = {}
        for cat in categories:
            apps_res = supabase.table("applications").select("id, draw_number").eq("category_id", cat["id"]).eq("status", "weighed").order("draw_number").execute()
            cat_participants[cat["id"]] = [app["id"] for app in apps_res.data]
            
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
                supabase.table("bouts").insert(chunk).execute()
                
        return {"status": "success", "message": f"Generated {len(bouts_to_insert)} bouts"}
        
    except Exception as e:
        import traceback
        print(f"Error generating brackets: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
