from fastapi import APIRouter, HTTPException
from typing import List, Optional
from uuid import UUID
import random
import math
from app.core.rest import rest_get
from app.schemas.competition import Bracket, Match

router = APIRouter(prefix="/brackets", tags=["brackets"])

@router.get("/{category_id}", response_model=Bracket)
async def get_bracket(category_id: UUID):
    # В реальном проекте здесь будет получение из БД
    # Но сейчас мы генерируем "на лету" или возвращаем ошибку, если еще не создано
    
    # Сначала получим всех одобренных атлетов в этой категории
    try:
        resp = await rest_get(
            "applications",
            {
                "select": "athlete_id,athletes(users!athletes_user_id_fkey(profiles(full_name)))",
                "category_id": f"eq.{str(category_id)}",
                "status": "eq.approved",
                "limit": "10000",
            },
            write=True,
        )
        rows = resp.json()
        if not isinstance(rows, list) or not rows:
             # Return empty bracket structure instead of 404 to avoid frontend crash if no participants yet
             return Bracket(category_id=category_id, type="pending", matches=[])

        athletes = []
        for app in rows:
            full_name = "Unknown"
            try:
                if (app.get("athletes") and 
                    app["athletes"].get("users") and 
                    app["athletes"]["users"].get("profiles")):
                    full_name = app["athletes"]["users"]["profiles"].get("full_name")
            except Exception as e:
                print(f"[Brackets] Error parsing athlete name: {e}")
                
            athletes.append({
                "id": app["athlete_id"],
                "name": full_name or "Unknown"
            })
    except Exception as e:
        print(f"[Brackets] Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        
    # Для примера генерируем сетку Round Robin или Single Elimination
    # В будущем здесь будет сохранение в БД
    if len(athletes) <= 5:
        return generate_round_robin(category_id, athletes)
    else:
        return generate_single_elimination(category_id, athletes)

def generate_round_robin(category_id: UUID, athletes: list) -> Bracket:
    matches = []
    match_count = 1
    for i in range(len(athletes)):
        for j in range(i + 1, len(athletes)):
            matches.append(Match(
                round_number=1,
                match_number=match_count,
                athlete1_id=athletes[i]["id"],
                athlete2_id=athletes[j]["id"],
                athlete1_name=athletes[i]["name"],
                athlete2_name=athletes[j]["name"]
            ))
            match_count += 1
    return Bracket(category_id=category_id, type="round_robin", matches=matches)

def generate_single_elimination(category_id: UUID, athletes: list) -> Bracket:
    # Перемешиваем атлетов
    random.shuffle(athletes)
    
    n = len(athletes)
    # Ближайшая степень двойки сверху
    pow2 = 1 << (n - 1).bit_length()
    
    matches = []
    # Первый раунд
    # Если n не степень двойки, некоторые атлеты проходят автоматом (byes)
    # Но для простоты сейчас сделаем базовую логику
    
    match_count = 1
    for i in range(0, n, 2):
        if i + 1 < n:
            matches.append(Match(
                round_number=1,
                match_number=match_count,
                athlete1_id=athletes[i]["id"],
                athlete2_id=athletes[i+1]["id"],
                athlete1_name=athletes[i]["name"],
                athlete2_name=athletes[i+1]["name"]
            ))
        else:
            # Бай (проход без боя)
            matches.append(Match(
                round_number=1,
                match_number=match_count,
                athlete1_id=athletes[i]["id"],
                athlete1_name=athletes[i]["name"],
                winner_id=athletes[i]["id"] # Сразу победитель
            ))
        match_count += 1
        
    return Bracket(category_id=category_id, type="single_elimination", matches=matches)
