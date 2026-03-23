import uuid

def generate_round_robin_bouts(participants):
    """
    Generates Round Robin pairings using the Circle Method.
    Returns a list of rounds, where each round is a list of (p1, p2) tuples.
    """
    n = len(participants)
    if n < 2:
        return []
        
    # If odd number of participants, add a dummy "bye" participant
    if n % 2 != 0:
        participants.append(None)
        n += 1
        
    rounds = []
    for i in range(n - 1):
        round_matches = []
        for j in range(n // 2):
            p1 = participants[j]
            p2 = participants[n - 1 - j]
            
            # Skip if one of them is the dummy "bye" participant
            if p1 is not None and p2 is not None:
                round_matches.append((p1, p2))
                
        rounds.append(round_matches)
        
        # Rotate all participants except the first one
        participants = [participants[0]] + [participants[-1]] + participants[1:-1]
        
    return rounds

def generate_double_elimination_bouts(participants):
    """
    Placeholder for Double Elimination logic.
    For now, it just creates a single elimination to avoid crashing, 
    but the real implementation requires generating a full tree structure.
    """
    # TODO: Implement full double elimination bracket graph.
    # Currently fallback to simple pairs for testing UI
    rounds = []
    round_matches = []
    for i in range(0, len(participants) - 1, 2):
        round_matches.append((participants[i], participants[i+1]))
    rounds.append(round_matches)
    return rounds

def generate_bouts_for_competition(comp_id, categories, cat_participants, cat_to_mat):
    all_bouts = []
    
    # We will collect rounds per mat to interleave them
    # Structure: mat_rounds[mat_id][round_index] = [bout1, bout2, ...]
    mat_rounds = {}
    
    for cat in categories:
        cat_id = cat["id"]
        mat_id = cat_to_mat.get(cat_id, 1)
        participants = cat_participants.get(cat_id, [])
        
        if len(participants) < 2:
            continue
            
        if mat_id not in mat_rounds:
            mat_rounds[mat_id] = []
            
        if len(participants) <= 6:
            # Round Robin
            rounds = generate_round_robin_bouts(participants)
            for r_idx, matches in enumerate(rounds):
                bouts_for_round = []
                for match in matches:
                    bout = {
                        "id": str(uuid.uuid4()),
                        "competition_id": comp_id,
                        "category_id": cat_id,
                        "mat_number": mat_id,
                        "bout_order": 0, # Will be set later during interleaving
                        "round_name": f"Круг {r_idx + 1}",
                        "bracket_type": "RR",
                        "red_athlete_id": match[0],
                        "blue_athlete_id": match[1],
                        "status": "pending"
                    }
                    bouts_for_round.append(bout)
                    
                # Ensure the mat_rounds list has enough inner lists
                while len(mat_rounds[mat_id]) <= r_idx:
                    mat_rounds[mat_id].append([])
                    
                mat_rounds[mat_id][r_idx].extend(bouts_for_round)
        else:
            # Double Elimination (Placeholder structure)
            # Just add them as "Round 1" for now
            rounds = generate_double_elimination_bouts(participants)
            for r_idx, matches in enumerate(rounds):
                bouts_for_round = []
                for match in matches:
                    bout = {
                        "id": str(uuid.uuid4()),
                        "competition_id": comp_id,
                        "category_id": cat_id,
                        "mat_number": mat_id,
                        "bout_order": 0,
                        "round_name": f"Сетка А - Круг {r_idx + 1}",
                        "bracket_type": "A",
                        "red_athlete_id": match[0],
                        "blue_athlete_id": match[1],
                        "status": "pending"
                    }
                    bouts_for_round.append(bout)
                while len(mat_rounds[mat_id]) <= r_idx:
                    mat_rounds[mat_id].append([])
                mat_rounds[mat_id][r_idx].extend(bouts_for_round)

    # Now assign bout_order by interleaving rounds on each mat
    for mat_id, rounds_list in mat_rounds.items():
        current_order = 10 # Start at 10, increment by 10
        for r_idx, bouts in enumerate(rounds_list):
            for bout in bouts:
                bout["bout_order"] = current_order
                all_bouts.append(bout)
                current_order += 10
                
    return all_bouts
