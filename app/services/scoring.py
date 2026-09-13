def available_points(available: float | None) -> float:
    # Provisional bands until final Qanas readiness weights are locked.
    if available is None: return 0
    if available <= 5_000: return 40
    if available <= 10_000: return 35
    if available <= 25_000: return 30
    if available <= 50_000: return 24
    if available <= 100_000: return 16
    if available <= 250_000: return 8
    return 0

def stability_points(sessions: int) -> float:
    return min(max(sessions, 0), 4) * 7.5

def half_level_points(reached: bool) -> float:
    return 20 if reached else 0

def low_distance_points(distance_pct: float | None) -> float:
    if distance_pct is None: return 0
    if distance_pct <= 5: return 10
    if distance_pct <= 10: return 7
    if distance_pct <= 20: return 3
    return 0

def compute_score(available, sessions, half_reached, distance_pct):
    return round(min(100, available_points(available) + stability_points(sessions) + half_level_points(half_reached) + low_distance_points(distance_pct)), 1)
