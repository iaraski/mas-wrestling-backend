import time
from typing import Dict, Tuple

_buckets: Dict[str, Tuple[float, float]] = {}

def allow(key: str, rate_per_minute: float = 10.0, burst: float = 20.0) -> bool:
    now = time.time()
    interval = 60.0
    capacity = burst
    fill_rate = rate_per_minute / interval
    last, tokens = _buckets.get(key, (now, capacity))
    delta = now - last
    tokens = min(capacity, tokens + delta * fill_rate)
    if tokens < 1.0:
        _buckets[key] = (now, tokens)
        return False
    _buckets[key] = (now, tokens - 1.0)
    return True
