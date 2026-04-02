import time
from threading import Lock
from typing import Any


class TTLCache:
    def __init__(self):
        self._lock = Lock()
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        now = time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at <= now:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: float) -> None:
        expires_at = time.time() + ttl_seconds
        with self._lock:
            self._store[key] = (expires_at, value)

    def invalidate_prefix(self, prefix: str) -> None:
        with self._lock:
            keys = [k for k in self._store.keys() if k.startswith(prefix)]
            for k in keys:
                self._store.pop(k, None)


cache = TTLCache()

