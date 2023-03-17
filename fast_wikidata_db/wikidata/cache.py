from typing import Dict, Any
from collections import deque


class LRUCache:
    def __init__(self, cache_size: int = 1000000):
        self.cache_size = cache_size
        self.queue = deque()
        self.data: Dict[str, Any] = {}

    def __contains__(self, key: str) -> bool:
        return key in self.queue
    
    def __getitem__(self, key: str) -> str:
        if key not in self.queue:
            raise KeyError(key)
        self.queue.remove(key)
        self.queue.append(key)
        return self.data[key]
    
    def __setitem__(self, key: str, value: str) -> None:
        if key in self.queue:
            return
        self.queue.append(key)
        self.data[key] = value
        if len(self.queue) > self.cache_size:
            del self.data[self.queue.popleft()]

    def __len__(self) -> int:
        return len(self.queue)
    
    def __delitem__(self, key: str) -> None:
        del self.data[key]
        self.queue.remove(key)

    def get(self, key: str, default_value=None) -> str:
        if key not in self.queue:
            return default_value
        self.queue.remove(key)
        self.queue.append(key)
        return self.data[key]
    
    def close(self) -> None:
        self.queue.clear()
        self.data.clear()