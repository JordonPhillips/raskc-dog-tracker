import functools
import threading
from collections import defaultdict


class Cache(object):
    def __init__(self):
        self._cache = defaultdict(lambda: {})
        self._lock_cache = defaultdict(lambda: threading.Lock())

    def cache(self, func):
        """Wrap a function with a cache."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = hash(func)
            func_cache = self._cache[key]
            func_lock = self._lock_cache[key]
            return self._get_cached_value(
                func, func_cache, func_lock, *args, **kwargs
            )
        return wrapper
    
    def _get_cached_value(self, func, cache, lock, *args, **kwargs):
        """Retrieves a value, using the cached value if available."""
        key = self._get_func_cache_key(*args, **kwargs)
        if key not in cache:
            # I don't *really* care about repeated work, but it's not terribly
            # hard to handle so why not.
            with lock:
                cache[key] = func(*args, **kwargs)
        return cache[key]
        
    def _get_func_cache_key(self, *args, **kwargs):
        """Get a cache key based on function arguments."""
        return hash(frozenset(args)), hash(frozenset(kwargs.items()))



