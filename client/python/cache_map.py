import time
from llist import *
from tx_clock import *

class _InternalCacheObject(object):
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(repr(self))

    def __repr__(self):
        return type(self).__name__ + str(self.__dict__)

"""
object returned by get method; type of cache_map values
"""
class CacheResult(_InternalCacheObject):
    def __init__(self, value_time, cached_time, value):
        self.value_time = value_time
        self.cached_time = cached_time
        self.value = value

"""
dictionary of cache entries implementing LRU cache
"""
class CacheMap(object):

    class _CacheKey(_InternalCacheObject):
        def __init__(self, table_id, key_id):
            self.table_id = table_id
            self.key_id = key_id

    class _CacheEntry(_InternalCacheObject):
        def __init__(self, cache_key, cache_result):
            self.cache_key = cache_key
            self.cache_result = cache_result

    def __init__(self, max_size):
        self.max_size = max_size

        # Store values as a dictionary of lists:
        # { (table_id, key_id): 
        #       [ (value_time, cached_time, value)]}
        self.cache_map = {}

        # Track the order of entries added to / accessed in 
        # cache to maintain LRU policy
        # [ (table_id, key_id) ]
        # TODO Remove self.lru_list = []
        self.lru_list = dllist()

    def _lru_add(self, entry):
        self.lru_list.appendleft(entry)
        self._lru_evict()

    def _lru_refresh(self, arg):
        if (type(arg) == int):
            i = arg
            lru_list = self.lru_list
            lru_list.appendleft(lru_list.remove(lru_list.nodeat(i)))
            self.lru_list = lru_list
        elif (type(arg) == self._CacheEntry):
            entry = arg
            lru_list = self.lru_list
            for i in xrange(len(lru_list)):
                current_entry = lru_list.nodeat(i).value
                if (current_entry == entry):
                    self._lru_refresh(i)
                    break

    def _lru_evict(self):
        cache_map = self.cache_map
        lru_list = self.lru_list
        max_size = self.max_size
        remove_index = len(lru_list) - max(0, len(lru_list)-max_size)
        for i in xrange(remove_index, len(lru_list)):
            # Remove evicted entries from the cache_map
            node = lru_list.nodeat(i)
            entry = node.value
            cache_key = entry.cache_key
            cache_result = entry.cache_result
            if (cache_key in cache_map):
                cache_values = cache_map[cache_key]
                cache_values.remove(cache_result)
            # Reset the lru_list
            lru_list.remove(node)
        self.lru_list = lru_list

    def _lru_remove(self, entries): 
        lru_list = self.lru_list
        for node in lru_list:
            if (node.value in entries):
                lru_list.remove(node)

    """
    add entry to cache cache_map
    """
    def put(self, read_time, value_time, table_id, key_id, value):
        if (type(read_time) != TxClock):
            read_time = TxClock(read_time)
        if (type(value_time) != TxClock):
            value_time = TxClock(value_time)

        # Add the entry to the cache
        key = self._CacheKey(table_id, key_id)
        value = CacheResult(value_time, read_time, value)
        entry = self._CacheEntry(key, value)

        if (key in self.cache_map):
            current_results = self.cache_map[key]
            refresh_index = None
            for i in xrange(len(current_results)):
                current_result = current_results[i]
                other_vt = current_result.value_time
                other_ct = current_result.cached_time
                if (type(other_vt) != TxClock):
                    raise TypeError("%s is not a TxClock instance" % other_vt)
                if (type(other_ct) != TxClock):
                    raise TypeError("%s is not a TxClock instance" % other_ct)
                if (value_time == other_vt):
                    # Update the cached time of an existing entry
                    current_result.cached_time = max(other_ct, read_time)
                    # Refresh updated value in LRU
                    refresh_index = i
                    # No two list entries have the same value time
                    break
            # Merged new entry with existing entry
            if (refresh_index != None):
                self._lru_refresh(refresh_index)
            # Add new value to existing key
            else:
                current_results.append(value)
                self._lru_add(entry)
        else:
            # Add new key
            self.cache_map[key] = [value]
            self._lru_add(entry)

    """
    lookup entry in cache cache_map
    """
    def get(self, table_id, key_id, read_time=None):
        if (read_time == None):
            read_time = TxClock(time.time())
        elif (type(read_time) != TxClock):
            read_time = TxClock(read_time)

        cache_map = self.cache_map
        key = self._CacheKey(table_id, key_id)
        most_recent_time = None
        most_recent_result = None
        for cache_key in cache_map:
            if (cache_key == key):
                cache_results = cache_map[key]
                for result in cache_results:
                    value_time = result.value_time
                    if (type(value_time) != TxClock):
                        raise TypeError("%s is not a TxClock instance" % value_time)
                    if (value_time <= read_time and 
                        (most_recent_time == None or 
                         value_time > most_recent_time)):
                        most_recent_time = result.value_time
                        most_recent_result = result
        # Update LRU list with entry just accessed
        if (most_recent_result != None):
            entry = self._CacheEntry(key, most_recent_result)
            self._lru_refresh(entry)
        # Give back CacheResult
        return most_recent_result

    def __str__(self):
        return str(self.cache_map)
        