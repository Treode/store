import time

from cache_map import *
from client_cache import *
from tx_clock import *

class Transaction(object):
    def __init__(self, cache, read_timestamp=None, max_age=None, no_cache=False):
        if (type(cache) != ClientCache):
            raise TypeError("cache must be a ClientCache instance")
        if (read_timestamp != None and type(read_timestamp) != TxClock):
            raise TypeError("read_timestamp must be a TxClock instance")

        if (read_timestamp == None):
            current_time = time.time()
            read_timestamp = TxClock(current_time)

        self.cache = cache
        self.read_timestamp = read_timestamp
        self.max_age = max_age
        self.no_cache = no_cache

        # Maintain min read time and max value time seen in transaction
        # (Track consistency of transaction)
        self.min_rt = TxClock.MinValue
        self.max_vt = TxClock.MaxValue
        
        # Maintain Transaction View of all operations
        # view: dict of (table,key) -> (op,value)
        self.view = {}

    def _update_min_rt_max_vt(cached_time, value_time):
        if (cached_time < min_rt):
            min_rt = cached_time
        if (value_time > max_vt):
            max_vt = value_time
        # Raise exception if this update makes the transaction inconsistent
        if (max_vt > min_rt):
            raise StaleException(read_txclock=min_rt, value_txclock=max_vt)

    def read(self, table, key, max_age=None, no_cache=False): # raises StaleException
        # Derive most restrictive read parameters
        max_age = min(max_age, self.max_age)
        no_cache = no_cache or self.no_cache
        # Attempt to read value from cache
        cache_result = self.cache.read(read_timestamp, table, key, max_age, no_cache)
        if (cache_result == None):
            # TODO: value tx clock?
            raise StaleException(read_txclock=read_timestamp, None)
        else:
            # Mark this value as a dependency of the transaction in the view
            self.view[(table, key)] = ("hold", None)
            # Extract the value_time, cached_time, and value
            value_time = cache_result.value_time
            cached_time = cache_result.cached_time
            value = cache_result.value
            # Maintain transaction consistency
            self._update_min_rt_max_vt(cached_time, value_time)
            return value

    def write(table, key, value):
        # Note value is JSON
        try:
            current_entry = self.read(table, key)
        except:
            current_entry = None
        finally:
            if (current_entry):
                self.view[(table, key)] = ("update", value)
            else:
                self.view[(table, key)] = ("create", value)

    def delete(table, key):
        self.view[(table, key)] = ("delete", None)

    def commit(): # raises StaleExcpetion 
        # Perform the commit provided no values have changed in the DB
        # since the min read time, i.e. oldest value we read is still fresh
        # TODO: Correct to use min_rt as condition time?
        condition_time = self.min_rt
        return self.cache.write(condition_time, self.view)


# TODO: Check min_rt and max_vt for consistency and throw StaleException as needed