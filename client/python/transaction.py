# Copyright 2014 Treode, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

from cache_map import *
from client_cache import *
from tx_clock import *
from tx_view import *

class Transaction(object):

    def __init__(self, cache, read_timestamp=None, max_age=None, no_cache=False):
        if (read_timestamp != None and type(read_timestamp) != TxClock):
            raise TypeError("read_timestamp must be a TxClock instance")

        if (read_timestamp == None):
            read_timestamp = TxClock.now()

        self.cache = cache
        self.read_timestamp = read_timestamp
        self.max_age = max_age
        self.no_cache = no_cache

        # Maintain min read time and max value time seen in transaction
        # (Track consistency of transaction)
        self.min_rt = TxClock.max_value()
        self.max_vt = TxClock.min_value()

        # Maintain Transaction View of all operations
        # view: dict of (table,key) -> (op,value)
        self.view = TxView()

    def _update_min_rt_max_vt(self, cached_time, value_time):
        if (cached_time < self.min_rt):
            self.min_rt = cached_time
        if (value_time > self.max_vt):
            self.max_vt = value_time
        # Raise exception if this update makes the transaction inconsistent
        if (self.max_vt > self.min_rt):
            raise StaleException(read_txclock=self.min_rt, value_txclock=self.max_vt)

    def read(self, table, key, max_age=None, no_cache=False): # raises StaleException
        # First, check the transaction's view
        view = self.view.get_view()
        if (table, key) in view:
            (_, value) = view[(table, key)]
            return value
        # Second, check the cache.  Derive most restrictive read parameters.
        max_age = min(max_age, self.max_age)
        no_cache = no_cache or self.no_cache
        # Attempt to read value from cache
        cache_result = self.cache.read(self.read_timestamp, table, key, max_age, no_cache)
        if (cache_result == None):
            # TODO: value tx clock?
            raise StaleException(read_txclock=read_timestamp, value_txclock=None)
        else:
            # Mark this value as a dependency of the transaction in the view
            self.view.hold(table, key)
            # Extract the value_time, cached_time, and value
            value_time = cache_result.value_time
            cached_time = cache_result.cached_time
            value = cache_result.value
            # Maintain transaction consistency
            self._update_min_rt_max_vt(cached_time, value_time)
            return value

    def write(self, table, key, value):
        # Note value is JSON
        try:
            current_entry = self.read(table, key)
        except:
            current_entry = None
        finally:
            if (current_entry):
                self.view.update(table, key, value)
            else:
                self.view.create(table, key, value)

    def delete(self, table, key):
        self.view.delete(table, key)

    def commit(self): # raises StaleExcpetion
        # Perform the commit provided no values have changed in the DB
        # since the min read time, i.e. oldest value we read is still fresh
        # TODO: Correct to use min_rt as condition time?
        condition_time = self.min_rt
        return self.cache.write(condition_time, self.view.get_view())
