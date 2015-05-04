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

import string
from urllib3 import HTTPConnectionPool
import json

from cache_map import *
from http_facade import *
from tx_clock import *
from stale_exception import *

"""
Treode Python client cache implementation
"""
class ClientCache(object):

    MAX_CACHE_SIZE = 1000

    def __init__(self, server, port=80, max_age=None, no_cache=False):
        self.server = server
        self.port = port
        self.max_age = max_age
        self.no_cache = no_cache

        # Initialize connection to the DB server
        http_facade = HTTPFacade(server, port)
        self.http_facade = http_facade

        # Initialize the cache value store.
        self.cache_map = CacheMap(ClientCache.MAX_CACHE_SIZE)

    """
    lookup entry in cache or request from database
    """
    def read(self, read_time, table, key, max_age=None, no_cache=False):
        if (read_time != None and type(read_time) != TxClock):
            raise TypeError("read_time must be TxClock")

        # If the value is in cache, just return it.
        cache_result = self.cache_map.get(table, key, read_time)
        if (cache_result) != None:
            return cache_result

        # Otherwise, create the read request
        result = self.http_facade.read(
            read_time, table, key, max_age, no_cache)
        # Read failed, nothing to store
        if (result == None):
            return
        else:
            (cached_time, value_time, json_value) = result
            # Store the new result in cache regardless of success
            self.cache_map.put(cached_time, value_time, table, key, json_value)
            # Return the Cache Result to the user
            return self.cache_map.get(table, key, cached_time)

    def _update_cache_from_tx_view(self, tx_view):
        for key in tx_view:
            (table_id, key_id) = key
            (op, value) = tx_view[key]
            if (op == "create" or op == "hold" or op == "update"):
                # TODO correct?
                read_time = TxClock.now()
                # Reading from the DB updates the cache values as needed
                cache_result = self.read(
                    read_time, table_id, key_id, no_cache=True)
                # Invariant: The values we just wrote should be in the DB
                if (cache_result == None):
                    raise ValueError("DB does not contain entry %s, which was supposedly just added" % 
                        str(value))
            elif (op == "delete"):
                # Value will eventually be evicted from cache
                pass

    """ 
    add entry to cache
    """
    def write(self, condition_time, tx_view):
        # Verify input type
        if (condition_time != None and type(condition_time) != TxClock):
            raise TypeError("condition_time must be TxClock")

        # Create and send write request to DB and receive response
        response = self.http_facade.write(condition_time, tx_view)

        value_txclock_key = "Value-TxClock"
        # Successful batch write; update the cache
        if (response.status == 200):
            self._update_cache_from_tx_view(tx_view)
        # Otherwise, status == 412, precondition failed
        elif (value_txclock_key in response.headers):
            value_txclock = response.headers[value_txclock_key]
            # TODO: read_txclock?
            raise StaleException(read_txclock=None, value_txclock=value_txclock)
        # The response does not contain the value time resulting in failure
        else:
            raise StaleException(read_txclock=None, value_txclock=None)

    def __repr__(self):
        builder = []
        builder += ["ClientCache("]

        # Required
        builder += [str(self.server) + ", "]

        # Optional
        builder += ["port=" + str(self.port) + ", "]
        builder += ["max_age=" + str(self.max_age) + ", "] if self.max_age != None else ""
        builder += ["no_cache=" + str(self.no_cache)] if self.no_cache != None else ""

        builder += ")"

        return "".join(builder)
