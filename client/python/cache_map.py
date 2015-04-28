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
from llist import *
from tx_clock import *

class _InternalCacheObject(object):

    def __eq__(self, other):
        if (isinstance(other, _InternalCacheObject)):
            return self.__dict__ == other.__dict__
        return False

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

    class _CacheEntry(_InternalCacheObject):

        def __init__(self, table_name, key, value,
            cached_time, value_time):
            self.table_name = table_name
            self.key = key
            self.value = value
            self.cached_time = cached_time
            self.value_time = value_time

    def __init__(self, max_size):
        self.max_size = max_size

        # Store values as a dictionary of lists:
        # { (table, key): [ dllistnode(_CacheEntry), ... ]}
        self.entry_table = {}

        # Track the order of entries added to / accessed in
        # cache to maintain LRU policy [ dllistnode(_CacheEntry) ... ]
        self.lru_list = dllist()

    def _lru_add(self, entry):
        node = self.lru_list.appendleft(entry)
        self._lru_evict()
        return node

    def _lru_refresh(self, node):
        lru_list = self.lru_list
        entry = lru_list.remove(node)
        new_node = lru_list.appendleft(entry)
        self.lru_list = lru_list
        return new_node

    def _lru_evict(self):
        entry_table = self.entry_table
        lru_list = self.lru_list
        max_size = self.max_size
        while (lru_list.size > max_size):
            # Remove evicted entries from the entry_table
            node = lru_list.last
            # Remove node from LRU list
            entry = lru_list.popright()
            # Remove node from hash table
            table_name = entry.table_name
            key = entry.key
            if ((table_name, key) in entry_table):
                nodes = entry_table[(table_name, key)]
                nodes.remove(node)
        # Reset the LRU list
        self.lru_list = lru_list

    """
    add entry to cache cache_map
    """
    def put(self, read_time, value_time, table_name, key, value):
        if (type(read_time) != TxClock or type(value_time) != TxClock):
            raise TypeError("input times must be TxClock instances")

        # Add the entry to the cache
        entry = self._CacheEntry(table_name, key, value,
            read_time, value_time)

        # Entry exists in hash table
        if ((table_name, key) in self.entry_table):
            nodes = self.entry_table[(table_name, key)]
            refreshed = False
            for i in xrange(len(nodes)):
                node = nodes[i]
                other_entry = node.value
                other_vt = other_entry.value_time
                other_ct = other_entry.cached_time
                if (value_time == other_vt):
                    # Update the cached time of an existing entry
                    other_entry.cached_time = max(other_ct, read_time)
                    # Refresh updated entry in LRU
                    new_node = self._lru_refresh(node)
                    # Update the dllist node in the hash table
                    nodes[i] = new_node
                    refreshed = True
                    # No two list entries have the same value time
                    break
            # Add new value to existing hash table entry
            if (not refreshed):
                node = self._lru_add(entry)
                nodes.append(node)
        # Add new hash table entry
        else:
            node = self._lru_add(entry)
            self.entry_table[(table_name, key)] = [node]

    """
    lookup entry in cache cache_map
    """
    def get(self, table_name, key, read_time=None):
        if (read_time == None):
            read_time = TxClock.now()
        elif (type(read_time) != TxClock):
            raise TypeError("input times must be TxClock instances")

        # Lookup by table_name and key in hash table
        entry_table = self.entry_table
        most_recent_time = None
        most_recent_node = None
        most_recent_hash_table_entry = None
        most_recent_hash_table_entry_index = None
        if (table_name, key) in entry_table:
            nodes = entry_table[(table_name, key)]
            for i in xrange(len(nodes)):
                node = nodes[i]
                entry = node.value
                value_time = entry.value_time
                if (value_time <= read_time and
                    (most_recent_time == None or
                     value_time > most_recent_time)):
                    most_recent_time = entry.value_time
                    most_recent_node = node
                    most_recent_hash_table_entry = nodes
                    most_recent_hash_table_entry_index = i
        # Update LRU list with node just accessed
        if (most_recent_node != None):
            new_node = self._lru_refresh(most_recent_node)
            # Update the hash table node entry
            nodes = most_recent_hash_table_entry
            i = most_recent_hash_table_entry_index
            # Replace old dllistnode with new one after refresh
            nodes[i] = new_node
            # Give back a Cache result
            most_recent_entry = most_recent_node.value
            return CacheResult(
                most_recent_entry.value_time,
                most_recent_entry.cached_time,
                most_recent_entry.value)
        # Not in cache map
        else:
            return None

    def __str__(self):
        return str(self.entry_table)
