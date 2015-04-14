import string
from urllib3 import HTTPConnectionPool
import json

from cache_map import *
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

        # Initialize connection to DB server
        pool = HTTPConnectionPool(self.server, port=self.port)
        self.pool = pool

        # Initialize the cache value store.
        self.cache_map = CacheMap(ClientCache.MAX_CACHE_SIZE)

    def _micro_to_sec(self, time):
        us_per_sec = 10**6
        # We don't care about integer division since the second
        # resolution is imprecise anyway.
        return time / us_per_sec

    def _get_most_restrictive_headers(self, max_age, no_cache):
        # Most restrictive constraint for max_age
        if (max_age != None and self.max_age == None):
            max_age = max_age
        elif (max_age == None and self.max_age != None):
            max_age = self.max_age
        elif (max_age != None and self.max_age != None):
            max_age = min(max_age, self.max_age)
        else:
            max_age = None

        # Most restrictive constraint for no_cache
        no_cache = no_cache or self.no_cache
        return (max_age, no_cache)

    def _get_max_age_directive(self, max_age):
        return ("max-age=%d" % max_age) if max_age else ""

    def _get_no_cache_directive(self, no_cache):
        return "no-cache" if no_cache else ""

    def _get_cache_control_string(self, max_age, no_cache):
        max_age_directive = self._get_max_age_directive(max_age)
        no_cache_directive = self._get_no_cache_directive(no_cache)
        cache_control_string = (max_age_directive + 
            ("," if (max_age_directive and no_cache_directive) else "") + 
            no_cache_directive)
        return cache_control_string

    def _construct_header_dict(self, read_time, max_age, no_cache, condition_time):
        # Generate the header dictionary from the most restrictive headers
        header_dict = {}
        # Custom Read-TxClock header: Value_time of entry must be earlier
        if (read_time != None):
            header_dict["Read-TxClock"] = read_time
        if (max_age != None or no_cache == True):
            header_dict["Cache-Control"] = self._get_cache_control_string(max_age, no_cache)
        # condition_time: only send back update if entry modified since condition_time
        if (condition_time != None):
            header_dict["Condition-TxClock"] = condition_time
        # Less precise condition_time header for intermediate proxy caches.
        if (condition_time != None):
            header_dict["If-Modified-Since"] = self._micro_to_sec(condition_time)
        return header_dict

    def _construct_request_path(self, table="", key="", batch_write=False):
        if (batch_write):
            return "/batch-write"
        else:
            return "/%s/%s" % (table, key)

    """
    lookup entry in cache
    """
    def read(self, read_time, table, key, max_age=None, no_cache=False, condition_time=None):
        # If the value is in cache, just return it.
        cache_result = self.cache_map.get(table, key, read_time)
        if (cache_result) != None:
            return cache_result

        # Otherwise, create the read request
        request_path = self._construct_request_path(table=table, key=key)
        header_dict = self._construct_header_dict(
            read_time, max_age, no_cache, condition_time)

        # Send the request to the DB server and received response
        response = self.pool.request('GET', request_path, 
            fields=header_dict)

        # Evaluate the response
        if (response.status != 200):
            # If the read failed, don't give the user a value.
            # We know the DB had no value at this instant.
            # TODO Correct?
            value_time = read_time
            json_value = None
        else:
            # If the read succeeded, parse the response
            headers = response.getheaders()
            read_time = headers["Read-TxClock"]
            value_time = headers["Value-TxClock"]
            body = response.data
            json_value = json.loads(body)

        # Store the new result in cache regardless of success
        self.cache_map.put(read_time, value_time, table, key, json_value)

        # Return the Cache Result to the user
        return self.cache_map.get(table, key, read_time)

    def _construct_json_list(self, ops_map):
        json_list = []
        for key in ops_map:
            (table_id, key_id) = key
            (op, value) = ops_map[key]
            entry = { "op": op, "table": table_id, "key": key_id, "value": value }
            json_list += [entry]
        return json.dumps(json_list)

    def _update_cache_from_ops_map(self, ops_map):
        for key in ops_map:
            (table_id, key_id) = key
            (op, value) = ops_map[key]
            if (op == "add" or op == "update"):
                read_time = TxClock(time.time())
                value_time = TxClock(time.time())
                self.cache_map.put(
                    read_time, value_time, table_id, key_id, value)
            elif (op == "hold"):
                # Value already in cache 
                read_time = TxClock(time.time())
                cache_result = self.cache_map.get(table_id, key_id)
                if (cache_result != None):
                    # Update the cache entry with a new cache time
                    cache_result.cached_time = read_time
            elif (op == "delete"):
                # Value will eventually be evicted from cache
                pass

    """ 
    add entry to cache
    """
    def write(self, condition_time, ops_map):
        # Create the write request
        request_path = self._construct_request_path(batch_write=True)
        header_dict = self._construct_header_dict(
            None, None, None, condition_time)
        body = self._construct_json_list(ops_map)

        # Send the request to the DB server and received response
        response = self.pool.urlopen('POST', request_path, 
            headers=header_dict, body=body)

        # Successful batch write
        if (response.status == 200):
            # Update the cache
            self._update_cache_from_ops_map(ops_map)
        else:
            value_txclock = response.headers["Value-TxClock"]
            raise StaleException(value_txclock)


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
