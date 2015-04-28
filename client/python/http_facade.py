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

class HTTPFacade(object):

    def __init__(self, server, port):
        self.server = server
        self.port = port

        # Initialize connection to DB server
        pool = HTTPConnectionPool(self.server, port=self.port)
        self.pool = pool

    """Returns (cached_time, value_time, value)"""
    def read(self, read_time, table, key, max_age, no_cache):
        request_path = self._construct_request_path(table=table, key=key)
        header_dict = self._construct_header_dict(
            read_time, max_age, no_cache, None)

        # Send the request to the DB server and received response
        response = self.pool.request('GET', request_path,
            fields=header_dict)

        # Evaluate the response
        if (response.status != 200):
            # If the read failed, don't give the user a value.
            # We know the DB had no value at this instant.
            # TODO Correct?
            return
        # If the read succeeded, parse the response and give the info to user
        else:
            headers = response.getheaders()
            cached_time = TxClock(micro_seconds=long(headers["Read-TxClock"]))
            value_time = TxClock(micro_seconds=long(headers["Value-TxClock"]))
            body = response.data
            json_value = json.loads(body)
            return (cached_time, value_time, json_value)

    """Returns HTTP response from attempt at optimistic, batched DB write"""
    def write(self, condition_time, tx_view):
        # Create the write request
        request_path = self._construct_request_path(batch_write=True)
        header_dict = self._construct_header_dict(
            None, None, None, condition_time)
        body = self._construct_json_list(tx_view)

        # Send the request to the DB server and receive response
        response = self.pool.urlopen('POST', request_path,
            headers=header_dict, body=body)
        return response

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
            header_dict["Read-TxClock"] = read_time.time
        if (max_age != None or no_cache == True):
            header_dict["Cache-Control"] = self._get_cache_control_string(max_age, no_cache)
        # condition_time: only send back update if entry modified since condition_time
        if (condition_time != None):
            header_dict["Condition-TxClock"] = condition_time.time
        # Less precise condition_time header for intermediate proxy caches.
        if (condition_time != None):
            header_dict["If-Modified-Since"] = condition_time.to_seconds()
        return header_dict

    def _construct_request_path(self, table="", key="", batch_write=False):
        if (batch_write):
            return "/batch-write"
        else:
            return "/%s/%s" % (table, key)

    def _construct_json_list(self, ops_map):
        json_list = []
        for key in ops_map:
            (table_id, key_id) = key
            (op, value) = ops_map[key]
            entry = { "op": op, "table": table_id,
                "key": key_id, "value": value }
            json_list += [entry]
        return json.dumps(json_list)
