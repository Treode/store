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

from mock import *
from client_cache import *

import urllib3
from tx_clock import *

class TestClientCache(object):

    def __init__(self, server, port=80):
        self.server = server
        self.port = port
        self.tests = [
            self.ClientCache_Init_Succeeds,
            self.ClientCache_CreatingServerConnection_Succeeds,
            self.ClientCache_ReadingFromCache_Succeeds,
            self.ClientCache_Write]

    # Test that we can create a ClientCache instance successfully
    def ClientCache_Init_Succeeds(self):
        print "test_client_cache_init",

        server = self.server
        max_age = 10

        # Default headers
        cache_default = ClientCache(server)
        assert(cache_default != None)

        # max_age header
        cache_max_age = ClientCache(server, 
            max_age=max_age)
        assert(cache_max_age != None)

        # no_cache header
        cache_no_cache = ClientCache(server, no_cache=True)
        assert(cache_no_cache != None)

        # max_age and no_cache headers
        cache_both = ClientCache(server, 
            max_age=max_age, no_cache=True)
        assert(cache_both != None)

        print "PASSED!"

    # Test that the ClientCache has a valid connection to the server
    def ClientCache_CreatingServerConnection_Succeeds(self):
        print "test_client_cache_connection",

        server = self.server
        max_age = 10

        cache = ClientCache(server, 
            max_age=max_age)
        result = cache.http_facade.pool.request('GET', '/')
        assert(result != None)

        print "PASSED!"

    # Test that calling read generates the expected request with headers
    def ClientCache_ReadingFromCache_Succeeds(self):
        print "test_client_cache_read",

        server = self.server
        cache = ClientCache(server)

        # Mock the request method
        headers = {
            "Date": "Wed, 14 Jan 2015 11:49:13 GMT",
            "Last-Modified": "Fri, 10 May 2013 02:07:43 GMT",
            "Read-TxClock": "10",
            "Value-TxClock": "5",
            "Vary": "Request-TxClock"
        }
        body = """{   "title": "Fruits",
    "types": [
        { "fruit": "apple", "flavor": "sour" },
        { "fruit": "banana", "flavor": "mushy" } ] }"""

        cached_time = TxClock(micro_seconds=long(headers["Read-TxClock"]))
        value_time = TxClock(micro_seconds=long(headers["Value-TxClock"]))
        json_value = json.loads(body)

        cache.http_facade = Mock()
        result = (cached_time, value_time, json_value)
        cache.http_facade.read = Mock(return_value=result)

        self.ClientCache_ReadingWithoutMaxAgeNoCacheParams_Succeeds(cache)
        self.ClientCache_ReadingWithMaxAgeNoCacheParams_Succeeds(cache)

        print "PASSED!"

    def ClientCache_ReadingWithoutMaxAgeNoCacheParams_Succeeds(self, cache):
        read_time = TxClock(10**10*6)
        table = "table1"
        key = "key1"
        cache_result = cache.read(read_time, table, key)
        json_result = cache_result.value

        assert(json_result["title"] == "Fruits")
        assert(json_result["types"] == [
            { "fruit": "apple", "flavor": "sour" },
            { "fruit": "banana", "flavor": "mushy" } ])

    def ClientCache_ReadingWithMaxAgeNoCacheParams_Succeeds(self, cache):
        read_time = TxClock(10*10**6)
        
        # With cache and max age
        table = "table1"
        key = "key2"
        max_age = 8
        no_cache = True
        condition_time = TxClock(10*10**6)
        cache_result = cache.read(read_time, table, key, 
            max_age=max_age, no_cache=no_cache)
        json_result = cache_result.value
        
        assert(json_result["title"] == "Fruits")
        assert(json_result["types"] == [
            { "fruit": "apple", "flavor": "sour" },
            { "fruit": "banana", "flavor": "mushy" } ])

        # Without cache but max age
        table = "table2"
        key = "key42"
        cache_result = cache.read(read_time, table, key, 
            max_age=max_age)
        json_result = cache_result.value

        assert(json_result["title"] == "Fruits")
        assert(json_result["types"] == [
            { "fruit": "apple", "flavor": "sour" },
            { "fruit": "banana", "flavor": "mushy" } ])

    def ClientCache_Write(self):
        print "test_client_cache_write ", 

        server = self.server
        max_age = 10
        cache = ClientCache(server, 
            max_age=max_age)

        self.ClientCache_Write_Succeeds(cache)
        self.ClientCache_Write_Fails(cache)

        print "PASSED!"
    
    def ClientCache_Write_Succeeds(self, cache):
        # Mock the urlopen method
        status = 200
        response = urllib3.response.HTTPResponse(status=status)
        cache.http_facade = Mock()
        cache.http_facade.write = Mock(return_value=response)

        condition_time = TxClock(5*10**6)
        ops_dict = {
            ("table1", "key1"): ("create", 42),
            ("table2", "key2"): ("hold", None),
            ("table3", "key3"): ("update", 54), 
            ("table4", "key4"): ("delete", 79)
        }

        try:
            cache.write(condition_time, ops_dict)
        except:
            # Expected, since we are not running a real DB in this test
            pass

    def ClientCache_Write_Fails(self, cache):
        # Mock the request method
        status = 412
        response = urllib3.response.HTTPResponse(status=status, 
            headers={"Value-TxClock": "992"})
        cache.http_facade = Mock()
        cache.http_facade.write = Mock(return_value=response)

        condition_time = TxClock(5*10**6)
        ops_dict = {
            ("table1", "key1"): ("update", 112)
        }

        try:
            cache.write(condition_time, ops_dict)
            # The write should fail
            assert(False)
        except StaleException as exn:
            # You failed.  Good job!
            (read_time, value_time) = exn.toTuple()
            assert(str(value_time) == "992")

    def test_all(self):
        for test in self.tests:
            test()

test_instance = TestClientCache("www.bbc.com")
test_instance.test_all()

if __name__ == "__main__":
    pass
    