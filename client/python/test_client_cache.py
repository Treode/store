from mock import *
from client_cache import *

import urllib3

class TestClientCache(object):

    def __init__(self):
        self.tests = [
            self.test_client_cache_init,
            self.test_client_cache_connection,
            self.test_client_cache_read,
            self.test_client_cache_write]

    # Test that we can create a ClientCache instance successfully
    def test_client_cache_init(self):
        print "test_client_cache_init",

        server = "www.google.com"
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
    def test_client_cache_connection(self):
        print "test_client_cache_connection",

        server = "www.google.com"
        max_age = 10

        cache = ClientCache(server, 
            max_age=max_age)
        result = cache.pool.request('GET', '/')
        assert(result != None)

        print "PASSED!"

    # Test that calling read generates the expected request with headers
    def test_client_cache_read(self):
        print "test_client_cache_read",

        server = "www.google.com"
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
        status = 200
        response = urllib3.response.HTTPResponse(body=body, headers=headers, status=status)

        cache.pool = Mock()
        cache.pool.request = Mock(return_value=response)

        self.test_client_cache_read_with_no_headers(cache)
        self.test_client_cache_read_with_headers(cache)

        print "PASSED!"

    def test_client_cache_read_with_no_headers(self, cache):
        read_time = 10
        table = "table1"
        key = "key1"
        cache_result = cache.read(read_time, table, key)
        json_result = cache_result.value
    
        cache.pool.request.assert_called_with("GET", "/table1/key1", fields={'Read-TxClock': 10})

        assert(json_result["title"] == "Fruits")
        assert(json_result["types"] == [
            { "fruit": "apple", "flavor": "sour" },
            { "fruit": "banana", "flavor": "mushy" } ])

    def test_client_cache_read_with_headers(self, cache):
        read_time = 10
        
        # With cache and max age
        table = "table1"
        key = "key2"
        max_age = 8
        no_cache = True
        condition_time = 10000000
        cache_result = cache.read(read_time, table, key, 
            max_age=max_age, no_cache=no_cache, condition_time=condition_time)
        json_result = cache_result.value

        cache.pool.request.assert_called_with("GET", "/table1/key2", 
            fields={'Cache-Control': 'max-age=8,no-cache', 
                    'Read-TxClock': 10, 
                    'Condition-TxClock': 10000000, 
                    'If-Modified-Since': 10})
        
        assert(json_result["title"] == "Fruits")
        assert(json_result["types"] == [
            { "fruit": "apple", "flavor": "sour" },
            { "fruit": "banana", "flavor": "mushy" } ])

        # Without cache but max age
        table = "table2"
        key = "key42"
        cache_result = cache.read(read_time, table, key, 
            max_age=max_age, condition_time=condition_time)
        json_result = cache_result.value

        cache.pool.request.assert_called_with("GET", "/table2/key42",
            fields={'Cache-Control': 'max-age=8', 
                    'Read-TxClock': 10, 
                    'Condition-TxClock': 10000000, 
                    'If-Modified-Since': 10})

        assert(json_result["title"] == "Fruits")
        assert(json_result["types"] == [
            { "fruit": "apple", "flavor": "sour" },
            { "fruit": "banana", "flavor": "mushy" } ])

    def test_client_cache_write(self):
        print "test_client_cache_write ", 

        server = "www.google.com"
        max_age = 10
        cache = ClientCache(server, 
            max_age=max_age)

        # Mock the request method
        status = 200
        response = urllib3.response.HTTPResponse(status=status)
        cache.pool = Mock()
        cache.pool.urlopen = Mock(return_value=response)

        condition_time = 5
        ops_dict = {
            ("table1", "key1"): ("add", 42),
            ("table2", "key2"): ("hold", None),
            ("table3", "key3"): ("delete", 79),
            ("table4", "key4"): ("update", 54) 
        }

        cache.write(condition_time, ops_dict)

        cache.pool.urlopen.assert_called_with(
            'POST', '/batch-write', 
            body='[{"table": "table2", "value": null, "key": "key2", "op": "hold"}, {"table": "table1", "value": 42, "key": "key1", "op": "add"}, {"table": "table4", "value": 54, "key": "key4", "op": "update"}, {"table": "table3", "value": 79, "key": "key3", "op": "delete"}]', 
            headers={'If-Modified-Since': 0, 'Condition-TxClock': 5})
        print "PASSED!"
        
    def test_all(self):
        for test in self.tests:
            test()

test_instance = TestClientCache()
test_instance.test_all()
