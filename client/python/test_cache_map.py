from cache_map import *

import random

TEST_CNT = 10000
MAX_VAL = 1000000

class TestCacheMap(object):

    def __init__(self):
        self.tests = [
            self.test_put,
            self.test_get
        ]

    def test_put(self):
        print "test_put",

        # Check size maintained
        max_size = 100
        cache_map = CacheMap(max_size) 
        for i in xrange(TEST_CNT):
            cache_map.put(
                random.randint(0,MAX_VAL),
                random.randint(0,MAX_VAL),
                random.randint(0,MAX_VAL),
                random.randint(0,MAX_VAL),
                random.randint(0,MAX_VAL))
        value_list =  [cache_value 
            for value_list in cache_map.cache_map.values() 
            for cache_value in value_list ]
        # Note this test is randomized, so it may fail periodically...
        # in case of many overlapping values or if TEST_CNT too small
        assert(len(value_list) == max_size)

        # Check entries maintained
        rt = 213423423142134 
        vt = 202132312413123 
        cache_map = CacheMap(max_size) 
        cache_map.put(rt, vt, 0, 0, 42)
        assert(cache_map.get(0, 0, read_time=213423423142132).value == 42)

        print "PASSED!"
        
    def test_get(self):
        print  "test_get",

        max_size = 2
        cache_map = CacheMap(max_size)
        # Add entry, verify we can read it
        rt = 213423423142134
        vt = 202132312413123  
        cache_map.put(rt, vt, 0, 0, 42)
        assert(cache_map.get(0, 0, read_time=213423423142134).value == 42)

        # Read multiple entries
        rt = 342523453425 
        vt = 324523452345 
        cache_map.put(rt, vt, 0, 1, 43)
        assert(cache_map.get(0, 1, read_time=342523453425).value == 43)        

        # Test eviction
        rt = 234234 
        vt = 231324 
        cache_map.put(rt, vt, 0, 2, 44)
        assert(cache_map.get(0, 2, read_time=234234).value == 44) 
        assert(cache_map.get(0, 1, read_time=342523453425).value == 43)  
        assert(cache_map.get(0, 0, read_time=213423423142134) == None)    

        # Test entry updates
        rt = 213423423142134
        vt = 202132312413123
        cache_map.put(rt, vt, 0, 0, 45)
        assert(cache_map.get(0, 0, read_time=213423423142134).value == 45)
        assert(cache_map.get(0, 1, read_time=342523453425).value == 43) 
        assert(cache_map.get(0, 2, read_time=234234) == None)

        rt = 232342342342342
        vt = 213423423142135
        cache_map.put(rt, vt, 0, 0, 46)
        assert(cache_map.get(0, 0, read_time=rt).value == 46)   
        assert(cache_map.get(0, 1, read_time=342523453425).value == 43) 
        assert(cache_map.get(0, 2, read_time=234234) == None) 
        
        print "PASSED!"

    def test_all(self):
        for test in self.tests:
            test()

test_cache_map = TestCacheMap()
test_cache_map.test_all()