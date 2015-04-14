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
        rt = 2 
        vt = 1 
        cache_map = CacheMap(max_size) 
        cache_map.put(rt, vt, 0, 0, "apple")
        assert(cache_map.get(0, 0, read_time=rt).value == "apple")

        print "PASSED!"
        
    def test_get(self):
        print  "test_get",

        max_size = 2
        cache_map = CacheMap(max_size)
        # Add entry, verify we can read it
        rt = 6
        vt = 5  
        cache_map.put(rt, vt, 0, 0, "apple")
        assert(cache_map.get(0, 0, read_time=rt).value == "apple")

        # Read multiple entries
        rt = 4 
        vt = 3 
        cache_map.put(rt, vt, 0, 1, "banana")
        assert(cache_map.get(0, 1, read_time=rt).value == "banana")        

        # Test eviction 
        rt = 2
        vt = 1
        cache_map.put(rt, vt, 0, 2, "cherry")
        assert(cache_map.get(0, 2, read_time=rt).value == "cherry") 
        assert(cache_map.get(0, 1, read_time=4).value == "banana")  
        assert(cache_map.get(0, 0, read_time=6) == None)    

        # Test entry updates
        rt = 6
        vt = 5
        cache_map.put(rt, vt, 0, 0, "date")
        assert(cache_map.get(0, 0, read_time=rt).value == "date")
        assert(cache_map.get(0, 1, read_time=342523453425).value == "banana") 
        assert(cache_map.get(0, 2, read_time=234234) == None)

        rt = 8
        vt = 7
        cache_map.put(rt, vt, 0, 0, "eggfruit")
        assert(cache_map.get(0, 0, read_time=rt).value == "eggfruit")   
        assert(cache_map.get(0, 1, read_time=342523453425).value == "banana") 
        assert(cache_map.get(0, 2, read_time=234234) == None) 
        
        print "PASSED!"

    def test_all(self):
        for test in self.tests:
            test()

test_cache_map = TestCacheMap()
test_cache_map.test_all()