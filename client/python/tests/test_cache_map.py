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

from cache_map import *

import random

TEST_COUNT = 10000
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
        for i in xrange(TEST_COUNT):
            cache_map.put(
                TxClock(micro_seconds=random.randint(0,MAX_VAL)),
                TxClock(micro_seconds=random.randint(0,MAX_VAL)),
                random.randint(0,MAX_VAL),
                random.randint(0,MAX_VAL),
                random.randint(0,MAX_VAL))
        value_list =  [cache_value 
            for value_list in cache_map.entry_table.values() 
            for cache_value in value_list ]
        # Note this test is randomized, so it may fail periodically...
        # in case of many overlapping values or if TEST_COUNT too small
        assert(len(value_list) == max_size)

        # Check entries maintained
        rt = TxClock(2*10**6) 
        vt = TxClock(1*10**6) 
        cache_map = CacheMap(max_size) 
        cache_map.put(rt, vt, 0, 0, "apple")
        assert(cache_map.get(0, 0, read_time=rt).value == "apple")

        print "PASSED!"
        
    def test_get(self):
        print  "test_get",

        def clock(sec):
            return TxClock(micro_seconds=sec * 10**6)

        max_size = 2
        cache_map = CacheMap(max_size)
        # Add entry, verify we can read it
        rt = clock(6)
        vt = clock(5)  
        cache_map.put(rt, vt, 0, 0, "apple")
        assert(cache_map.get(0, 0, read_time=rt).value == "apple")

        # Read multiple entries
        rt = clock(4) 
        vt = clock(3) 
        cache_map.put(rt, vt, 0, 1, "banana")
        assert(cache_map.get(0, 1, read_time=rt).value == "banana")        

        # Test eviction 
        rt = clock(2)
        vt = clock(1)
        cache_map.put(rt, vt, 0, 2, "cherry")
        assert(cache_map.get(0, 2, read_time=rt).value == "cherry") 
        assert(cache_map.get(0, 1, read_time=clock(4)).value == "banana")  
        assert(cache_map.get(0, 0, read_time=clock(6)) == None)    

        # Test entry updates
        rt = clock(6)
        vt = clock(5)
        cache_map.put(rt, vt, 0, 0, "date")
        assert(cache_map.get(0, 0, read_time=rt).value == "date")
        assert(cache_map.get(0, 1, read_time=clock(4)).value == "banana") 
        assert(cache_map.get(0, 2, read_time=clock(2)) == None)

        rt = clock(8)
        vt = clock(7)
        cache_map.put(rt, vt, 0, 0, "eggfruit")
        assert(cache_map.get(0, 0, read_time=rt).value == "eggfruit")   
        assert(cache_map.get(0, 1, read_time=clock(4)).value == "banana") 
        assert(cache_map.get(0, 2, read_time=clock(2)) == None) 
        
        print "PASSED!"

    def test_all(self):
        for test in self.tests:
            test()

test_cache_map = TestCacheMap()
test_cache_map.test_all()

if __name__ == "__main__":
    pass
