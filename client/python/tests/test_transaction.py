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

import urllib3
from mock import *

from transaction import *
from client_cache import *

class TestTransaction(object):
    def __init__(self):
        self.tests = [
            self.test_read,
            self.test_write,
            self.test_delete,
            self.test_commit
        ]

    def test_read(self):
        print "test_read",

        cache = Mock()
        value_time = TxClock(micro_seconds=5*10**6)
        cached_time = TxClock(micro_seconds=10*10**6)
        value = 42
        response = CacheResult(value_time, cached_time, value)
        cache.read = Mock(return_value=response)

        transaction = Transaction(cache)
        result = transaction.read("table1", "key1")
        assert(result == 42)

        print "PASSED!"

    def test_write(self):
        print "test_write",
        server = "www.google.com"
        cache = ClientCache(server)
        transaction = Transaction(cache)
        table_id = "table1"
        key_id = "key1"
        value = 86
        transaction.write(table_id, key_id, value)
        assert(transaction.view.get_view()[(table_id, key_id)] == ('create', 86))
        # Note we test update functionality in commit
        print "PASSED!"

    def test_delete(self):
        print "test_delete",
        server = "www.google.com"
        cache = ClientCache(server)
        transaction = Transaction(cache)
        table_id = "table1"
        key_id = "key1"
        transaction.delete(table_id, key_id)
        assert(transaction.view.get_view()[(table_id, key_id)] == ('delete', None))       
        print "PASSED!"

    # We mock the request and urlopen methods here to verify that the HTTP 
    # request send to the DB has the correct form
    def test_commit(self):
        print "test_commit",

        cache = Mock()
        cache.write = Mock()

        transaction = Transaction(cache)
        transaction.write("table1", "key1", 79)
        transaction.commit()

        view = cache.write.call_args[0][1]
        assert(view == {('table1', 'key1'): ('create', 79)})
        
        print "PASSED!"

    def test_all(self):
        for test in self.tests:
            test()

test_transaction = TestTransaction()
test_transaction.test_all()

if __name__ == "__main__":
    pass
    