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
        body = "42"
        status = 200
        response = urllib3.response.HTTPResponse(body=body, headers=headers, status=status)

        cache.pool = Mock()
        cache.pool.request = Mock(return_value=response)

        # add entries to the cache 
        status_ok = 200
        response = urllib3.response.HTTPResponse(status=status_ok)
        cache.pool.urlopen = Mock(return_value=response)

        condition_time = TxClock(5)
        ops_dict = {
            ("table1", "key1"): ("create", 42)
        }
        cache.write(condition_time, ops_dict)

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
        assert(transaction.view[(table_id, key_id)] == ('create', 86))
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
        assert(transaction.view[(table_id, key_id)] == ('delete', None))       
        print "PASSED!"

    def test_commit(self):
        print "test_commit",
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
        body = "79"
        status = 200
        response = urllib3.response.HTTPResponse(body=body, headers=headers, status=status)

        cache.pool = Mock()
        cache.pool.request = Mock(return_value=response)

        # Mock the urlopen method
        status = 200
        response = urllib3.response.HTTPResponse(status=status)
        cache.pool.urlopen = Mock(return_value=response)

        transaction = Transaction(cache)
        transaction.write("table1", "key1", 79)
        transaction.commit()

        # Note the op = update here because our mocked DB already contains the value 79 
        # before we write it.
        cache.pool.urlopen.assert_called_with(
            'POST', '/batch-write', 
            body='[{"table": "table1", "value": 79, "key": "key1", "op": "update"}]',
            headers={'If-Modified-Since': 0L, 'Condition-TxClock': 10L})
        print "PASSED!"

    def test_all(self):
        for test in self.tests:
            test()

test_transaction = TestTransaction()
test_transaction.test_all()

if __name__ == "__main__":
    pass
    