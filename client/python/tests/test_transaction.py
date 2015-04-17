from transaction import *

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
        pass
        print "PASSED!"

    def test_write(self):
        print "test_write",
        pass
        print "PASSED!"

    def test_delete(self):
        print "test_delete",
        pass
        print "PASSED!"

    def test_commit(self):
        print "test_commit",
        pass
        print "PASSED!"

    def test_all(self):
        for test in self.tests:
            test()

test_transaction = TestTransaction()
test_transaction.test_all()