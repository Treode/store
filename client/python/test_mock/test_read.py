from mock import Mock
from mock import patch
import test_stubs
import urllib3


#####################################################
# Basic Usage
#####################################################

class ClientCache(object):
    pass

real = ClientCache()
real.method = Mock(return_value=3)
real.method(3, 4, 5, key='value')
real.method.assert_called_with(3, 4, 5, key='value')

mock = Mock(side_effect=KeyError('foo'))
values = {'a': 1, 'b': 2, 'c': 3}
def side_effect(arg):
    return values[arg]
mock.side_effect = side_effect
print mock('a'), mock('b'), mock('c')
mock.side_effect = [5, 4, 3, 2, 1]
print mock(), mock(), mock(), mock('a'), mock(42)



######################################################
# Patches
#####################################################



def test_urllib3():
    http = urllib3.PoolManager()
    http.request = Mock()
    r = http.request('GET', 'www.google.com')
test_urllib3()

@patch('test_stubs.ClassName2')
@patch('test_stubs.ClassName1')
def test(MockClass1, MockClass2):
    c1 = test_stubs.ClassName1('dogfood')
    test_stubs.ClassName2('woah, dude')    
    assert MockClass1.called
    assert MockClass2.called
    assert MockClass1.call_count == 1
    assert MockClass2.call_count == 1
    print MockClass1.call_args_list
    print MockClass2.call_args_list
test()

@patch('urllib3.connection_from_url')
def test_foo(urllib3_mock_connection_from_url):
    #print dir(urllib3_mock_connection_from_url)
    print urllib3.connection_from_url('google.com')
test_foo()




#####################################################
# Foo object extended with mock
#####################################################



 
# The mock object
class Foo(object):
    # instance properties
    _fooValue = 123
     
    def callFoo(self):
        print "Foo:callFoo_"
     
    def doFoo(self, argValue):
        print "Foo:doFoo:input = ", argValue
 
# creating the mock object
fooObj = Foo()
print fooObj
# returns: <__main__.Foo object at 0x68550>
 
mockFoo = Mock(return_value = fooObj)
print mockFoo
# returns: <Mock id='2788144'>
 
# creating an "instance"
mockObj = mockFoo()
print mockObj
print dir(mockObj)
# returns: <__main__.Foo object at 0x68550>
 
# working with the mocked instance
print mockObj._fooValue
# returns: 123
mockObj.callFoo()
# returns: Foo:callFoo_
mockObj.doFoo("narf")
# returns: Foo:doFoo:input =  narf






# The mock object
class Foo(object):
    # instance properties
    _fooValue = 123
     
    def callFoo(self):
        pass
     
    def doFoo(self, argValue):
        pass
 
# create the mock object
mockFoo = Mock(spec = Foo)
print mockFoo
# returns <Mock spec='Foo' id='507120'>
 
mockFoo.doFoo("narf")
mockFoo.doFoo.assert_called_with("narf")
# assertion passes
 
mockFoo.doFoo("zort")
#mockFoo.doFoo.assert_called_with("narf")
# AssertionError: Expected call: doFoo('narf')
# Actual call: doFoo('zort')








###############################################
# Just patching
###############################################

# TODO: This doesn't work.  How do I override methods in a given object
# with a patch?

class PatchClass(object):
    def isCool(self):
        return "patch successful"

class UnpatchedClass(object):
    def isCool(self):
        return "patch failed"

@patch('test_stubs.UnpatchedClass')
def testPatch(PatchClass):
    obj = test_stubs.UnpatchedClass()
    print obj.isCool()
testPatch()

print "PASSED!"