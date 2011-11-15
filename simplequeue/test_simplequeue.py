# -*- coding: UTF-8 -*-
from SimpleQueue import SimpleQueue
RANDOM_STRINGS = [
# the following line is a R-L language
'زشك:یقینحاصلکردمکه'
,'Iñtërnâtiôn�lizætiøn',
'http://flash-mini.com/car2hand/http://flash-mini.com/car2hand/337358/1985_BMW,_316_(4Dr)_38,000_\xe0\xb8\x9a\xe0\xb8\xb2\xe0\xb8\x97_\xe0\xb8\x82\xe0\xb8\xb2\xe0\xb8\xa2_BMW_E30_(\xe0\xb9\x80\xe0\xb8\xad\xe0\xb8\xb2\xe0\xb9\x84\xe0\xb8\xa7\xe0\xb9\x89\xe0\xb8\x82\xe0\xb8\xb1\xe0\xb8\x9a\xe0\xb9\x80\xe0\xb8\xa5\xe0\xb9\x88\xe0\xb8\x99\xe0\xb9\x86\xe0\xb8\x8b\xe0\xb8\xb1\xe0\xb8\x81\xe0\xb8\x84\xe0\xb8\xb1\xe0\xb8\x99)/\xe0\xb9\x80\xe0\xb8\x84\xe0\xb8\xa3\xe0\xb8\xb7\xe0\xb9\x88\xe0\xb8\xad\xe0\xb8\x87Rb-20Turbo_<br>Inter_Rb-26<br>TurboRb-25_<.htm',
'asdf',
'!@#$%^&*()+}{":>?"} +%20',
'string with\r +*',
'string with newline\n +*',
'string with \x01**',
#'string with \x00**', # this will fail
]

PORT=8080

def pytest_generate_tests(metafunc):
    if metafunc.function in [test_put_get]:
        for data in RANDOM_STRINGS:
            metafunc.addcall(funcargs=dict(data=data))

def test_put_get(data):
    c = SimpleQueue(port=PORT, debug=True)
    c.put(data)
    d = c.get()
    assert d == data
    
def test_put_mget():
    c = SimpleQueue(port=PORT, debug=True)
    c.put('test1')
    c.put('test2')
    items = c.mget(num_items=10).splitlines()
    assert len(items) == 2
    assert items[0] == 'test1'
    assert items[1] == 'test2'
    c.put('test3')
    c.put('test4')
    items = c.mget().splitlines()
    assert len(items) == 1
    assert items[0] == 'test3'
    assert c.get() == 'test4'
        
def test_mget_badindex():
    c = SimpleQueue(port=PORT, debug=True)
    msg = c.mget(num_items=-2)
    assert msg == 'number of items must be > 0\n'
    msg2 = c.mget(num_items=0)
    assert msg2 == 'number of items must be > 0\n'
    c.put('test1')
    items = c.mget().splitlines()
    assert len(items) == 1
    assert items[0] == 'test1'

def test_mput_get():
    c = SimpleQueue(port=PORT, debug=True)
    
    # default separator
    c.mput('test1\ntest2\ntest3')
    assert c.get() == 'test1'
    assert c.get() == 'test2'
    assert c.get() == 'test3'
    
    # separator specified
    c.mput(data='test4|test5|test6', separator='|')
    assert c.get() == 'test4'
    assert c.get() == 'test5'
    assert c.get() == 'test6'
    
    # default separator with get request instead of post
    c.mput('test1\ntest2\ntest3', post=False)
    assert c.get() == 'test1'
    assert c.get() == 'test2'
    assert c.get() == 'test3'
    
    # separator specified with get request instead of post
    c.mput(data='test4|test5|test6', separator='|', post=False)
    assert c.get() == 'test4'
    assert c.get() == 'test5'
    assert c.get() == 'test6'
    
    # yes this looks like double, actually checking nothing on queue == ''
    # instead of None
    assert c.get() == ''
    assert c.get() == ''
    
def test_mput_mget():
    c = SimpleQueue(port=PORT, debug=True)
    
    # default separator
    c.mput('test1\ntest2\ntest3')
    items = c.mget(num_items=10).splitlines()
    assert len(items) == 3
    assert items[0] == 'test1'
    assert items[1] == 'test2'
    assert items[2] == 'test3'
    
    # separator specified
    c.mput(data='test4|test5|test6', separator='|')
    items = c.mget(num_items=10).splitlines()
    assert len(items) == 3
    assert items[0] == 'test4'
    assert items[1] == 'test5'
    assert items[2] == 'test6'

    # multichar separator specified, and at end
    c.mput(data='test7SEPtest8SEPtest9SEP', separator='SEP')
    items = c.mget(num_items=10).splitlines()
    assert len(items) == 3
    assert items[0] == 'test7'
    assert items[1] == 'test8'
    assert items[2] == 'test9'

    # default separator with get request instead of post
    c.mput('test1\ntest2\ntest3', post=False)
    items = c.mget(num_items=10).splitlines()
    assert len(items) == 3
    assert items[0] == 'test1'
    assert items[1] == 'test2'
    assert items[2] == 'test3'
    
    # separator specified with get request instead of post
    c.mput(data='test4|test5|test6', separator='|', post=False)
    items = c.mget(num_items=10).splitlines()
    assert len(items) == 3
    assert items[0] == 'test4'
    assert items[1] == 'test5'
    assert items[2] == 'test6'

    # multichar separator specified, and at end with get request instead of post
    c.mput(data='test7SEPtest8SEPtest9SEP', separator='SEP', post=False)
    items = c.mget(num_items=10).splitlines()
    assert len(items) == 3
    assert items[0] == 'test7'
    assert items[1] == 'test8'
    assert items[2] == 'test9'
        
    # yes this looks like double, actually checking nothing on queue == ''
    # instead of None
    assert c.get() == ''
    assert c.get() == ''
        
def test_order():
    # first thing put in, should be first thing out
    c = SimpleQueue(port=PORT, debug=True)
    c.put('test1')
    c.put('test2')
    d1 = c.get()
    d2 = c.get()
    assert d1 == 'test1'
    assert d2 == 'test2'

def test_dump():
    c = SimpleQueue(port=PORT, debug=True)
    c.put('test1')
    c.put('test2')
    d = c.dump()
    c.get()
    c.get()
    assert d == ['test1', 'test2']

if __name__ == "__main__":
    print "tests against port 5150"
    print "usage: py.test test_simplequeue.py"
