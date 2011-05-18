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

PORT=5150

def pytest_generate_tests(metafunc):
    if metafunc.function in [test_put_get]:
        for data in RANDOM_STRINGS:
            metafunc.addcall(funcargs=dict(data=data))

def test_put_get(data):
    c = SimpleQueue(port=PORT, debug=True)
    c.put(data)
    d = c.get()
    assert d == data

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
    print "tests against port 8082"
    print "usage: py.test test_simplequeue.py"
