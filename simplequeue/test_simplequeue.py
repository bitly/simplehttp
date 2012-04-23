import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../shared_tests"))

import simplejson as json
from test_shunt import valgrind_cmd, SubprocessTest, http_fetch, http_fetch_json

class SimplequeueTest(SubprocessTest):
    binary_name = "simplequeue"
    working_dir = os.path.dirname(__file__)
    test_output_dir = os.path.join(working_dir, "test_output")
    process_options = [valgrind_cmd(test_output_dir, os.path.join(working_dir, binary_name), '--enable-logging')]
   
    def test_basic(self):
        # put/get in order
        http_fetch('/put', dict(data='12345'))
        http_fetch('/put', dict(data='23456'))
        data = json.loads(http_fetch('/stats', dict(format="json")))
        assert data['depth'] == 2
        assert data['bytes'] == 10
        data = http_fetch('/get')
        assert data == "12345"
        data = http_fetch('/get')
        assert data == "23456"
        data = http_fetch('/get')
        assert data == ''

        data = json.loads(http_fetch('/stats', dict(format="json")))
        assert data['depth'] == 0
        assert data['bytes'] == 0
        
        # mget
        http_fetch('/put', dict(data='12345'))
        http_fetch('/put', dict(data='23456'))
        http_fetch('/put', dict(data='34567'))
        http_fetch('/put', dict(data='45678'))
        data = http_fetch('/mget', dict(items=5))
        assert data == '12345\n23456\n34567\n45678\n'
        
        # mput GET req
        http_fetch('/mput', dict(data='test1\ntest2\ntest3'))
        data = http_fetch('/get')
        assert data == 'test1'
        data = http_fetch('/get')
        assert data == 'test2'
        data = http_fetch('/get')
        assert data == 'test3'

        # mput POST req
        http_fetch('/mput', body='test1\ntest2\ntest3')
        data = http_fetch('/get')
        assert data == 'test1'
        data = http_fetch('/get')
        assert data == 'test2'
        data = http_fetch('/get')
        assert data == 'test3'


if __name__ == "__main__":
    print "usage: py.test"
