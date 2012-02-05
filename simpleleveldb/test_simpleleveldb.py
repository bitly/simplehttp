import os
import simplejson as json
import urllib
import logging
from test_shunt import valgrind_cmd, SubprocessTest
import tornado.httpclient
import time

def http_fetch_json(endpoint, params, status_code=200, status_txt="OK", body=None):
    body = http_fetch(endpoint, params, 200, body)
    data = json.loads(body)
    assert data['status_code'] == status_code
    assert data['status_txt'] == status_txt
    return data['data']

def http_fetch(endpoint, params, response_code=200, body=None):
    http_client = tornado.httpclient.HTTPClient()
    url = 'http://127.0.0.1:8080' + endpoint
    if params:
        url += '?' + urllib.urlencode(params, doseq=1)
    method = "POST" if body else "GET"
    try:
        res = http_client.fetch(url, method=method, body=body)
    except tornado.httpclient.HTTPError, e:
        logging.info(e)
        res = e.response
    assert res.code == response_code
    return res.body

class SimpleLeveldbTest(SubprocessTest):
    process_options = [valgrind_cmd('simpleleveldb', '--db-file=%s/db' % os.path.join(os.path.dirname(__file__), "test_output"), '--enable-logging')]

    def graceful_shutdown(self):
        try:
            http_fetch('/exit', dict())
        except:
            # we never get a reply if this works correctly
            time.sleep(1)
    
    def test_basic(self):
        data = http_fetch_json('/put', dict(key='test', value='12345'))
        data = http_fetch_json('/get', dict(key='test'))
        assert data == '12345'
        data = http_fetch('/get', dict(key='test', format='txt'))
        assert data == 'test,12345\n'
        data = http_fetch_json('/del', dict(key='test'))
        data = http_fetch_json('/get', dict(key='test'), 404, 'NOT_FOUND')
        
        http_fetch_json("/put", dict(), 400, 'MISSING_ARG_KEY')
        http_fetch_json("/put", dict(key='test'), 400, 'MISSING_ARG_VALUE')
        http_fetch_json("/get", dict(), 400, 'MISSING_ARG_KEY')
        
        http_fetch_json('/put', dict(key='test1', value='asdf1'))
        http_fetch_json('/put', dict(key='test2', value='asdf2'))
        
        data = http_fetch('/mget', dict(key=['test1', 'test2', 'test3'], format='txt'))
        print data
        assert data == 'test1,asdf1\ntest2,asdf2\n'
        
        
        # test list stuff
        http_fetch_json('/get', dict(key='list_test'), 404, 'NOT_FOUND')
        data = http_fetch_json('/list_append', dict(key='list_test', value='testvalue1', echo_data='1'))
        assert data == 'testvalue1'
        data = http_fetch_json('/list_append', dict(key='list_test', value='testvalue2'))
        assert data == ''
        data = http_fetch_json('/get', dict(key='list_test'))
        assert data == 'testvalue1,testvalue2'
        data = http_fetch_json('/list_append', dict(key='list_test', value='testvalue3'))
        data = http_fetch_json('/list_remove', dict(key='list_test', value='testvalue2'))
        data = http_fetch_json('/get', dict(key='list_test'))
        assert data == 'testvalue1,testvalue3'
        data = http_fetch_json('/list_remove', dict(key='list_test', value='testvalue1'))
        data = http_fetch_json('/get', dict(key='list_test'))
        assert data == 'testvalue3'
        
        # try a /put with a POST body
        http_fetch_json('/put', dict(key='testpost'), body='asdfpost')
        data = http_fetch_json('/get', dict(key='testpost'))
        assert data == 'asdfpost'
        data = http_fetch_json('/del', dict(key='testpost'))
        
        # test dump to csv
        # we need to check more than 500 entries
        for x in range(505):
            http_fetch_json('/put', dict(key='dump.%d' % x, value='dump.value.%d' % x))
        
        data = http_fetch('/dump_csv', {})
        assert data.startswith("dump.0,dump.value.0\n")
        assert data.endswith("test2,asdf2\n")
        assert data.count("\n") > 505
        
        data = http_fetch('/dump_csv', dict(key="dump."))
        assert data.count("\n") == 505

        