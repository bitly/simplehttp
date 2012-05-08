import os
import re
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../shared_tests"))

from test_shunt import valgrind_cmd, SubprocessTest, http_fetch, http_fetch_json


class SimpleLeveldbTest(SubprocessTest):
    binary_name = "simpleleveldb"
    working_dir = os.path.dirname(__file__)
    test_output_dir = os.path.join(working_dir, "test_output")
    process_options = [valgrind_cmd(test_output_dir, os.path.join(working_dir, binary_name), '--db-file=%s/db' % test_output_dir, '--enable-logging')]
    
    # /put, /get, /del
    def test_basic(self):
        data = http_fetch_json('/put', dict(key='test_basic', value='12345'))
        data = http_fetch_json('/get', dict(key='test_basic'))
        assert data == '12345'
        data = http_fetch_json('/get', dict(key='test_basic', format='json'))
        assert data == '12345'
        data = http_fetch('/get', dict(key='test_basic', format='txt'))
        assert data == 'test_basic,12345\n'
        data = http_fetch('/get', dict(key='test_basic', format='txt', separator='/'))
        assert data == 'test_basic/12345\n'
        data = http_fetch('/put', dict(key='test_basic', value='22', format='txt'))
        data = http_fetch_json('/get', dict(key='test_basic'))
        assert data == '22'
        data = http_fetch_json('/put', dict(key='test_basic', value='33', format='json'))
        data = http_fetch_json('/get', dict(key='test_basic'))
        assert data == '33'
        data = http_fetch_json('/put', dict(key='test_basic'), body="44")
        data = http_fetch_json('/get', dict(key='test_basic'))
        assert data == '44'
        data = http_fetch_json('/del', dict(key='test_basic'))
        data = http_fetch_json('/get', dict(key='test_basic'), 404, 'NOT_FOUND')
        data = http_fetch('/get', dict(key='test_basic', format='txt'), 404)
        data = http_fetch_json('/get', dict(key='test_basic', format='json'), 404, 'NOT_FOUND')
        data = http_fetch_json('/put', dict(key='test_basic', value='a'))
        data = http_fetch('/del', dict(key='test_basic', format='txt'))
        data = http_fetch_json('/get', dict(key='test_basic'), 404, 'NOT_FOUND')
    
    def test_missing(self):
        http_fetch_json("/put", dict(), 400, 'MISSING_ARG_KEY')
        http_fetch_json("/put", dict(key='test_missing'), 400, 'MISSING_ARG_VALUE')
        http_fetch_json("/get", dict(), 400, 'MISSING_ARG_KEY')
        http_fetch_json("/del", dict(), 400, 'MISSING_ARG_KEY')
        http_fetch_json("/fwmatch", dict(), 400, 'MISSING_ARG_KEY')
        http_fetch("/put", dict(format="txt"), 400)
        http_fetch("/put", dict(format='txt', key='test_missing'), 400)
        http_fetch("/get", dict(format='txt'), 400)
        http_fetch("/del", dict(format='txt'), 400)
        http_fetch_json("/get", dict(key="test_missing", separator="asdf"), 400, 'INVALID_SEPARATOR')
        http_fetch_json("/get", dict(key="test_missing", separator=""), 400, 'INVALID_SEPARATOR')
        http_fetch("/get", dict(format="txt", key="test_missing", separator=""), 400)
        http_fetch_json('/mput', dict(), 400, 'MISSING_ARG_VALUE', body='.')
        http_fetch_json('/mput', dict(), 400, 'MALFORMED_CSV', body='vvvvvvvvvv')
        http_fetch_json('/mput', dict(separator='|'), 400, 'MALFORMED_CSV', body='test_missing,val1\n')
        http_fetch_json('/mget', dict(separator=';;'), 400, 'INVALID_SEPARATOR')
        http_fetch_json('/mget', dict(), 400, 'MISSING_ARG_KEY')
        http_fetch('/mget', dict(format="txt"), 400)
        http_fetch_json('/fwmatch', dict(), 400, 'MISSING_ARG_KEY')
        http_fetch_json('/range_match', dict(), 400, 'MISSING_ARG_KEY')
        http_fetch_json('/range_match', dict(start="a"), 400, 'MISSING_ARG_KEY')
        http_fetch_json('/range_match', dict(end="b"), 400, 'MISSING_ARG_KEY')
        http_fetch_json('/range_match', dict(start="b", end="a"), 400, 'INVALID_START_KEY')
    
    def test_multikey(self):
        http_fetch_json('/put', dict(key='test_multikey_1', value='asdf1'))
        http_fetch_json('/put', dict(key='test_multikey_2', value='asdf2'))
        data = http_fetch_json('/mget', dict(key=['test_multikey_1', 'test_multikey_2', 'test_multikey_3']))
        assert data == [{'key': 'test_multikey_1', 'value': 'asdf1'},
                        {'key': 'test_multikey_2', 'value': 'asdf2'}]
        data = http_fetch('/mget', dict(key=['test_multikey_1', 'test_multikey_2', 'test_multikey_3'], format='txt'))
        assert data == 'test_multikey_1,asdf1\ntest_multikey_2,asdf2\n'
        data = http_fetch_json("/fwmatch", dict(key="test_multikey"))
        assert data == [{'test_multikey_1': 'asdf1'}, {'test_multikey_2': 'asdf2'}]
        data = http_fetch("/fwmatch", dict(key="test_multikey", format="txt"))
        assert data == "test_multikey_1,asdf1\ntest_multikey_2,asdf2\n"
        http_fetch_json('/mput', body='test_multikey_3,mv1a;mv1b\ntest_multikey_4,mv2a;mv2b\ntest_multikey_5,mv3a')
        data = http_fetch_json('/get', dict(key='test_multikey_3'))
        assert data == 'mv1a;mv1b'
        data = http_fetch('/mget', dict(key=['test_multikey_4', 'test_multikey_5'], format='txt'))
        assert data == 'test_multikey_4,mv2a;mv2b\ntest_multikey_5,mv3a\n'
        http_fetch_json('/mput', body='test_multikey_ms,a,b,c')
        data = http_fetch_json('/get', dict(key='test_multikey_ms'))
        assert data == 'a,b,c'
    
    def test_lists_1(self):
        http_fetch_json('/get', dict(key='list_test'), 404, 'NOT_FOUND')
        data = http_fetch_json('/list_append', dict(key='list_test', value='testvalue1', return_data='1'))
        assert data['value'] == ['testvalue1']
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
    
    def test_post(self):
        http_fetch_json('/put', dict(key='testpost'), body='asdfpost')
        data = http_fetch_json('/get', dict(key='testpost'))
        assert data == 'asdfpost'
        data = http_fetch_json('/del', dict(key='testpost'))
    
    def test_dump_csv(self):
        # we need to check more than 500 entries
        http_fetch_json('/put', dict(key='zzz.dump', value='bump.value'))
        for x in range(505):
            http_fetch_json('/put', dict(key='dump.%d' % x, value='dump.value.%d' % x))
        data = http_fetch('/dump_csv')
        assert data.startswith("dump.0,dump.value.0\n")
        assert data.count("\n") > 505
        data = http_fetch('/dump_csv', dict(key="dump."))
        assert data.count("\n") == 505
    
    def test_lists_2(self):
        http_fetch_json('/list_prepend', dict(key='new_list', value='appval1'))
        data = http_fetch_json('/list_prepend', dict(key='new_list', value='appvala', return_data='1'))
        assert data['value'] == ['appvala', 'appval1']
        data = http_fetch('/list_prepend', dict(key='new_list', value='appvalb', return_data='1', format='txt'))
        assert data == 'new_list,appvalb,appvala,appval1\n'
        data = http_fetch_json('/get', dict(key='new_list'))
        assert data == 'appvalb,appvala,appval1'
        data = http_fetch_json('/list_pop', dict(key='new_list'))
        assert data['popped'] == ['appvalb']
        data = http_fetch_json('/get', dict(key='new_list'))
        assert data == 'appvala,appval1'
        data = http_fetch('/list_pop', dict(key='new_list', position='-1', format='txt'))
        assert data == 'appval1\n'
        data = http_fetch_json('/get', dict(key='new_list'))
        assert data == 'appvala'
        http_fetch_json('/list_append', dict(key='new_list', value=["blah1", "blah2", "blah3"]))
        data = http_fetch_json('/get', dict(key='new_list'))
        assert data == 'appvala,blah1,blah2,blah3'
        data = http_fetch_json('/list_pop', dict(key='new_list', position='2', count='2'))
        assert data['popped'] == ['blah2', 'blah3']

    def test_sets_1(self):
        http_fetch_json('/set_add', dict(key='testset', value=['si1', 'si2']))
        http_fetch_json('/set_remove', dict(key='testset', value='si1'))
        data = http_fetch_json('/get', dict(key='testset'))
        assert data == 'si2'
        http_fetch_json('/set_add', dict(key='testset', value=['si1', 'si2', 'si3']))
        data = http_fetch_json('/get', dict(key='testset'))
        datalist = re.split(',', data)
        assert 'si1' in datalist
        assert 'si2' in datalist
        assert 'si3' in datalist
        assert ','.join(sorted(datalist)) == 'si1,si2,si3'
        http_fetch_json('/set_remove', dict(key='testset', value=['si3', 'si1', 'si2']))
        data = http_fetch_json('/get', dict(key='testset'))
        assert data == ''
        set_list = ['sl1', 'sl2', 'sl3', 'sl4']
        http_fetch_json('/set_add', dict(key='testset', value=set_list))
        for x in range(len(set_list)):
            data = http_fetch_json('/set_pop', dict(key='testset'))
            assert len(data['popped']) == 1
            assert data['popped'][0] in set_list
            set_list.remove(data['popped'][0])
        data = http_fetch_json('/set_pop', dict(key='testset'))
        assert data['popped'] == []
    
    def test_separators(self):
        http_fetch_json('/list_append', dict(key='testsep', value=['3,9', '4,16', '2,4'], separator='|'))
        data = http_fetch_json('/get', dict(key='testsep'))
        assert data == '3,9|4,16|2,4'
        http_fetch_json('/list_remove', dict(key='testsep', value='2,4', separator='|'))
        data = http_fetch_json('/get', dict(key='testsep'))
        assert data == '3,9|4,16'
        http_fetch_json('/set_add', dict(key='testsep', value=['5,25', '7,49'], separator='|'))
        http_fetch_json('/set_remove', dict(key='testsep', value=['3,9', '5,25'], separator='|'))
        data = http_fetch_json('/get', dict(key='testsep'))
        datalist = re.split('|', data)
        assert '|'.join(sorted(datalist)) == '4,16|7,49'
        http_fetch_json('/list_pop', dict(key='testsep', separator='long'), 400, 'INVALID_SEPARATOR')
        http_fetch_json('/mput', dict(separator='|'), body='testsep2|sepv2\ntestsep1|sepv1\n')
        data = http_fetch_json('/get', dict(key='testsep1'))
        assert data == 'sepv1'
        data = http_fetch_json('/get', dict(key='testsep2'))
        assert data == 'sepv2'


if __name__ == "__main__":
    print "usage: py.test"
