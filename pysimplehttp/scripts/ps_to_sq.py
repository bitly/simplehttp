#!/usr/bin/env python
"""
generic pubsub to simplequeue daemon that takes command line arguments:
    --pubsub-url=<http://127.0.0.1:8090/sub>
    (multiple) --simplequeue-url=<http://127.0.0.1:6000>

when multiple destination simplequeue arguments are specified, the daemon will 
randomly choose one endpoint to write a message to
"""
import logging
import tornado.httpclient
import tornado.options
import sys
import urllib
import random
try:
    import ujson as json
except ImportError:
    import json

from pysimplehttp.pubsub_reader import PubsubReader


class PubsubToSimplequeue(PubsubReader):
    def __init__(self, simplequeue_urls, filter_require, filter_exclude, **kwargs):
        assert isinstance(simplequeue_urls, (list, tuple))
        self.simplequeue_urls = simplequeue_urls
        self.filter_require = dict([data.split('=', 1) for data in filter_require])
        for key, value in self.filter_require.items():
            logging.info("requiring json key=%s value=%s" % (key, value) )
        self.filter_exclude = dict([data.split('=', 1) for data in filter_exclude])
        for key, value in self.filter_exclude.items():
            logging.info("excluding json key=%s value=%s" % (key, value) )
        self.http = tornado.httpclient.AsyncHTTPClient()
        super(PubsubToSimplequeue, self).__init__(**kwargs)
    
    def http_fetch(self, url, params, callback, headers={}):
        url += '?' + urllib.urlencode(params)
        req = tornado.httpclient.HTTPRequest(url=url,
                        method='GET',
                        follow_redirects=False,
                        headers=headers,
                        user_agent='ps_to_sq')
        self.http.fetch(req, callback=callback)
    
    def _finish(self, response):
        if response.code != 200:
            logging.info(response)
    
    def callback(self, data):
        """
        handle a single pubsub message
        """
        if not data or len(data) == 1:
            return
        assert isinstance(data, str)
        if self.filter_require or self.filter_exclude:
            try:
                msg = json.loads(data)
            except Exception:
                logging.error('failed json.loads(%r)' % data)
                return
            for key, value in self.filter_require.items():
                if msg.get(key) != value:
                    return
            for key, value in self.filter_exclude.items():
                if msg.get(key) == value:
                    return
        endpoint = random.choice(self.simplequeue_urls) + '/put'
        self.http_fetch(endpoint, dict(data=data), callback=self._finish)


if __name__ == "__main__":
    tornado.options.define('pubsub_url', type=str, default="http://127.0.0.1:8080/sub?multipart=0", help="url for pubsub to read from")
    tornado.options.define('simplequeue_url', type=str, multiple=True, help="(multiple) url(s) for simplequeue to write to")
    tornado.options.define('filter_require', type=str, multiple=True, help="filter json message to require for key=value")
    tornado.options.define('filter_exclude', type=str, multiple=True, help="filter json message to exclude for key=value")
    tornado.options.parse_command_line()
    if not tornado.options.options.pubsub_url:
        sys.stderr.write('--pubsub-url requrired\n')
        sys.exit(1)
        
    if not tornado.options.options.simplequeue_url:
        sys.stderr.write('--simplequeue-url requrired\n')
        sys.exit(1)
    
    reader = PubsubToSimplequeue(
        simplequeue_urls=tornado.options.options.simplequeue_url,
        filter_require=tornado.options.options.filter_require,
        filter_exclude=tornado.options.options.filter_exclude,
        pubsub_url=tornado.options.options.pubsub_url
    )
    reader.start()
