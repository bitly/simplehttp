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
import urllib
import random

from pysimplehttp.pubsub_reader import PubsubReader


class PubsubToSimplequeue(PubsubReader):
    def __init__(self, simplequeue_urls, **kwargs):
        assert isinstance(simplequeue_urls, (list, tuple))
        self.simplequeue_urls = simplequeue_urls
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
        if not data:
            return
        # logging.debug(data)
        assert isinstance(data, str)
        endpoint = random.choice(self.simplequeue_urls) + '/put'
        self.http_fetch(endpoint, dict(data=data), callback=self._finish)


if __name__ == "__main__":
    tornado.options.define('pubsub_url', type=str, default="http://127.0.0.1:8080/sub?multipart=0", help="url for pubsub to read from")
    tornado.options.define('simplequeue_url', type=str, multiple=True, help="(multiple) url(s) for simplequeue to write to")
    tornado.options.parse_command_line()
    
    reader = PubsubToSimplequeue(
        simplequeue_urls=tornado.options.options.simplequeue_url, 
        pubsub_url=tornado.options.options.pubsub_url
    )
    reader.start()
