import tornado.iostream
import tornado.ioloop
import socket
import logging
import urlparse
import functools
import base64


class HTTPError(Exception):
    def __init__(self, code, msg=None):
        self.code = code
        self.msg = msg
        super(HTTPError, self).__init__('%s %s' % (code , msg))

class PubsubReader(object):
    def __init__(self, pubsub_url, io_loop=None):
        self.io_loop = io_loop or tornado.ioloop.IOLoop.instance()
        self.socket = None

        urlinfo = urlparse.urlparse(pubsub_url)
        assert urlinfo.scheme == 'http'
        netloc = urlinfo.netloc
        self.basic_auth = None
        port = 80
        if '@' in netloc:
            self.basic_auth, netloc = netloc.split('@', 1)
        if ':' in netloc:
            netloc, port = netloc.rsplit(':', 1)
            port = int(port)
        self.host = netloc
        self.port = port
        self.get_line = urlparse.urlunparse(('', '', urlinfo.path, urlinfo.params, urlinfo.query, urlinfo.fragment))
    
    def start(self):
        self.open(self.host, self.port)
        self.io_loop.start()
    
    def _callback(self, data):
        try:
            self.callback(data[:-1])
        except Exception:
            logging.exception('failed in callback')
        # NOTE: to work around a maximum recursion error (later fix by https://github.com/facebook/tornado/commit/f8f3a9bf08f1cab1d2ab232074a14e7a94eaa4b1)
        # we schedule the read_until for later call by the io_loop
        # this can go away w/ tornado 2.0
        callback = functools.partial(self.stream.read_until, '\n', self._callback)
        self.io_loop.add_callback(callback)
        
    def callback(self, data):
        raise Exception("Not Implemented")
        
    def close(self):
        logging.info('closed')
        self.io_loop.stop()
    
    def http_get_line(self):
        line = "GET %s HTTP/1.0\r\n" % self.get_line
        if self.basic_auth:
            line += "Authorization: Basic %s\r\n" % base64.b64encode(self.basic_auth)
        return line + "\r\n"
    
    def open(self, host, port):
        assert isinstance(port, int)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        logging.info('opening socket to %s:%s' % (host, port))
        self.socket.connect((host, port))
        self.stream = tornado.iostream.IOStream(self.socket)
        self.stream.set_close_callback(self.close)
        get_line = self.http_get_line()
        logging.info(get_line)
        self.stream.write(get_line)
        self.stream.read_until("\r\n\r\n", self.on_headers)
    
    def on_headers(self, data):
        headers = {}
        lines = data.split("\r\n")
        status_line = lines[0]
        if status_line.count(' ') < 2:
            raise HTTPError(599, 'connect error')
        status_code = status_line.split(' ', 2)[1]
        if status_code != "200":
            raise HTTPError(status_code)
        for line in lines[1:]:
           parts = line.split(":")
           if len(parts) == 2:
               headers[parts[0].strip()] = parts[1].strip()
        self.stream.read_until('\n', self._callback)
    
