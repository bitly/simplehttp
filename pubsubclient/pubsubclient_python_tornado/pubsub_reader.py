import tornado.iostream
import tornado.ioloop
import socket
import logging

class HTTPError(Exception):
    def __init__(self, code, msg=None):
        self.code = code
        self.msg = msg
        super(HTTPError, self).__init__('%s %s' % (code , msg))

class PubsubReader(object):
    def __init__(self, io_loop=None):
        self.io_loop = io_loop or tornado.ioloop.IOLoop.instance()
        self.socket = None
    
    def _callback(self, data):
        try:
            self.callback(data)
        except:
            logging.exception('failed in callback')
        self.stream.read_until('\n', self._callback)
        
    def callback(self, data):
        raise
        
    def close(self):
        logging.info('closed')
        self.io_loop.stop()
    
    def http_get_line(self):
        return "GET /sub?multipart=0 HTTP/1.0\r\n\r\n"
    
    def open(self, host, port=80):
        self.host = host
        self.port = port
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
        logging.info(lines)
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
    
