"""
Subscription Client

This client will block and raise a pycurl error on failure. Curl error 18
is 'transfer closed with outstanding read data remaining', 7 is a connect
error. The other error codes are here:

http://curl.haxx.se/libcurl/c/libcurl-errors.html


Example bitchen client
----------------------
s = Subscription(None, debug=False)
while 1:
    try:
        s.get("http://localhost:8080/sub")
    except pycurl.error, e:
        if e[0] is not 7 and e[0] is not 18:
            raise
        time.sleep(5)
        pass


Example bash publisher
----------------------
while [ 1 ]; do curl -d "test chunk data" 'http://localhost:8080/pub'; sleep 2; done
"""

import re
import sys
import time
import pycurl
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass


class Subscription:
    def __init__(self, subcb, debug=False):
        self.debug = debug
        self.chunk = ""
        self.nchunks = 0 
        self.ihdr = {}
        if subcb is not None:
            self.subcb = subcb
        else:
            self.subcb = self.default_subs_cb

    def dbg_cb(self, debug_type, debug_msg):
        print "debug(%d): %s" % (debug_type, debug_msg)

    def default_subs_cb(self, data):
        print "chunk(%d): %.100s" % (self.nchunks, data)

    def parse_hdr(self, buf):
        lines = buf.splitlines()
        s = lines[0].split()
        if len(s) is 3 and s[0].startswith("HTTP"):
            self.version = s[0]
            self.code    = s[1]
            self.reason  = s[2]
            lines = lines[1:]

        for l in lines:
            p = l.split(":",1)
            if len(p) == 2:
                k = p[0].lower()
                self.ihdr[k] = p[1].strip()
                m = re.match(".*boundary=(\w+)", p[1])
                if m is not None:
                    self.boundary = m.groups()[0]

    def header_cb(self, data):
        self.parse_hdr(data)

    def body_cb(self, data):
        self.chunk += data
        m = re.match(r"^(.*)(?:\n|\r\n?){2}(.*)(?:\n|\r\n?)--"+self.boundary+".*", self.chunk, re.DOTALL)
        if m is not None:
            self.nchunks += 1
            self.subcb(m.groups()[1])
            self.chunk = ''

    def get(self, url):
        c = pycurl.Curl()
        c.setopt(pycurl.URL, url)
        c.setopt(pycurl.HEADERFUNCTION, self.header_cb)
        c.setopt(pycurl.WRITEFUNCTION, self.body_cb)
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.NOSIGNAL, 1)
        if self.debug is True:
            c.setopt(pycurl.DEBUGFUNCTION, self.dbg_cb)
            c.setopt(pycurl.VERBOSE, 1)
        try:
            c.perform()
        except pycurl.error, e:
            print "pycurl error:", e.args
            c.close()
            raise
        c.close()


