import sys
import traceback
import pycurl
import cStringIO
import urllib

class SimpleQueue:
    def __init__(self, address='127.0.0.1', port=8080, debug=False):
        self.address = address
        self.port = port
        self.debug = debug
        
    def request(self, url, timeout_ms=500):
        buffer = cStringIO.StringIO()
        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.TIMEOUT_MS, timeout_ms)
        curl.setopt(pycurl.WRITEFUNCTION, buffer.write)
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl.perform()
        result = buffer.getvalue()
        buffer.close()
        curl.close()

        return result
      
    def put(self, data, timeout_ms=500):
        url = "http://%s:%d/put?data=%s" % (self.address, self.port, urllib.quote(data))
        
        try:
            self.request(url, timeout_ms)
            return True
        except:
            traceback.print_tb(sys.exc_info()[2])
            return False
            
    def get(self, timeout_ms=500):
        url = "http://%s:%d/get" % (self.address, self.port)
        
        try:
            return self.request(url, timeout_ms)
        except:
            traceback.print_tb(sys.exc_info()[2])
            return False
            
    def dump(self, timeout_ms=500):
        url = "http://%s:%d/dump" % (self.address, self.port)
        
        try:
            return self.request(url, timeout_ms).strip().split('\n')
        except:
            traceback.print_tb(sys.exc_info()[2])
            return False
            
    def stats(self, timeout_ms=500, reset=False):
        url = "http://%s:%d/stats" % (self.address, self.port)
        stats = {}
        
        if reset:
            url = "%s?reset=1" % url
        
        try:
            response = self.request(url, timeout_ms)
            
            if reset:
                return True
                
            for line in response.split('\n'):
                parts = line.split(':')  
                key = parts[0]
                value = parts[1]
                stats[key] = value
                
            return stats
        except:
            traceback.print_tb(sys.exc_info()[2])
            return False