import pycurl

def read_cb(data):
    # note: this may not be called with per-message blocks of data
    print "data: **%s**" % (data)

def dbg_cb(debug_type, debug_msg):
    print "debug(%d): %s" % (debug_type, debug_msg)

def geturl(url):
    c = pycurl.Curl()
    c.setopt(pycurl.URL, url)
    #c.setopt(pycurl.VERBOSE, 1)
    c.setopt(pycurl.WRITEFUNCTION, read_cb)
    c.setopt(pycurl.FOLLOWLOCATION, 1)
    c.setopt(pycurl.NOSIGNAL, 1)
    c.setopt(pycurl.DEBUGFUNCTION, dbg_cb)
    c.perform()
    c.close()

geturl("http://localhost:8080/sub")
