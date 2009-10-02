import time
import pycurl
from Subscription import Subscription

s = Subscription(None, debug=False)
while 1:
    try:
        #s.get("http://localhost:8080/sub")
        s.get("http://m01.bit.ly:8090/sub")
    except pycurl.error, e:
        if e[0] is not 7 and e[0] is not 18:
            raise
        time.sleep(5)
        pass

