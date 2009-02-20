from SimpleQueue import SimpleQueue
from time import sleep

queue = SimpleQueue()
i = 0

while 1:
    data = queue.get()
    
    if data == '':
        sleep(.5)
        continue
    
    i = i + 1
    print "%d:%s" % (i, data)