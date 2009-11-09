CFLAGS = -I../server/ -I$(LIBEVENT)/include -Wall -g
LIBS = -L../server/ -L$(LIBEVENT)/lib -levent 

simplequeue: simplequeue.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp -ljson 

install:
	/usr/bin/install simplequeue $(LIBEVENT)/bin

clean:
	rm -rf *.a *.o simplequeue *.dSYM
