CFLAGS = -I../server -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L../server -L$(LIBEVENT)/lib -levent 

simplequeue: simplequeue.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

install:
	/usr/bin/install simplequeue $(LIBEVENT)/bin

clean:
	rm -rf *.a *.o simplequeue *.dSYM
