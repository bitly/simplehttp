CFLAGS = -I. -I../server/ -I$(LIBEVENT)/include -Wall -g
LIBS = -L. -L../server/ -L$(LIBEVENT)/lib -levent

simplequeue: simplequeue.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp
clean:
	rm -rf *.a *.o testserver *.dSYM
