CFLAGS = -I. -I../server/ -I$(LIBEVENT)/include -I/usr/local/include/json -Wall -g
LIBS = -L. -L../server/ -L$(LIBEVENT)/lib -L/usr/local/lib -levent -ljson -ltokyotyrant -ltokyocabinet -lz -lbz2 -lresolv -ldl -lpthread -lm -lc

simplequeue: simplequeue.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp -ljson
clean:
	rm -rf *.a *.o testserver *.dSYM
