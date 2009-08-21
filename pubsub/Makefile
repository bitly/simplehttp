CFLAGS = -I. -I../server -I$(LIBEVENT)/include -g
LIBS = -L. -L../server -L$(LIBEVENT)/lib -levent -L/usr/local/lib -lz -lbz2 -lresolv -ldl -lpthread -lm -lc


pubsub: pubsub.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.o pubsub *.dSYM
