CFLAGS = -I. -I../server -I/usr/local/include/json -g
LIBS = -L. -L../server -L/usr/local/lib -levent -ljson -lz -lbz2 -lresolv -ldl -lpthread -lm -lc


pubsub: pubsub.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp 

clean:
	rm -rf *.o pubsub *.dSYM
