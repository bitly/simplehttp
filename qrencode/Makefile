CFLAGS = -I. -I../server -I/usr/local/include -I$(LIBEVENT)/include -g
LIBS = -L. -L../server -L$(LIBEVENT)/lib -levent -L/usr/local/lib -lqrencode -lpng -lz -lbz2 -lresolv -ldl -lpthread -lm -lc

qrencode: qrencode.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.o qrencode *.dSYM
