CFLAGS = -I. -I../server -I$(LIBEVENT)/include -g
LIBS = -L. -L../server -L$(LIBEVENT)/lib -levent -L/usr/local/lib -ljson -ltokyotyrant -ltokyocabinet -lz -lbz2 -lresolv -ldl -lpthread -lm -lc

simplegeo: simplegeo.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.o simplegeo *.dSYM
