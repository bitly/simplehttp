CFLAGS = -I. -I../server -I$(LIBEVENT)/include -I/bitly/local/include -g
LIBS = -L. -L../server -L$(LIBEVENT)/lib -levent -L/usr/local/lib -L/bitly/local/lib -ljson -ltokyotyrant -ltokyocabinet -lz -lbz2 -lresolv -ldl -lpthread -lm -lc


simpleattributes: simpleattributes.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS) -lsimplehttp

clean:
	rm -rf *.o simpleattributes *.dSYM
