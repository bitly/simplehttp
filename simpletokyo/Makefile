CFLAGS = -I. -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L. -L$(LIBEVENT)/lib -levent -ljson -ltokyotyrant -ltokyocabinet -lsimplehttp

simpletokyo: simpletokyo.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	/usr/bin/install -D simpletokyo $(LIBEVENT)/bin/simpletokyo

clean:
	rm -rf *.o simpletokyo *.dSYM
