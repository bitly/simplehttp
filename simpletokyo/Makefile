LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I. -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L. -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -L/usr/local/lib -levent -ljson -ltokyotyrant -ltokyocabinet -lsimplehttp

simpletokyo: simpletokyo.c ../lib/timer.c ../lib/util.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install simpletokyo $(TARGET)/bin/

clean:
	rm -rf *.o simpletokyo *.dSYM test_output
