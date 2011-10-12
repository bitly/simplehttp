LIBEVENT ?= /usr/local
TARGET ?= /usr/local
LIBSIMPLEHTTP ?= ../simplehttp
LIBSIMPLEHTTP_INC ?= $(LIBSIMPLEHTTP)/..
LIBSIMPLEHTTP_LIB ?= $(LIBSIMPLEHTTP)

CFLAGS = -I$(LIBSIMPLEHTTP_INC) -I$(LIBEVENT)/include -Wall -g -O2
LIBS = -L$(LIBSIMPLEHTTP_LIB) -L$(LIBEVENT)/lib -levent -lsimplehttp -lm -lJudy -lpcre 

jujufly: jujufly.c j_arg_d.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/bin
	/usr/bin/install jujufly $(TARGET)/bin

clean:
	rm -rf *.a *.o jujufly *.dSYM test_output test.db
