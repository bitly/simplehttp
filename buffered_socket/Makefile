TARGET ?= /usr/local
LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local

CFLAGS += -I. -I$(LIBSIMPLEHTTP)/include -I.. -I$(LIBEVENT)/include -g -Wall -O2
LIBS = -L. -L$(LIBEVENT)/lib -L/usr/local/lib -levent -lbuffered_socket
AR = ar
AR_FLAGS = rc
RANLIB = ranlib

all: libbuffered_socket.a demo

libbuffered_socket.a: buffered_socket.o
	/bin/rm -f $@
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

demo: demo.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

install:
	/usr/bin/install -d $(TARGET)/lib/
	/usr/bin/install -d $(TARGET)/bin/
	/usr/bin/install -d $(TARGET)/include/buffered_socket
	/usr/bin/install libbuffered_socket.a $(TARGET)/lib/
	/usr/bin/install buffered_socket.h $(TARGET)/include/buffered_socket

clean:
	/bin/rm -f *.a *.o demo
