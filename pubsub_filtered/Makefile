TARGET ?= /usr/local
LIBEVENT ?= /usr/local
LIBSIMPLEHTTP ?= /usr/local
BLDDIR = build

BINARIES = pubsub_filtered stream_filter

SRCS_pubsub_filtered = pubsub_filtered.c md5.c shared.c
SRCS_stream_filter   = stream_filter.c md5.c shared.c

all: $(BINARIES)

CFLAGS = -I. -I$(LIBSIMPLEHTTP)/include -I.. -I$(LIBEVENT)/include -g 
LDFLAGS_pubsub_filtered = -L. -L$(LIBSIMPLEHTTP)/lib -L../simplehttp -L$(LIBEVENT)/lib -levent -lsimplehttp -ljson -lpcre -lm -lpubsubclient -lcrypto
LDFLAGS_stream_filter = -L. -L$(LIBSIMPLEHTTP)/lib -L../simplehttp -lsimplehttp -ljson

OBJS_pubsub_filtered := $(patsubst %.c, $(BLDDIR)/%.o, $(SRCS_pubsub_filtered))
OBJS_stream_filter   := $(patsubst %.c, $(BLDDIR)/%.o, $(SRCS_stream_filter))
OBJS_all             := $(OBJS_pubsub_filtered) $(OBJS_stream_filter)

DEPS := $(patsubst %.o, %.o.deps, $(OBJS_all))
# sort removes duplicates
-include $(sort $(DEPS))

pubsub_filtered: $(OBJS_pubsub_filtered)
stream_filter: $(OBJS_stream_filter)

$(BINARIES): %:
	@echo " LD " $@
	@mkdir -p $(dir $@)
	@$(CC) -o $@ $(OBJS_$@) $(LDFLAGS_$@)

$(BLDDIR)/%.o: %.c
	@echo " CC " $@
	@mkdir -p $(dir $@)
	@$(CC) -o $@ -c $< $(CFLAGS) -MMD -MF $@.deps

install: $(patsubst %, $(TARGET)/bin/%, $(BINARIES))

$(TARGET)/bin/%: %
	@echo " INSTALL " $@
	@/usr/bin/install -d $(dir $@)
	@/usr/bin/install -D $< $@

clean:
	@echo " RM " $(BINARIES) $(OBJS_all) $(DEPS)
	@rm -rf      $(BINARIES) $(OBJS_all) $(DEPS)

.PHONY: all install clean
