#include "event.h"

int
pubsub_to_pubsub_main(char *source_address, int source_port, char *target_address, int target_port, int (*cb)(char *data, struct evbuffer *evb, void *arg), void *cbarg);
