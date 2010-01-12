#include "event.h"

int
    pubsub_to_pubsub_main(char *source_address, int source_port, char *target_address, int target_port, int (*cb)(char *data, struct evbuffer *evb, char **target_path, void *arg), void *cbarg);
int
    pubsub_to_pubsub_main_path(char *source_address, int source_port, char *target_address, int target_port, int (*cb)(char *data, struct evbuffer *evb, char **target_path, void *arg), void *cbarg, char *target_path);
