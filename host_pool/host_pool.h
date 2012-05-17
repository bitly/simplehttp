#ifndef __host_pool_h
#define __host_pool_h

#include <uthash.h>
#include <time.h>

struct HostPoolEndpoint {
    int id;
    int alive;
    int retry_count;
    int retry_delay;
    time_t next_retry;
    char *address;
    int port;
    char *path;
    UT_hash_handle hh;
};

struct HostPool {
    int count;
    int retry_failed_hosts;
    int retry_interval;
    time_t max_retry_interval;
    int reset_on_all_failed;
    struct HostPoolEndpoint *endpoints;
    struct HostPoolEndpoint *current_endpoint;
    int64_t checkpoint;
};

enum HostPoolEndpointSelectionMode {
    HOST_POOL_RANDOM,
    HOST_POOL_ROUND_ROBIN,
    HOST_POOL_SINGLE
};

struct HostPool *new_host_pool(int retry_failed_hosts, int retry_interval,
                               int max_retry_interval, int reset_on_all_failed);
void free_host_pool(struct HostPool *host_pool);
struct HostPoolEndpoint *new_host_pool_endpoint(struct HostPool *host_pool,
        const char *address, int port, char *path);
void free_host_pool_endpoint(struct HostPoolEndpoint *host_pool_endpoint);
void host_pool_from_json(struct HostPool *host_pool, json_object *host_pool_endpoint_list);
struct HostPoolEndpoint *host_pool_get_endpoint(struct HostPool *host_pool,
        enum HostPoolEndpointSelectionMode mode, int64_t state);
struct HostPoolEndpoint *host_pool_next_endpoint(struct HostPool *host_pool,
        enum HostPoolEndpointSelectionMode mode, int64_t state);
void host_pool_mark_success(struct HostPool *host_pool, int id);
void host_pool_mark_failed(struct HostPool *host_pool, int id);
void host_pool_reset(struct HostPool *host_pool);

#endif
