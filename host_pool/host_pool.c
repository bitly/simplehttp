#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <uthash.h>
#include <json/json.h>
#include <simplehttp/simplehttp.h>
#include "host_pool.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

/*
 * retry_failed_hosts - the number of times to retry a failed host. set to -1 for indefinite retries
 * retry_interval - seconds between retries. set to -1 for exponential backoff (ie: 1, 2, 4, 8, ...)
 * max_retry_interval - the maximum seconds to wait between retries
 * reset_on_all_failed - reset all hosts to alive when all are marked as failed.
 */
struct HostPool *new_host_pool(int retry_failed_hosts, int retry_interval,
                               int max_retry_interval, int reset_on_all_failed)
{
    struct HostPool *host_pool;
    
    host_pool = malloc(sizeof(struct HostPool));
    host_pool->count = 0;
    host_pool->retry_failed_hosts = retry_failed_hosts;
    host_pool->retry_interval = retry_interval;
    host_pool->max_retry_interval = max_retry_interval;
    host_pool->reset_on_all_failed = reset_on_all_failed;
    host_pool->endpoints = NULL;
    host_pool->current_endpoint = NULL;
    host_pool->checkpoint = -1;
    
    return host_pool;
}

void free_host_pool(struct HostPool *host_pool)
{
    struct HostPoolEndpoint *endpoint, *tmp;
    
    if (host_pool) {
        HASH_ITER(hh, host_pool->endpoints, endpoint, tmp) {
            HASH_DELETE(hh, host_pool->endpoints, endpoint);
            free_host_pool_endpoint(endpoint);
        }
        
        free(host_pool);
    }
}

struct HostPoolEndpoint *new_host_pool_endpoint(struct HostPool *host_pool,
        const char *address, int port, char *path)
{
    struct HostPoolEndpoint *host_pool_endpoint;
    
    host_pool_endpoint = malloc(sizeof(struct HostPoolEndpoint));
    host_pool_endpoint->address = strdup(address);
    host_pool_endpoint->port = port;
    host_pool_endpoint->path = strdup(path);
    host_pool_endpoint->id = host_pool->count++;
    host_pool_endpoint->alive = 1;
    host_pool_endpoint->retry_count = 0;
    host_pool_endpoint->retry_delay = 0;
    host_pool_endpoint->next_retry = 0;
    
    HASH_ADD_INT(host_pool->endpoints, id, host_pool_endpoint);
    
    return host_pool_endpoint;
}

void free_host_pool_endpoint(struct HostPoolEndpoint *host_pool_endpoint)
{
    if (host_pool_endpoint) {
        free(host_pool_endpoint->address);
        free(host_pool_endpoint->path);
        free(host_pool_endpoint);
    }
}

void host_pool_from_json(struct HostPool *host_pool, json_object *host_pool_endpoint_list)
{
    char *endpoint_url;
    char *address;
    char *path;
    int port;
    int i;
    
    for (i = 0; i < json_object_array_length(host_pool_endpoint_list); i++) {
        endpoint_url = (char *)json_object_get_string(json_object_array_get_idx(host_pool_endpoint_list, i));
        if (!simplehttp_parse_url(endpoint_url, strlen(endpoint_url), &address, &port, &path)) {
            fprintf(stderr, "ERROR: failed to parse host pool endpoint (%s)\n", endpoint_url);
            exit(1);
        }
        new_host_pool_endpoint(host_pool, address, port, path);
        free(address);
        free(path);
    }
}

struct HostPoolEndpoint *host_pool_get_endpoint(struct HostPool *host_pool,
        enum HostPoolEndpointSelectionMode mode, int64_t state)
{
    struct HostPoolEndpoint *endpoint;
    int c;
    time_t now;
    
    c = host_pool->count;
    while (c--) {
        endpoint = host_pool_next_endpoint(host_pool, mode, state);
        
        _DEBUG("HOST_POOL: trying #%d (%s:%d%s)\n", endpoint->id, endpoint->address, endpoint->port, endpoint->path);
        
        if (endpoint->alive) {
            return endpoint;
        }
        
        if ((host_pool->retry_failed_hosts == -1) ||
                (endpoint->retry_count <= host_pool->retry_failed_hosts)) {
            time(&now);
            if (endpoint->next_retry < now) {
                endpoint->retry_count++;
                if (host_pool->retry_interval == -1) {
                    endpoint->retry_delay = endpoint->retry_delay * 2;
                    if (endpoint->retry_delay > host_pool->max_retry_interval) {
                        endpoint->retry_delay = host_pool->max_retry_interval;
                    }
                } else {
                    endpoint->retry_delay = host_pool->retry_interval;
                }
                endpoint->next_retry = now + endpoint->retry_delay;
                return endpoint;
            }
        }
        
        // if any of the modes fail default to round robin in order
        // to find a suitable endpoint.
        //
        // this ensures we always try each endpoint in the host pool
        //
        // however, if we were asked to find a random endpoint, randomize once
        // more so that the endpoint following the failed endpoint won't get a
        // disproportionate number of additional requests
        if (mode == HOST_POOL_RANDOM) {
            host_pool_next_endpoint(host_pool, mode, state);
        }
        mode = HOST_POOL_ROUND_ROBIN;
    }
    
    if (host_pool->reset_on_all_failed) {
        host_pool_reset(host_pool);
        return host_pool_next_endpoint(host_pool, mode, 0);
    }
    
    return NULL;
}

struct HostPoolEndpoint *host_pool_next_endpoint(struct HostPool *host_pool,
        enum HostPoolEndpointSelectionMode mode, int64_t state)
{
    int index;
    
    switch (mode) {
        default:
        case HOST_POOL_RANDOM:
            // choose HOST_POOL_RANDOMly
            index = rand() % host_pool->count;
            HASH_FIND_INT(host_pool->endpoints, &index, host_pool->current_endpoint);
            break;
        case HOST_POOL_ROUND_ROBIN:
            // round-robin through the endpoints for each request
            host_pool->current_endpoint = host_pool->current_endpoint ?
                                          (host_pool->current_endpoint->hh.next ? host_pool->current_endpoint->hh.next :
                                           host_pool->endpoints) : host_pool->endpoints;
            break;
        case HOST_POOL_SINGLE:
            // choose the same endpoint for all requests for this message
            if (state != host_pool->checkpoint) {
                host_pool->checkpoint = state;
                host_pool->current_endpoint = host_pool->current_endpoint ?
                                              (host_pool->current_endpoint->hh.next ? host_pool->current_endpoint->hh.next :
                                               host_pool->endpoints) : host_pool->endpoints;
            }
            break;
    }
    
    assert(host_pool->current_endpoint != NULL);
    
    return host_pool->current_endpoint;
}

void host_pool_mark_success(struct HostPool *host_pool, int id)
{
    struct HostPoolEndpoint *endpoint;
    
    HASH_FIND_INT(host_pool->endpoints, &id, endpoint);
    assert(endpoint != NULL);
    
    _DEBUG("HOST_POOL: marking endpoint #%d (%s:%d%s) as SUCCESS\n",
           endpoint->id, endpoint->address, endpoint->port, endpoint->path);
           
    endpoint->alive = 1;
}

void host_pool_mark_failed(struct HostPool *host_pool, int id)
{
    struct HostPoolEndpoint *endpoint;
    time_t now;
    
    HASH_FIND_INT(host_pool->endpoints, &id, endpoint);
    assert(endpoint != NULL);
    
    _DEBUG("HOST_POOL: marking endpoint #%d (%s:%d%s) as FAILED\n",
           endpoint->id, endpoint->address, endpoint->port, endpoint->path);
           
    if (endpoint->alive) {
        endpoint->alive = 0;
        endpoint->retry_count = 0;
        if (host_pool->retry_interval == -1) {
            endpoint->retry_delay = 1;
        } else {
            endpoint->retry_delay = 0;
        }
        time(&now);
        endpoint->next_retry = now + endpoint->retry_delay;
    }
}

void host_pool_reset(struct HostPool *host_pool)
{
    struct HostPoolEndpoint *endpoint, *tmp;
    
    HASH_ITER(hh, host_pool->endpoints, endpoint, tmp) {
        host_pool_mark_success(host_pool, endpoint->id);
    }
}
