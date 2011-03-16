#ifndef _REQUEST_H
#define _REQUEST_H

struct simplehttp_request {
    struct evhttp_request *req;
    simplehttp_ts start_ts;
    uint64_t id;
    int index;
    int async;
    TAILQ_ENTRY(simplehttp_request) entries;
};
TAILQ_HEAD(, simplehttp_request) simplehttp_reqs;

struct simplehttp_request *simplehttp_request_new(struct evhttp_request *req, uint64_t id);
struct simplehttp_request *simplehttp_request_get(struct evhttp_request *req);
struct simplehttp_request *simplehttp_async_check(struct evhttp_request *req);
void simplehttp_request_finish(struct evhttp_request *req, struct simplehttp_request *s_req);;

#endif
