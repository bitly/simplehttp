#ifndef __queuereader_h
#define __queuereader_h

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include <evhttp.h>
#include <signal.h>
#include <time.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>

#define QR_CONT                     0
#define QR_EMPTY                    1
#define QR_FAILURE                  2
#define QR_SUCCESS                  3
#define QR_CONT_SOURCE_REQUEST      4
#define QR_REQUEUE_WITHOUT_BACKOFF  5
#define QR_REQUEUE_WITHOUT_DELAY    6


int queuereader_main(struct json_object *tasks,
                     const char *source_address, int source_port, const char *path,
                     int (*message_cb)(struct json_object *json_msg, void *arg),
                     void (*error_cb)(int status_code, void *arg),
                     void *cbarg);
void queuereader_run();
void queuereader_free();
void queuereader_init(struct json_object *tasks,
                      const char *source_address, int source_port, const char *path,
                      int (*message_cb)(struct json_object *json_msg, void *arg),
                      void (*error_cb)(int status_code, void *arg),
                      void *cbarg);
void queuereader_finish_message(int return_code);
void queuereader_set_sleeptime_queue_empty_ms(int milliseconds);
struct json_object *queuereader_copy_tasks(struct json_object *input_array);
void queuereader_finish_task_by_name(const char *finished_task);

#endif
