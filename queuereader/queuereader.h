#ifndef __queuereader_h
#define __queuereader_h

#include <event.h>

#define QR_CONT                     0
#define QR_EMPTY                    1
#define QR_FAILURE                  2
#define QR_SUCCESS                  3
#define QR_CONT_SOURCE_REQUEST      4
#define QR_REQUEUE_WITHOUT_BACKOFF  5

int queuereader_main(const char *source_address, int source_port, const char *path,
                      int (*message_cb)(char *data, void *arg),
                      void (*error_cb)(int status_code, void *arg),
                      void *cbarg);
void queuereader_finish_message(int return_code);
void queuereader_set_sleeptime_queue_empty_ms(int milliseconds);

#endif
