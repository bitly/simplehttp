#define _GNU_SOURCE // for strndup()
#include "queuereader.h"

#ifdef DEBUG
#define _DEBUG(...) fprintf(stdout, __VA_ARGS__)
#else
#define _DEBUG(...) do {;} while (0)
#endif

#define MAX_BACKOFF_COUNTER 8

void queuereader_termination_handler(int signum);
void queuereader_trigger_source_request(int seconds, int microseconds);
void queuereader_free(void);
void queuereader_source_request(int fd, short what, void *cbarg);
void queuereader_source_cb(struct evhttp_request *req, void *cbarg);
void queuereader_requeue_message_request(int delay);
void queuereader_requeue_message_cb(struct evhttp_request *req, void *cbarg);
int queuereader_calculate_backoff_seconds();
void queuereader_increment_backoff(void);
void queuereader_decrement_backoff(void);

struct GlobalData {
    int (*message_cb)(struct json_object *json_msg, void *cbarg);
    void (*error_cb)(int status_code, void *cbarg);
    void *cbarg;
    const char *source_address;
    int source_port;
    const char *path;
    struct json_object *tasks;
    int max_tries;
};

static struct json_object *json_msg = NULL;
static int backoff_counter = 0;
static struct GlobalData *data = NULL;
static struct event ev;
static int sleeptime_queue_empty_ms = 500;
static int max_tries = 5;
static struct timeval sleeptime_queue_empty_tv;
extern struct event_base *current_base;

void queuereader_termination_handler(int signum)
{
    event_loopbreak();
}

int queuereader_calculate_backoff_seconds()
{
    int seconds;
    
    seconds = backoff_counter ? (1 << backoff_counter) : 0;
    if (seconds > 0) {
        fprintf(stderr, "NOTICE: backing off for %d seconds\n", seconds);
    }
    
    return seconds;
}

void queuereader_trigger_source_request(int seconds, int microseconds)
{
    struct timeval tv = { seconds, microseconds };
    
    evtimer_del(&ev);
    evtimer_set(&ev, queuereader_source_request, data);
    evtimer_add(&ev, &tv);
}

void queuereader_decrement_backoff(void)
{
    // bounded decrement of backoff counter
    backoff_counter = ((backoff_counter - 1) > 0) ? (backoff_counter - 1) : 0;
}

void queuereader_increment_backoff(void)
{
    // bounded increment of backoff counter
    backoff_counter = ((backoff_counter + 1) > MAX_BACKOFF_COUNTER) ?
                      MAX_BACKOFF_COUNTER : (backoff_counter + 1);
}

void queuereader_finish_message(int return_code)
{
    switch (return_code) {
        case QR_CONT:
            // do nothing
            break;
        case QR_EMPTY:
            queuereader_trigger_source_request(sleeptime_queue_empty_tv.tv_sec, sleeptime_queue_empty_tv.tv_usec);
            break;
        case QR_SUCCESS:
            queuereader_decrement_backoff();
            queuereader_trigger_source_request(queuereader_calculate_backoff_seconds(), 0);
            break;
        case QR_FAILURE:
            queuereader_increment_backoff();
            queuereader_requeue_message_request(1);
            break;
        case QR_CONT_SOURCE_REQUEST:
            queuereader_trigger_source_request(queuereader_calculate_backoff_seconds(), 0);
            break;
        case QR_REQUEUE_WITHOUT_BACKOFF:
            queuereader_requeue_message_request(1);
            break;
        case QR_REQUEUE_WITHOUT_DELAY:
            queuereader_requeue_message_request(0);
            break;
    }
}

void queuereader_requeue_message_cb(struct evhttp_request *req, void *cbarg)
{
    // TODO: this should limit (and perhaps backoff) failed requeue requests
    queuereader_finish_message((req && req->response_code == 200) ?
                               QR_CONT_SOURCE_REQUEST : QR_REQUEUE_WITHOUT_BACKOFF);
}

void queuereader_requeue_message_request(int delay)
{
    struct evbuffer *evb;
    const char *message;
    char *encoded_message;
    struct json_object *tmp_obj;
    int tries;
    time_t retry_on;
    
    tries = json_object_get_int(json_object_object_get(json_msg, "tries"));
    if (tries > max_tries) {
        // TODO: dump message
        queuereader_finish_message(QR_CONT_SOURCE_REQUEST);
        return;
    }
    
    tmp_obj = json_object_object_get(json_msg, "retry_on");
    retry_on = json_object_get_int(tmp_obj);
    if (delay) {
        if (!retry_on) {
            retry_on = time(NULL);
        }
        retry_on += 90;
        json_object_object_add(json_msg, "retry_on", json_object_new_int(retry_on));
    }
    
    message = json_object_to_json_string(json_msg);
    fprintf(stderr, "NOTICE: requeue message %s\n", message);
    evb = evbuffer_new();
    encoded_message = simplehttp_encode_uri(message);
    evbuffer_add_printf(evb, "/put?data=%s", encoded_message);
    new_async_request((char *)data->source_address, data->source_port,
                      (char *)EVBUFFER_DATA(evb), queuereader_requeue_message_cb, (void *)NULL);
    evbuffer_free(evb);
    free(encoded_message);
}

void queuereader_source_cb(struct evhttp_request *req, void *cbarg)
{
    struct GlobalData *client_data = (struct GlobalData *)cbarg;
    char *line;
    size_t line_len;
    struct evbuffer *evb;
    struct json_object *new_json_msg;
    struct json_object *tasks_array;
    struct json_object *tmp_obj;
    char *message;
    int ret = QR_EMPTY;
    time_t retry_on;
    
    if (json_msg) {
        json_object_put(json_msg);
        json_msg = NULL;
    }
    
    evb = req->input_buffer;
    line = (char *)EVBUFFER_DATA(evb);
    line_len = EVBUFFER_LENGTH(evb);
    if (line_len) {
        message = strndup(line, line_len);
        
        json_msg = json_tokener_parse(message);
        if (!json_msg) {
            fprintf(stdout, "ERROR: failed to parse JSON (%s)\n", message);
            ret = QR_CONT_SOURCE_REQUEST;
        } else {
            tmp_obj = json_object_object_get(json_msg, "data");
            if (!tmp_obj) {
                new_json_msg = json_object_new_object();
                json_object_object_add(new_json_msg, "data", json_msg);
                json_object_object_add(new_json_msg, "tries", json_object_new_int(0));
                json_object_object_add(new_json_msg, "retry_on", NULL);
                json_object_object_add(new_json_msg, "started", json_object_new_int(time(NULL)));
                json_msg = new_json_msg;
            }
            
            tmp_obj = json_object_object_get(json_msg, "tasks_left");
            if (!tmp_obj) {
                tasks_array = queuereader_copy_tasks(client_data->tasks);
                json_object_object_add(json_msg, "tasks_left", tasks_array);
            }
            
            retry_on = json_object_get_int(json_object_object_get(json_msg, "retry_on"));
            if (retry_on > time(NULL)) {
                ret = QR_REQUEUE_WITHOUT_DELAY;
            } else {
                ret = (*client_data->message_cb)(json_msg, client_data->cbarg);
            }
        }
        
        free(message);
    }
    
    queuereader_finish_message(ret);
}

struct json_object *queuereader_copy_tasks(struct json_object *input_array)
{
    struct json_object *tasks_array;
    const char *task;
    int i;
    
    tasks_array = json_object_new_array();
    for (i = 0; i < json_object_array_length(input_array); i++) {
        task = json_object_get_string(json_object_array_get_idx(input_array, i));
        json_object_array_add(tasks_array, json_object_new_string(task));
    }
    
    return tasks_array;
}

void queuereader_finish_task_by_name(const char *finished_task)
{
    struct json_object *new_tasks_array;
    struct json_object *tasks;
    int i;
    const char *task;
    
    // walk the array of tasks_left and skip the one that matches the tast string specified...
    tasks = json_object_object_get(json_msg, "tasks_left");
    new_tasks_array = json_object_new_array();
    for (i = 0; i < json_object_array_length(tasks); i++) {
        task = json_object_get_string(json_object_array_get_idx(tasks, i));
        if (strcmp(finished_task, task) != 0) {
            json_object_array_add(new_tasks_array, json_object_new_string(task));
        }
    }
    json_object_object_add(json_msg, "tasks_left", new_tasks_array);
}

void queuereader_finish_task(int index)
{
    struct json_object *new_tasks_array;
    struct json_object *tasks;
    int i;
    const char *task;
    
    // walk the array of tasks_left and skip the one that matches the index specified...
    tasks = json_object_object_get(json_msg, "tasks_left");
    new_tasks_array = json_object_new_array();
    for (i = 0; (i < json_object_array_length(tasks)) && (i != index); i++) {
        task = json_object_get_string(json_object_array_get_idx(tasks, i));
        json_object_array_add(new_tasks_array, json_object_new_string(task));
    }
    json_object_object_add(json_msg, "tasks_left", new_tasks_array);
}

void queuereader_source_request(int fd, short what, void *cbarg)
{
    struct GlobalData *client_data = (struct GlobalData *)cbarg;
    struct evbuffer *evb;
    
    evb = evbuffer_new();
    evbuffer_add_printf(evb, client_data->path);
    new_async_request((char *)client_data->source_address, client_data->source_port,
                      (char *)EVBUFFER_DATA(evb), queuereader_source_cb, cbarg);
    evbuffer_free(evb);
}

void queuereader_set_sleeptime_queue_empty_ms(int milliseconds)
{
    sleeptime_queue_empty_ms = milliseconds;
}

void queuereader_init(struct json_object *tasks, 
                      const char *source_address, int source_port, const char *path,
                      int (*message_cb)(struct json_object *json_msg, void *arg),
                      void (*error_cb)(int status_code, void *arg),
                      void *cbarg)
{
    int timeout_seconds = (sleeptime_queue_empty_ms * 1000) / 1000000;
    int timeout_microseconds = (sleeptime_queue_empty_ms * 1000) - (timeout_seconds * 1000000);
    
    sleeptime_queue_empty_tv.tv_sec = timeout_seconds;
    sleeptime_queue_empty_tv.tv_usec = timeout_microseconds;
    
    signal(SIGINT, queuereader_termination_handler);
    signal(SIGQUIT, queuereader_termination_handler);
    signal(SIGTERM, queuereader_termination_handler);
    signal(SIGHUP, queuereader_termination_handler);
    
    if (!current_base) {
        event_init();
    }
    
    data = calloc(1, sizeof(struct GlobalData));
    data->message_cb = message_cb;
    data->error_cb = error_cb;
    data->cbarg = cbarg;
    data->source_address = source_address;
    data->source_port = source_port;
    data->path = path;
    data->tasks = tasks;
    data->max_tries = max_tries;
    
    queuereader_trigger_source_request(sleeptime_queue_empty_tv.tv_sec, sleeptime_queue_empty_tv.tv_usec);
}

int queuereader_main(struct json_object *tasks,
                     const char *source_address, int source_port, const char *path,
                     int (*message_cb)(struct json_object *json_msg, void *arg),
                     void (*error_cb)(int status_code, void *arg),
                     void *cbarg)
{
    queuereader_init(tasks, source_address, source_port, path, message_cb, error_cb, cbarg);
    queuereader_run();
    queuereader_free();
    return 0;
}

void queuereader_run(void)
{
    event_dispatch();
}

void queuereader_free(void)
{
    free(data);
}
