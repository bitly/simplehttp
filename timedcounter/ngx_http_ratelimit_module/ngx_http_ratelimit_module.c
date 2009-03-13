/*
 * Copyright (C) Jay Ridgeway
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <curl/curl.h>


typedef struct {
    ngx_array_t         *rl_lengths;
    ngx_array_t         *rl_values;
    ngx_str_t           zone;
    ngx_str_t           server;
    ngx_uint_t          interval;
    ngx_uint_t          threshold;
} ngx_http_ratelimit_conf_t;

struct MemoryStruct {
    char *memory;
    size_t size;
};

static void ngx_http_ratelimit_cleanup(void *data);
static void *ngx_http_ratelimit_create_conf(ngx_conf_t *cf);
static char *ngx_http_ratelimit_merge_conf(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_http_ratelimit(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_http_ratelimit_init(ngx_conf_t *cf);
static ngx_int_t ngx_http_ratelimit_init_process(ngx_cycle_t *cycle);
static void ngx_http_ratelimit_exit_process();
static char * ngx_http_ratelimit_zone(ngx_conf_t *cf, ngx_command_t *cmd, 
    void *conf);


static CURL *curl;
static char curlErrorBuf[CURL_ERROR_SIZE];

static ngx_command_t  ngx_http_ratelimit_commands[] = {

    { ngx_string("ratelimit_zone"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_ratelimit_zone,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },
    { ngx_string("ratelimit_server"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_ratelimit_conf_t, server),
      NULL },
    { ngx_string("ratelimit_interval"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_ratelimit_conf_t, interval),
      NULL },

    { ngx_string("ratelimit_threshold"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_ratelimit_conf_t, threshold),
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_ratelimit_module_ctx = {
    NULL,                                  /* preconfiguration */
    ngx_http_ratelimit_init,               /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_ratelimit_create_conf,        /* create location configration */
    ngx_http_ratelimit_merge_conf          /* merge location configration */
};


ngx_module_t  ngx_http_ratelimit_module = {
    NGX_MODULE_V1,
    &ngx_http_ratelimit_module_ctx,        /* module context */
    ngx_http_ratelimit_commands,           /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_http_ratelimit_init_process,       /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_http_ratelimit_exit_process,       /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static size_t
WriteMemoryCallback(void *ptr, size_t size, size_t nmemb, void *data)
{
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *)data;

    mem->memory = realloc(mem->memory, mem->size + realsize + 1);
    if (mem->memory) {
        memcpy(&(mem->memory[mem->size]), ptr, realsize);
        mem->size += realsize;
        mem->memory[mem->size] = 0;
    }
    return realsize;
}

static ngx_int_t
ngx_http_ratelimit_handler(ngx_http_request_t *r)
{
    ngx_http_ratelimit_conf_t     *rlcf;
    struct MemoryStruct           chunk;
    CURLcode                      res;
#define MAX_URL 1024*16
    u_char                        buf[MAX_URL];

//fprintf(stderr, "ngx_http_ratelimit_handler\n");

    rlcf = ngx_http_get_module_loc_conf(r, ngx_http_ratelimit_module);

    if (rlcf->rl_lengths == NULL || rlcf->rl_values == NULL) {
        return NGX_OK;
    }

    if (ngx_http_script_run(r, &rlcf->zone, rlcf->rl_lengths->elts,
                            0, rlcf->rl_values->elts) == NULL) {
        return NGX_ERROR;
    }

    if (rlcf->zone.len == 0 || rlcf->server.len == 0) {
        return NGX_OK;
    }

    buf[0] = '\0';
    ngx_snprintf(buf, MAX_URL, "%V?key=%V&duration=%d", &rlcf->server, 
                 &rlcf->zone, (int)rlcf->interval);

/*
    fprintf(stderr,"url %s zone %s server %s interval %d threshold %d\n",
                   buf, rlcf->zone.data, rlcf->server.data, (int)rlcf->interval, 
                   (int)rlcf->threshold);
*/

    chunk.memory=NULL;
    chunk.size = 0;

    curl_easy_setopt(curl, CURLOPT_URL, buf);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);

    res = curl_easy_perform(curl);
    if (res != 0) {
       fprintf(stderr, "curl(%d): %s\n", res, curlErrorBuf);
    } else {
        int cnt = atoi((char *)chunk.memory);
        if (cnt >= (int)rlcf->threshold) {
            if(chunk.memory) {
                free(chunk.memory);
            }
            return NGX_HTTP_SERVICE_UNAVAILABLE;
        }
    }

    if(chunk.memory) {
        free(chunk.memory);
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_ratelimit_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_PREACCESS_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_http_ratelimit_handler;

    return NGX_OK;
}


static void *
ngx_http_ratelimit_create_conf(ngx_conf_t *cf)
{
    ngx_http_ratelimit_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_ratelimit_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    conf->interval  = NGX_CONF_UNSET_UINT;
    conf->threshold = NGX_CONF_UNSET_UINT;

    return conf;
}


static char *
ngx_http_ratelimit_merge_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_ratelimit_conf_t *prev = parent;
    ngx_http_ratelimit_conf_t *conf = child;

    ngx_conf_merge_uint_value(conf->interval, prev->interval, 60);
    ngx_conf_merge_uint_value(conf->threshold, prev->threshold, 300);

    if (conf->zone.data == NULL && conf->rl_lengths == NULL) {
        conf->zone = prev->zone;
        conf->rl_lengths = prev->rl_lengths;
        conf->rl_values = prev->rl_values;
    }

    if (conf->interval < 1) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, 
            "interval must be equal or more than 1");
        return NGX_CONF_ERROR;
    }
    if (conf->threshold < 1) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, 
            "threshold must be equal or more than 1");
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}

static ngx_int_t 
ngx_http_ratelimit_init_process(ngx_cycle_t *cycle)
{
    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();

    if(curl) {
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);
        curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
        curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, &curlErrorBuf);
        return NGX_OK;
    }

    return NGX_ERROR;
}

static void 
ngx_http_ratelimit_exit_process()
{
    if (curl) {
        curl_easy_cleanup(curl);
        curl_global_cleanup();
    }
}

static char *
ngx_http_ratelimit_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                  *value;
    ngx_http_ratelimit_conf_t  *lcf = (ngx_http_ratelimit_conf_t *)conf;
    ngx_int_t                   n;
    ngx_http_script_compile_t   sc;

    value = cf->args->elts;

    n = ngx_http_script_variables_count(&value[1]);

    if (n == 0) {
        lcf->zone = value[1];
        return NGX_CONF_OK;
    }

    ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

    sc.cf = cf;
    sc.source = &value[1];
    sc.lengths = &lcf->rl_lengths;
    sc.values = &lcf->rl_values;
    sc.variables = n;
    sc.complete_lengths = 1;
    sc.complete_values = 1;

    if (ngx_http_script_compile(&sc) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}

