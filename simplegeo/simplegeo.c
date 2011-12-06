#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>

#define RECONNECT 5
#define MAXRES 1000

double geo_distance(double lat1, double lng1, double lat2, double long2);
void geo_box(double lat, double lng, double miles, double *ulat, double *ulng, double *llat, double *llng);
void finalize_json(struct evhttp_request *req, struct evbuffer *evb, struct evkeyvalq *args, struct json_object *jsobj);
int open_db(char *addr, int port, TCRDB **rdb);
void db_reconnect(int fd, short what, void *ctx);
void db_error_to_json(int code, struct json_object *jsobj);
void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void search_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void distance_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void box_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
static int CmpElem(const void *e1, const void *e2);

struct event ev;
struct timeval tv = {RECONNECT, 0};
char *db_host = "127.0.0.1";
int db_port = 1978;
TCRDB *rdb;
int db_status;
char *g_progname;

double pi;
double longDistance[181];
double latDistance = 69.169144;
double radius = 3958.75587;

typedef struct Geo_Result {
    int id;
    char *data;
    double latitude;
    double longitude;
    double distance;
} Geo_Result;

double geo_distance(double lat1, double lng1, double lat2, double lng2)
{
    double rlat, rlng, rlat2, rlng2;
    double miles;
    
    rlat = lat1 * pi / 180;
    rlng = lng1 * pi / 180;
    rlat2 = lat2 * pi / 180;
    rlng2 = lng2 * pi / 180;
    
    if (rlat == rlat2 && rlng == rlng2) {
        miles = 0;
    } else {
        // Spherical Law of Cosines
        miles = radius * acos(sin(rlat) * sin(rlat2) + cos(rlng - rlng2) * cos(rlat) * cos(rlat2));
    }
    
    return miles;
}

void geo_box(double lat, double lng, double miles, double *ulat, double *ulng, double *llat, double *llng)
{
    double latD, lngD, clat, clng;
    
    lngD = miles / longDistance[(int) fabs(lat)];
    latD = miles / latDistance;
    *llat = (lat + latD > 180 ? 180 - lat + latD : lat + latD);
    *llng = (lng + lngD > 180 ? 180 - lng + lngD : lng + lngD);
    *ulat = (lat - latD < -180 ? 180 + lat - latD : lat - latD);
    *ulng = (lng - lngD < -180 ? 180 + lng - lngD : lng - lngD);
}

void finalize_json(struct evhttp_request *req, struct evbuffer *evb,
                   struct evkeyvalq *args, struct json_object *jsobj)
{
    const char *json;
    char *jsonp;
    
    jsonp = (char *)evhttp_find_header(args, "jsonp");
    json = json_object_to_json_string(jsobj);
    if (jsonp) {
        evbuffer_add_printf(evb, "%s(%s)\n", jsonp, json);
    } else {
        evbuffer_add_printf(evb, "%s\n", json);
    }
    json_object_put(jsobj); // Odd free function
    
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(args);
}

int open_db(char *addr, int port, TCRDB **rdb)
{
    int ecode = 0;
    
    if (*rdb != NULL) {
        if (!tcrdbclose(*rdb)) {
            ecode = tcrdbecode(*rdb);
            fprintf(stderr, "close error: %s\n", tcrdberrmsg(ecode));
        }
        tcrdbdel(*rdb);
        *rdb = NULL;
    }
    *rdb = tcrdbnew();
    if (!tcrdbopen(*rdb, addr, port)) {
        ecode = tcrdbecode(*rdb);
        fprintf(stderr, "open error(%s:%d): %s\n", addr, port, tcrdberrmsg(ecode));
        *rdb = NULL;
    } else {
        char *status = tcrdbstat(*rdb);
        printf("adding indices\n---------------------\n");
        tcrdbtblsetindex(*rdb, "x", RDBITDECIMAL);
        tcrdbtblsetindex(*rdb, "y", RDBITDECIMAL);
        printf("%s---------------------\n", status);
        if (status) {
            free(status);
        }
    }
    return ecode;
}

void db_reconnect(int fd, short what, void *ctx)
{
    int i, s;
    
    s = db_status;
    if (s != TTESUCCESS && s != TTEINVALID && s != TTEKEEP && s != TTENOREC) {
        db_status = open_db(db_host, db_port, &rdb);
    }
    evtimer_del(&ev);
    evtimer_set(&ev, db_reconnect, NULL);
    evtimer_add(&ev, &tv);
}

void db_error_to_json(int code, struct json_object *jsobj)
{
    fprintf(stderr, "error(%d): %s\n", code, tcrdberrmsg(code));
    json_object_object_add(jsobj, "status", json_object_new_string("error"));
    json_object_object_add(jsobj, "code", json_object_new_int(code));
    json_object_object_add(jsobj, "message",
                           json_object_new_string((char *)tcrdberrmsg(code)));
}

void box_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    double lat, lng, miles, ulat, ulng, llat, llng;
    char *json;
    struct evkeyvalq args;
    struct json_object *jsobj;
    
    evhttp_parse_query(req->uri, &args);
    
    lat = get_double_argument(&args, "lat", 0);
    lng = get_double_argument(&args, "lng", 0);
    miles = get_double_argument(&args, "miles", 0);
    
    geo_box(lat, lng, miles, &ulat, &ulng, &llat, &llng);
    
    jsobj = json_object_new_object();
    json_object_object_add(jsobj, "ulat", json_object_new_double(ulat));
    json_object_object_add(jsobj, "ulng", json_object_new_double(ulng));
    json_object_object_add(jsobj, "llat", json_object_new_double(llat));
    json_object_object_add(jsobj, "llng", json_object_new_double(llng));
    
    finalize_json(req, evb, &args, jsobj);
}

void distance_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    double lat1, lng1, lat2, lng2;
    char *json;
    struct evkeyvalq args;
    struct json_object *jsobj;
    
    evhttp_parse_query(req->uri, &args);
    
    lat1 = get_double_argument(&args, "lat1", 0);
    lat1 = get_double_argument(&args, "lng1", 0);
    lat2 = get_double_argument(&args, "lat2", 0);
    lat2 = get_double_argument(&args, "lng2", 0);
    
    jsobj = json_object_new_object();
    json_object_object_add(jsobj, "distance", json_object_new_double(geo_distance(lat1, lng1, lat2, lng2)));
    
    finalize_json(req, evb, &args, jsobj);
}

void del_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *json, *id;
    struct evkeyvalq args;
    struct json_object *jsobj;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    evhttp_parse_query(req->uri, &args);
    
    id = (char *)evhttp_find_header(&args, "id");
    if (id == NULL) {
        evhttp_send_error(req, 400, "id is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    if (tcrdbtblout(rdb, id, sizeof(id))) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }
    
    finalize_json(req, evb, &args, jsobj);
}

void get_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    const char *value, *name;
    char *json, *hash, *key;
    struct evkeyvalq args;
    struct json_object *jsobj, *jsobj2, *jsobj3;
    TCMAP *cols;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    evhttp_parse_query(req->uri, &args);
    
    hash = (char *)evhttp_find_header(&args, "hash");
    key = (char *)evhttp_find_header(&args, "key");
    
    if (hash == NULL) {
        evhttp_send_error(req, 400, "hash is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    jsobj = json_object_new_object();
    jsobj2 = json_object_new_object();
    cols = tcrdbtblget(rdb, hash, sizeof(hash));
    
    if (cols) {
        tcmapiterinit(cols);
        jsobj3 = json_object_new_object();
        
        
        if (key) {
            value = tcmapget2(cols, key);
            
            if (!value) {
                value = "";
            }
            
            json_object_object_add(jsobj2, key, json_object_new_string(value));
        } else {
            while ((name = tcmapiternext2(cols)) != NULL) {
                json_object_object_add(jsobj2, name, json_object_new_string(tcmapget2(cols, name)));
            }
        }
        
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
        json_object_object_add(jsobj, "results", jsobj2);
        
        tcmapdel(cols);
    } else {
        json_object_object_add(jsobj, "status", json_object_new_string("error"));
    }
    
    finalize_json(req, evb, &args, jsobj);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *id, *data, *json, *key, *value;
    double lat, lng;
    int x, y;
    char buf[16];
    struct evkeyvalq args;
    struct json_object *jsobj;
    TCMAP *cols;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    evhttp_parse_query(req->uri, &args);
    
    lat = get_double_argument(&args, "lat", 0);
    lng = get_double_argument(&args, "lng", 0);
    id = (char *)evhttp_find_header(&args, "id");
    data = (char *)evhttp_find_header(&args, "data");
    
    if (id == NULL) {
        evhttp_send_error(req, 400, "id is required");
        evhttp_clear_headers(&args);
        return;
    }
    
    x = (lat * 10000) + 1800000;
    y = (lng * 10000) + 1800000;
    
    cols = tcmapnew();
    tcmapput2(cols, "data", data);
    sprintf(buf, "%d", x);
    tcmapput2(cols, "x", buf);
    sprintf(buf, "%d", y);
    tcmapput2(cols, "y", buf);
    sprintf(buf, "%f", lat);
    tcmapput2(cols, "lat", buf);
    sprintf(buf, "%f", lng);
    tcmapput2(cols, "lng", buf);
    
    jsobj = json_object_new_object();
    if (tcrdbtblput(rdb, id, strlen(id), cols)) {
        json_object_object_add(jsobj, "status", json_object_new_string("ok"));
    } else {
        db_status = tcrdbecode(rdb);
        db_error_to_json(db_status, jsobj);
    }
    
    tcmapdel(cols);
    
    finalize_json(req, evb, &args, jsobj);
}

void search_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    char *json;
    double lat, lng, distance, minlat, minlng, maxlat, maxlng, miles, lat2, lng2;
    int x1, x2, y1, y2, id, max;
    int total;
    struct evkeyvalq args;
    int ecode, pksiz, i, rsiz;
    char pkbuf[256];
    char minx[8];
    char miny[8];
    char maxx[8];
    char maxy[8];
    const char *rbuf, *name, *buf;
    RDBQRY *query;
    TCLIST *result;
    TCMAP *cols;
    Geo_Result *georesultPtr, **georesults;
    struct json_object *jsobj, *jsobj2, *jsarr;
    
    if (rdb == NULL) {
        evhttp_send_error(req, 503, "database not connected");
        return;
    }
    
    evhttp_parse_query(req->uri, &args);
    
    lat = get_int_argument(&args, "lat", 0);
    lng = get_int_argument(&args, "lng", 0);
    miles = get_int_argument(&args, "miles", 0);
    max = get_int_argument(&args, "max", 1);
    
    geo_box(lat, lng, miles, &minlat, &minlng, &maxlat, &maxlng);
    
    x1 = (minlat * 10000) + 1800000;
    y1 = (minlng * 10000) + 1800000;
    x2 = (maxlat * 10000) + 1800000;
    y2 = (maxlng * 10000) + 1800000;
    
    sprintf(minx, "%d", x1);
    sprintf(miny, "%d", y1);
    sprintf(maxx, "%d", x2);
    sprintf(maxy, "%d", y2);
    
    query = tcrdbqrynew(rdb);
    tcrdbqryaddcond(query, "x", RDBQCNUMGT, minx);
    tcrdbqryaddcond(query, "x", RDBQCNUMLT, maxx);
    tcrdbqryaddcond(query, "y", RDBQCNUMGT, miny);
    tcrdbqryaddcond(query, "y", RDBQCNUMLT, maxy);
    tcrdbqrysetorder(query, "x", RDBQONUMASC);
    
    cols = tcmapnew();
    result = tcrdbqrysearch(query);
    total = tclistnum(result);
    
    georesults = malloc(sizeof(Geo_Result *) * total);
    
    for (i = 0; i < total; i++) {
        rbuf = tclistval(result, i, &rsiz);
        cols = tcrdbtblget(rdb, rbuf, rsiz);
        
        if (cols) {
            georesultPtr = malloc(sizeof(*georesultPtr));
            
            tcmapiterinit(cols);
            buf = tcmapget2(cols, "lat");
            lat2 = atof(buf);
            georesultPtr->latitude = lat2;
            buf = tcmapget2(cols, "lng");
            lng2 = atof(buf);
            georesultPtr->longitude = lng2;
            id = atoi(rbuf);
            georesultPtr->id = id;
            georesultPtr->data = strdup(tcmapget2(cols, "data"));
            distance = geo_distance(lat, lng, lat2, lng2);
            georesultPtr->distance = distance;
            georesults[i] = georesultPtr;
            tcmapdel(cols);
        }
    }
    
    tclistdel(result);
    tcrdbqrydel(query);
    
    qsort(georesults, total, sizeof(Geo_Result *), CmpElem);
    
    jsobj = json_object_new_object();
    jsarr = json_object_new_array();
    
    for (i = 0; i < total; i++) {
        georesultPtr = georesults[i];
        
        if (i < max) {
            jsobj2 = json_object_new_object();
            json_object_object_add(jsobj2, "id", json_object_new_int(georesultPtr->id));
            json_object_object_add(jsobj2, "data", json_object_new_string(georesultPtr->data));
            json_object_object_add(jsobj2, "latitude", json_object_new_double(georesultPtr->latitude));
            json_object_object_add(jsobj2, "longitude", json_object_new_double(georesultPtr->longitude));
            json_object_object_add(jsobj2, "distance", json_object_new_double(georesultPtr->distance));
            json_object_array_add(jsarr, jsobj2);
        }
        free(georesultPtr->data);
        free(georesultPtr);
    }
    
    free(georesults);
    
    json_object_object_add(jsobj, "total", json_object_new_int(total));
    json_object_object_add(jsobj, "results", jsarr);
    
    finalize_json(req, evb, &args, jsobj);
}

int main(int argc, char **argv)
{
    int i, errCode;
    double magic, rlat, s, c;
    int lat;
    
    define_simplehttp_options();
    option_define_str("ttserver_host", OPT_OPTIONAL, "127.0.0.1", &db_host, NULL, NULL);
    option_define_int("ttserver_port", OPT_OPTIONAL, 1978, &db_port, NULL, NULL);
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    pi = atan(1.0) * 4;
    magic = cos(pi / 180.0);
    for (lat = 0; lat < 181; ++lat) {
        rlat = lat * pi / 180;
        s = sin(rlat);
        c = cos(rlat);
        longDistance[lat] = radius * acos((s * s) + (magic * c * c));
    }
    
    memset(&db_status, -1, sizeof(db_status));
    simplehttp_init();
    db_reconnect(0, 0, NULL);
    simplehttp_set_cb("/search*", search_cb, NULL);
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/get*", get_cb, NULL);
    simplehttp_set_cb("/del*", del_cb, NULL);
    simplehttp_set_cb("/distance*", distance_cb, NULL);
    simplehttp_set_cb("/box*", box_cb, NULL);
    simplehttp_main();
    
    if (!tcrdbclose(rdb)) {
        errCode = tcrdbecode(rdb);
        fprintf(stderr, "close error: %s\n", tcrdberrmsg(errCode));
    }
    tcrdbdel(rdb);
    free_options();
    
    return 0;
}

static int CmpElem(const void *e1, const void *e2)
{
    Geo_Result *s1 = *((Geo_Result **) e1);
    Geo_Result *s2 = *((Geo_Result **) e2);
    
    if (s1->distance > s2->distance) {
        return 1;
    } else if (s1->distance < s2->distance) {
        return -1;
    } else {
        return 0;
    }
}
