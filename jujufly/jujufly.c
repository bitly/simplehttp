#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <glob.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include "pcre.h"
#include <Judy.h>
#include "simplehttp/queue.h"
#include "simplehttp/simplehttp.h"
#include "j_arg_d.h"

#define NAME "jujufly"
#define VERSION "0.1"

#define OVECCOUNT 30    /* should be a multiple of 3 */
#define MAXVAL 1024*16
#define DB_SIZE 1024*1024*100
#define WRITEHEAD(x) (x->data+x->header->end)
#define WRITE(j,d,l) { memcpy(WRITEHEAD(j), d, l); \
                       j->header->end += l; }

#if defined __APPLE__ && defined __MACH__
#define GLOBC(g) g.gl_matchc
#else
#define GLOBC(g) g.gl_pathc
#endif

enum comparator {
    EQ = 0,
    NEQ,
    LT,
    GT,
    LTEQ,
    GTEQ,
    RE
};

typedef struct predicate {
    char *field;
    char *value;
    pcre *re;
    const char *error;
    int erroroffset;
    int fpos;
    int indexed;
    enum comparator comp;
    Word_t count;
} predicate;

typedef struct juju_record {
    uint32_t len;
    uint32_t when;
    char data[1];
} juju_record;

typedef struct juju_header {
    uint64_t nrecords;
    time_t youngest;
    time_t oldest;
    off_t end;            /* offset of next free block */
} juju_header;

typedef struct juju_db {
    int fd;
    char *filename;
    char *data;
    void *map_base;
    size_t map_size;
    size_t remaining_space;
    size_t index_size;
    Pvoid_t indices;
    juju_header *header;
    TAILQ_ENTRY(juju_db) entries;
} juju_db;

size_t db_size = DB_SIZE;
char *db_file = "db";
int ndatabases = 100;
time_t when_i_started;

int fieldc;
char **fieldv;
int *field_indexed;
Pvoid_t field_array = (PWord_t)NULL;

TAILQ_HEAD(db_list, juju_db) dbs;

/*
 * utils
 */

void timestr(time_t seconds, struct evbuffer *evb)
{
    if (seconds < 60) {
        evbuffer_add_printf(evb, "%lu seconds", (seconds < 0 ? 0 : seconds));
    } else if (seconds < 3600) {
        evbuffer_add_printf(evb, "%lu minutes %lu seconds", seconds/60, seconds%60);
    } else if (seconds < 86400) {
        evbuffer_add_printf(evb, "%lu hours %lu minutes %lu seconds", seconds/3600,
                            (seconds%3600)/60, seconds%60);
    } else {
        evbuffer_add_printf(evb, "%lu days %lu hours %lu minutes %lu seconds", seconds/86400,
                            (seconds%86400)/3600, (seconds%3600)/60, seconds%60);
    }
}

char **split_keys(char *keys, int *nkeys, int sep)
{
    char *c, **elems;
    int n, i = 0; 
    
    *nkeys = 0; 
    if (keys == NULL) return NULL;
    for (n=1, c = keys; *c != '\0'; c++) {
        if (*c == sep) n++; 
    }    
    elems = malloc(sizeof(char *)*n+1 + strlen(keys)+1);
    c = (char *) elems + sizeof(char *)*n+1;
    strcpy(c, keys);
    elems[i++] = c; 
    while (*c != '\0') {
        if (*c == sep) {
            *c = '\0';
            elems[i++] = c+1; 
        }    
        c++; 
    }    
    *nkeys = n; 
    return elems;
}

void rec_to_argv(juju_record *rec, j_arg_d *jargv)
{
    char *field;

    field = rec->data;
    while (field - (char *)rec < rec->len) {
        j_arg_d_append(jargv, field);
        field += strlen(field)+1;
    }
}

Pvoid_t get_pjlarray(juju_db *jjdb, char *field, char *value)
{
    Word_t *arr;

    JSLG(arr, jjdb->indices, (unsigned char *)field);
    if (!arr) return NULL;
    JSLG(arr, *(PPvoid_t)arr, (unsigned char *)value);
    if (!arr) return NULL;
    return arr;
}

void add_to_index(juju_db *jjdb, char *index, char *key, off_t pos, time_t when)
{
    Word_t *idx, *arr, *val;

    JSLI(idx, jjdb->indices, (unsigned char *)index);
    JSLI(arr, *(PPvoid_t)idx, (unsigned char *)key);
    if (!arr) {
        // new key
        jjdb->index_size += strlen(key);
    }
    JLI(val, *(PPvoid_t)arr, (Word_t)pos);
    jjdb->index_size += sizeof(Word_t)*2;
    *val = when;
}

bool append_record(juju_db *jjdb, char *line)
{
    char *field, *p;
    off_t *where;
    uint32_t len, recsz, when, fieldnum = 0;

    if (!jjdb) return false;
    when = atoi(line);  // first field in a record is time_t
    len = strlen(line);
    recsz = sizeof(uint32_t)*2+len+1;

    if (recsz > jjdb->remaining_space) {
        return false;
    }
    jjdb->remaining_space -= recsz;
    
    for (p = line; *p != '\0'; p++) {
        if (*p == '\t') *p = '\0';
    }

/*
    fprintf(stderr, "append_record #%llu btyes %d remaining %lu\n",
            jjdb->header->nrecords, len, jjdb->remaining_space);
*/
    where = (off_t *)jjdb->header->end;
    WRITE(jjdb, &recsz, sizeof(uint32_t));
    WRITE(jjdb, &when, sizeof(uint32_t));
    WRITE(jjdb, line, len+1);
    jjdb->header->nrecords += 1;

    if (!jjdb->header->youngest || when < jjdb->header->youngest) {
        jjdb->header->youngest = when;
    }
    if (!jjdb->header->oldest || when > jjdb->header->oldest) {
        jjdb->header->oldest = when;
    }

    field = line;
    while (field - line < len) {
        if (fieldnum < fieldc && field_indexed[fieldnum]) {
            //fprintf(stderr, "add to index: %d\n", fieldnum);
            add_to_index(jjdb, fieldv[fieldnum], field, (off_t)where, when);
        }
        field += strlen(field)+1;
        fieldnum++;
    }
    return true;
}

void print_indices(juju_db *jjdb)
{
    Word_t *val;
    uint8_t Index[1024];

    Index[0] = '\0';
    JSLF(val, jjdb->indices, Index);
    while (val)  {
        fprintf(stderr, "index %s\n", (char *)Index);
        JSLN(val, jjdb->indices, Index);
    }
}

void open_db(juju_db *jjdb)
{
    struct stat st;
    juju_record *rec;
    j_arg_d jargv;
    juju_header hdr;
    int i, j, processed=0;

    jjdb->fd = open(jjdb->filename, O_RDWR|O_CREAT, 0655);
    if (jjdb->fd < 0) {
        fprintf(stderr, "open(%s) failed: %s\n", jjdb->filename,
                strerror(errno));
        exit(1);
    }
    fstat(jjdb->fd, &st);
    if (st.st_size < sizeof(juju_header)) {
        memset(&hdr, 0, sizeof(juju_header));
        lseek(jjdb->fd, (off_t)0, SEEK_SET);
        write(jjdb->fd, &hdr, sizeof(hdr));
        lseek(jjdb->fd, (off_t)db_size, SEEK_SET);
        write(jjdb->fd, "", 1);
        fstat(jjdb->fd, &st);
    }
    jjdb->map_size = st.st_size;
    jjdb->map_base = mmap(NULL, (off_t)st.st_size, PROT_READ|PROT_WRITE,
                          MAP_SHARED, jjdb->fd, 0); 
    jjdb->header = (juju_header *)jjdb->map_base;
    jjdb->data = (char *)jjdb->map_base + sizeof(juju_header);
    jjdb->remaining_space = jjdb->map_size - sizeof(juju_header) - 
                            jjdb->header->end;
    if (jjdb->map_base == MAP_FAILED) {
        fprintf(stderr, "mmap() failed: %s\n", strerror(errno));
        exit(1);
    }   
    fprintf(stderr, "***********\n");
    fprintf(stderr, "%s\n\tnrecords: %llu\n\tdata size: %llu\n\t",
            jjdb->filename,
            jjdb->header->nrecords,
            jjdb->header->end);
    fprintf(stderr, "avg record: %G\n\tyoungest: %lu\n\toldest: %lu\n",
            (jjdb->header->end * 1.0 / jjdb->header->nrecords),
            jjdb->header->youngest,
            jjdb->header->oldest);

    j_arg_d_init(&jargv);
    rec = (juju_record *)jjdb->data;
    for (i=0; i < jjdb->header->nrecords; i++) {
        rec_to_argv(rec, &jargv);
        if (jargv.argc != fieldc && fieldc) {
            fprintf(stderr, "***fucked up record\n");
            j_arg_d_print(stderr, &jargv);
        }
        for (j=0; j < jargv.argc && j <  fieldc; j++) {
            if  (field_indexed[j]) {
                add_to_index(jjdb, fieldv[j], jargv.argv[j],
                             (char *)rec-jjdb->data, rec->when);
            }
        }
        j_arg_d_reset(&jargv);
        if (++processed % 100000 == 0) {
            fprintf(stderr, "%.2f%% complete\n",
                    i*100.0/jjdb->header->nrecords);
        }
        rec = (juju_record *)((char *)rec+rec->len);
    }
    fprintf(stderr, "100%% complete\n%s: processed %d records\n", jjdb->filename, processed);
    //print_indices(jjdb);
    j_arg_d_free(&jargv);
}

void close_db(juju_db *jjdb)
{
    Word_t *arr1, *arr2, rc;
    uint8_t field[1024], key[1024*4];
    
    TAILQ_REMOVE(&dbs, jjdb, entries);
    munmap(jjdb->map_base, jjdb->map_size);
    close(jjdb->fd);
    
    field[0] = '\0';
    JSLF(arr1, jjdb->indices, field);
    while (arr1) {
        // loop through each indexed field name 
        key[0] = '\0';
        JSLF(arr2, *(PPvoid_t)arr1, key);
        while (arr2) {
            // loop through each key in an index
            JLFA(rc, *(PPvoid_t)arr2);
            JSLN(arr2, *(PPvoid_t)arr1, key);
        }
        JSLFA(rc, *(PPvoid_t)arr1);
        JSLN(arr1, jjdb->indices, field);
    }
    JSLFA(rc, jjdb->indices);
    free(jjdb->filename);
    free(jjdb);
}

void roll_dbs()
{
    juju_db *jjdb, *deljjdb;
    char *ext, buf[1024];
    int i, numdbs;

    numdbs = 0;
    TAILQ_FOREACH(jjdb, &dbs, entries) {
        numdbs++;
    }
    jjdb = TAILQ_LAST(&dbs, db_list);
    while (jjdb) {
        if (numdbs-- >= ndatabases) {
            // delete and free
            deljjdb = jjdb;
            jjdb = TAILQ_PREV(jjdb, db_list, entries);
            TAILQ_REMOVE(&dbs, deljjdb, entries);
            close_db(deljjdb);
        } else {
            ext = strrchr(jjdb->filename, '.');
            if (ext && ++ext) {
                // i have no idea what to do if this doesnt work, let's exit
                errno = 0;
                i = (int)strtol(ext, NULL, 10);
                if (i == 0 && errno != 0) {
                    fprintf(stderr, "whoa nelly, bogus extension: %s: %s\n",
                            jjdb->filename, strerror(errno));
                    exit(1);                    
                } else {
                    sprintf(buf, "%s.%03d", db_file, i+1);
                    rename(jjdb->filename, buf);
                    free(jjdb->filename);
                    jjdb->filename = strdup(buf);
                }
            }
            jjdb = TAILQ_PREV(jjdb, db_list, entries);
        }
    }
}

void open_all_dbs()
{
    juju_db *jjdb;
    glob_t g;
    char buf[1024];
    int i;
    
    sprintf(buf, "%s.[0-9][0-9][0-9]", db_file);
    fprintf(stderr, "looking for: %s\n", buf);
    glob(buf, 0, NULL, &g);
    for (i=0; i < GLOBC(g) && i < ndatabases; i++) {
        fprintf(stderr, "path: %s\n", g.gl_pathv[i]);
        jjdb = calloc(1, sizeof(*jjdb));
        jjdb->filename = strdup(g.gl_pathv[i]);
        open_db(jjdb);
        TAILQ_INSERT_TAIL(&dbs, jjdb, entries);
    }
    globfree(&g);
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    size_t pos = 0;
    int nputs = 0;
    char *s;
    juju_db *jjdb;
    size_t len = EVBUFFER_LENGTH(req->input_buffer);
    char *data = (char *)EVBUFFER_DATA(req->input_buffer);

    jjdb = TAILQ_FIRST(&dbs);
    /*
     * Whacking a buffer we don't own. Living on the edge.
     */
    s = data;
    while (pos <= len) {
        if (data[pos] == '\n') {
            data[pos++] = '\0';
            while (append_record(jjdb, s) == false) {
                roll_dbs();
                jjdb = calloc(1, sizeof(*jjdb));
                asprintf(&jjdb->filename, "%s.000", db_file);
                open_db(jjdb);
                TAILQ_INSERT_HEAD(&dbs, jjdb, entries);
            }
            s = data+pos;
            nputs++;
        }
        pos++;
    }
    evbuffer_add_printf(evb, "nputs\t%d\n", nputs);
    evhttp_add_header(req->output_headers, "content-type", "text/plain");
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

int predicate_count_cmp(const void *va, const void *vb)
{
    const predicate *a = (predicate *) va;
    const predicate *b = (predicate *) vb;

    if (a->count > b->count) {
        return 1;
    } else if (a->count < b->count) {
        return -1;
    } else {
        return 0;
    }
}

Word_t count_by_predicate(predicate *pred)
{
    juju_db *jjdb;
    Word_t *arr, count;
    unsigned char buf[MAXVAL];
    
    TAILQ_FOREACH(jjdb, &dbs, entries) {
        count = 0;
        buf[0] = '\0';
        if (pred->comp == EQ) {
            arr = get_pjlarray(jjdb, pred->field, pred->value);
            if (arr) JLC(count, *(PPvoid_t)arr, 0, -1);
        }
        pred->count += count;
    }
    return pred->count;
}

int field_index(char *fieldname)
{
    Word_t *val;
       
    JSLG(val, field_array, (unsigned char *)fieldname);
    if (val) {
        return (int)*val;
    } else {
        return -1;
    }
}

predicate *build_predicates(struct evkeyvalq *pargs, int *npredicates)
{
    struct evkeyval *pair;
    predicate *pred, *predlist;
    char *re_tail;
    int i=0;

    *npredicates = 0;
    // find the number of clauses ...
    TAILQ_FOREACH(pair, pargs, next) {
        if (pair->key[0] != '_') (*npredicates)++;
    }
    if (*npredicates == 0) return (predicate *)NULL;

    // and their record sizes ...
    predlist = calloc(*npredicates, sizeof(*predlist));
    TAILQ_FOREACH(pair, pargs, next) {
        if (pair->key[0] == '_') continue;
        pred = &predlist[i];
        switch (pair->value[0]) {
            case '<':
                pred->comp = LT;
                pred->value = pair->value+1;
                break;
            case ',':
                pred->comp = LTEQ;
                pred->value = pair->value+1;
                break;
            case '>':
                pred->comp = GT;
                pred->value = pair->value+1;
                break;
            case '.':
                pred->comp = GTEQ;
                pred->value = pair->value+1;
                break;
            case '!':
                pred->comp = NEQ;
                pred->value = pair->value+1;
                break;
            case '/':
                pred->comp = RE;
                pred->value = pair->value+1;
                re_tail = strrchr(pred->value, '/');
                if (re_tail) {
                    *re_tail = '\0';
                    pred->re = pcre_compile(
                        pred->value,
                        0,
                        &pred->error,
                        &pred->erroroffset,
                        NULL);
                    break;
                }
            default:
                pred->comp = EQ;
                pred->value = pair->value;
        }
        pred->field = pair->key;
        pred->fpos = field_index(pred->field);
        pred->indexed = pred->fpos < 0 ? 0 : field_indexed[pred->fpos];
        count_by_predicate(pred);
        fprintf(stderr, "%s -> %s count %lu\n", pred->field, pred->value, pred->count);
        i++;
    }
    return predlist;
}

void search_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    juju_db *jjdb;
    j_arg_d jargv;
    struct evkeyvalq args;
    juju_record *rec;
    int i, cmp, matches, npredicates, limit=1000, result_count=0;
    predicate *pred1, *pred2, *predlist;
    Word_t *arr1, *arr2, *val1, *val2, pos;
    char *slimit, *sbefore, *ssince;
    int ovector[OVECCOUNT];
    time_t since = 0, before = time(NULL);
    
    j_arg_d_init(&jargv);    
    evhttp_parse_query(req->uri, &args);
    predlist = build_predicates(&args, &npredicates);
    printf("npredicates %d\n", npredicates);
    if (npredicates == 0) goto done;
    qsort(predlist, npredicates, sizeof(*predlist), predicate_count_cmp);    

    slimit = (char *)evhttp_find_header(&args, "_limit");
    sbefore = (char *)evhttp_find_header(&args, "_before");
    ssince = (char *)evhttp_find_header(&args, "_since");
    if (slimit) limit = strtol(slimit, NULL, 10);
    if (sbefore) before = strtol(sbefore, NULL, 10);
    if (ssince) since = strtol(ssince, NULL, 10);

    for (i=0, pred1=NULL; i < npredicates; i++) {
        if (predlist[i].comp == EQ
            && predlist[i].indexed
            && predlist[i].count > 0) {
            pred1 = &predlist[i];
            break;
        }
    }
    if (!pred1) goto done;  // must have at least one EQ

    TAILQ_FOREACH(jjdb, &dbs, entries) {
        arr1 = get_pjlarray(jjdb, pred1->field, pred1->value);
        if (!arr1) {
            printf("cant find %s == %s\n", pred1->field, pred1->value);
            continue;
        }
        if (jjdb->header->oldest < since
            || jjdb->header->youngest > before) {
            continue;
        }
        pos = -1;
        JLL(val1, *(PPvoid_t)arr1, pos);
        while (val1 && result_count < limit) {
            if (*val1 >= before || *val1 <= since) {
                JLP(val1, *(PPvoid_t)arr1, pos);
                continue;
            }
            for (i=0, matches=1; i < npredicates && matches >= i; i++) {
                pred2 = &predlist[i];
                if (pred1 == pred2) continue;
                
                if (pred2->indexed && (pred2->comp == EQ || pred2->comp == NEQ)) {
                    arr2 = get_pjlarray(jjdb, pred2->field, pred2->value);
                    if (pred2->comp == EQ) {
                        if (arr2) {
                            JLG(val2, *(PPvoid_t)arr2, pos);
                            if (val2) matches++;
                        }
                    } else {
                        if (arr2) {
                            JLG(val2, *(PPvoid_t)arr2, pos);
                            if (!val2) matches++;
                        } else {
                            matches++;
                        }
                    }
                } else if (pred2->fpos >= 0) {
                    rec = (juju_record *)(jjdb->data + pos);
                    rec_to_argv(rec, &jargv);
                    if (jargv.argc > pred2->fpos) {
                        if (pred2->comp == RE && pred2->re) {
                            cmp = pcre_exec(
                                pred2->re,
                                NULL,
                                jargv.argv[pred2->fpos],
                                strlen(jargv.argv[pred2->fpos]),
                                0,
                                0,
                                ovector,
                                OVECCOUNT);
                            if (cmp > 0) matches++;
                        } else {
                            cmp = strcmp(jargv.argv[pred2->fpos], pred2->value);
                            if ((pred2->comp == EQ && cmp == 0)
                                || (pred2->comp == NEQ && cmp != 0)
                                || (pred2->comp == LT && cmp < 0)
                                || (pred2->comp == LTEQ && cmp <= 0)
                                || (pred2->comp == GT && cmp > 0)
                                || (pred2->comp == GTEQ && cmp >= 0)) {
                                    matches++;
                            }
                        }
                    }
                    j_arg_d_reset(&jargv);
                }
            }
            
            //fprintf(stderr, "matches %d pred %d\n", matches, npredicates-1);
            if (matches == npredicates) {
                //fprintf(stderr, "pos %d when %d\n", pos, *val);
                result_count++;
                rec = (juju_record *)(jjdb->data + pos);
                rec_to_argv(rec, &jargv);
                for (i=0; i < jargv.argc; i++) {
                    evbuffer_add(evb, jargv.argv[i], strlen(jargv.argv[i]));
                    if (i != jargv.argc) {
                        evbuffer_add(evb, "\t", 1);
                    }
                }
                evbuffer_add(evb, "\n", 1);
                j_arg_d_reset(&jargv);
            }
            JLP(val1, *(PPvoid_t)arr1, pos);
        }
    }

done:
    evhttp_add_header(req->output_headers, "content-type", "text/plain");
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
    if (predlist) {
        for (i=0; i < npredicates; i++) {
            if (predlist[i].re) free(predlist[i].re);
        }
        free(predlist);
    }
    j_arg_d_free(&jargv);
}

void printidx_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    juju_db *jjdb;
    struct evkeyvalq args;
    char *field;
    unsigned char buf[1024*16];
    Word_t *arr, *val, *varr, count;

    evhttp_parse_query(req->uri, &args);
    field = (char *)evhttp_find_header(&args, "field");
    if (field) {
        TAILQ_FOREACH(jjdb, &dbs, entries) {
            evbuffer_add_printf(evb, "# %s\n", jjdb->filename);
            JSLG(arr, jjdb->indices, (unsigned char *)field);
            if (arr) {
                buf[0] = '\0';
                JSLF(val, *(PPvoid_t)arr, buf);
                while (val)  {
                    count = 0;
                    JSLG(varr, *(PPvoid_t)arr, buf);
                    if (varr) {
                        JLC(count, *(PPvoid_t)varr, 0, -1);
                    }
                    evbuffer_add_printf(evb, "%s\t%lu\n", buf, count);
                    JSLN(val, *(PPvoid_t)arr, buf);
                }
            }
        }
    }
    evhttp_add_header(req->output_headers, "content-type", "text/plain");
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
    evhttp_clear_headers(&args);
}

void dbstats_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    juju_db *jjdb;
    time_t min, max;

    jjdb = TAILQ_FIRST(&dbs);
    if (jjdb) max = jjdb->header->oldest;
    jjdb = TAILQ_LAST(&dbs, db_list);
    if (jjdb) min = jjdb->header->youngest;

    evbuffer_add_printf(evb, "file\tnrecords\tused\tremaining\tavgrecsz\t"
                        "idxsz\tidxpct\n");
    TAILQ_FOREACH(jjdb, &dbs, entries) {
        evbuffer_add_printf(evb, "%s\t%llu\t%llu\t%lu\t%G\t%lu\t%.2f\n",
                            jjdb->filename,
                            jjdb->header->nrecords,
                            jjdb->header->end,
                            jjdb->remaining_space,
                            (jjdb->header->end * 1.0 / jjdb->header->nrecords),
                            jjdb->index_size,
                            (jjdb->index_size * 100.0 / jjdb->header->end));
    }
    evhttp_add_header(req->output_headers, "content-type", "text/plain");
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

void stats_cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    juju_db *jjdb;
    time_t min = 0, max = 0, now = time(NULL);
    uint64_t nrec = 0, recsz = 0, idxsz = 0;

    jjdb = TAILQ_FIRST(&dbs);
    if (jjdb) max = jjdb->header->oldest;
    jjdb = TAILQ_LAST(&dbs, db_list);
    if (jjdb) min = jjdb->header->youngest;
    TAILQ_FOREACH(jjdb, &dbs, entries) {
        nrec += jjdb->header->nrecords;
        recsz += jjdb->header->end;
        idxsz += jjdb->index_size;
    }

    evbuffer_add_printf(evb, "uptime\t");
    timestr(now - when_i_started, evb);
    evbuffer_add_printf(evb, "\n");
    evbuffer_add_printf(evb, "db_file\t%s\n", db_file);
    evbuffer_add_printf(evb, "db_size\t%lu\n", db_size);
    evbuffer_add_printf(evb, "num_dbs\t%d\n", ndatabases);
    evbuffer_add_printf(evb, "records\t%llu\n", nrec);
    evbuffer_add_printf(evb, "recsz\t%llu\n", recsz);
    evbuffer_add_printf(evb, "idxsz\t%llu\n", idxsz);
    evbuffer_add_printf(evb, "youngest\t");
    timestr(now - max, evb);
    evbuffer_add_printf(evb, "\n");
    evbuffer_add_printf(evb, "oldest\t");
    timestr(now - min, evb);
    evbuffer_add_printf(evb, "\n");
    evbuffer_add_printf(evb, "span\t");
    timestr(max - min, evb);
    evbuffer_add_printf(evb, "\n");
    evbuffer_add_printf(evb, "est\t");
    timestr(((db_size * ndatabases)*1.0/recsz) * (max - min), evb);
    evbuffer_add_printf(evb, "\n");
    evhttp_add_header(req->output_headers, "content-type", "text/plain");
    evhttp_send_reply(req, HTTP_OK, "OK", evb);
}

int version_cb(int value) {
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    int i, j, indexc=0;
    Word_t *val;
    char **indexv;
    
    when_i_started = time(NULL);
    define_simplehttp_options();
    option_define_str("db_file", OPT_REQUIRED, "db", &db_file, NULL, "path of root db file (/tmp/db)");
    option_define_str("field_names", OPT_REQUIRED, NULL, NULL, NULL, "field1,field2,field3 (field names)");
    option_define_str("field_index", OPT_REQUIRED, NULL, NULL, NULL, "field2,field3 (index by field name)");
    option_define_int("db_size", OPT_OPTIONAL, db_size, (int *)&db_size, NULL, "size in bytes");
    option_define_int("num_dbs", OPT_OPTIONAL, ndatabases, &ndatabases, NULL, "number of databases");
    option_define_bool("version", OPT_OPTIONAL, 0, NULL, version_cb, VERSION);
    
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }
    
    fieldv = split_keys(option_get_str("field_names"), &fieldc, ',');
    indexv = split_keys(option_get_str("field_index"), &indexc, ',');

    field_indexed = calloc(fieldc+1, sizeof(int));
    for (i=0; i < indexc; i++) {
        for (j=0; j < fieldc; j++) {
            if (strcmp(indexv[i], fieldv[j]) == 0) {
                field_indexed[j] = 1;
            }
            JSLI(val, field_array, (unsigned char *)fieldv[j]);
            *val = j;
        }
    }
    
    TAILQ_INIT(&dbs);
    open_all_dbs();
    simplehttp_init();
    simplehttp_set_cb("/put*", put_cb, NULL);
    simplehttp_set_cb("/search*", search_cb, NULL);
    simplehttp_set_cb("/printidx*", printidx_cb, NULL);
    simplehttp_set_cb("/dbstats", dbstats_cb, NULL);
    simplehttp_set_cb("/stats", stats_cb, NULL);
    simplehttp_main();
    free_options();
    return 0;
}
