#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <simplehttp/queue.h>
#include <simplehttp/simplehttp.h>
#include <json/json.h>
#include "png.h"
#include "qrencode.h"

/*
 * libqrencode
 * http://megaui.net/fukuchi/works/qrencode/index.en.html
 *
 * os x installation
 * http://blog.loudhush.ro/2009/12/creating-qr-codes-on-mac-os-x-snow.html
 *
 * libpng
 * http://www.libpng.org/pub/png/libpng.html
 *
 */


static FILE *fp; // avoid clobbering by setjmp.
static int casesensitive = 1;
static int eightbit = 0;
static int version = 0;
static int size = 3;
static int margin = 4;
static int structured = 0;
static QRecLevel level = QR_ECLEVEL_L;
static QRencodeMode hint = QR_MODE_8;



static int 
writePNG(QRcode *qrcode)
{
    png_structp png_ptr;
    png_infop info_ptr;
    unsigned char *row, *p, *q;
    int x, y, xx, yy, bit;
    int realwidth;

    realwidth = (qrcode->width + margin * 2) * size;
    row = (unsigned char *)malloc((realwidth + 7) / 8);
    if(row == NULL) {
        fprintf(stderr, "Failed to allocate memory.\n");
        exit(EXIT_FAILURE);
    }

    png_ptr = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
    if(png_ptr == NULL) {
        fprintf(stderr, "Failed to initialize PNG writer.\n");
        exit(EXIT_FAILURE);
    }

    info_ptr = png_create_info_struct(png_ptr);
    if(info_ptr == NULL) {
        fprintf(stderr, "Failed to initialize PNG write.\n");
        exit(EXIT_FAILURE);
    }

    if(setjmp(png_jmpbuf(png_ptr))) {
        png_destroy_write_struct(&png_ptr, &info_ptr);
        fprintf(stderr, "Failed to write PNG image.\n");
        exit(EXIT_FAILURE);
    }

    fseek(fp, 0, SEEK_SET);
    ftruncate(fileno(fp), 0);
    png_init_io(png_ptr, fp);
    png_set_IHDR(png_ptr, info_ptr,
                realwidth, realwidth,
                1,
                PNG_COLOR_TYPE_GRAY,
                PNG_INTERLACE_NONE,
                PNG_COMPRESSION_TYPE_DEFAULT,
                PNG_FILTER_TYPE_DEFAULT);
    png_write_info(png_ptr, info_ptr);

    /* top margin */
    memset(row, 0xff, (realwidth + 7) / 8);
    for(y=0; y<margin * size; y++) {
        png_write_row(png_ptr, row);
    }

    /* data */
    p = qrcode->data;
    for(y=0; y<qrcode->width; y++) {
        bit = 7;
        memset(row, 0xff, (realwidth + 7) / 8);
        q = row;
        q += margin * size / 8;
        bit = 7 - (margin * size % 8);
        for(x=0; x<qrcode->width; x++) {
            for(xx=0; xx<size; xx++) {
                *q ^= (*p & 1) << bit;
                bit--;
                if(bit < 0) {
                    q++;
                    bit = 7;
                }
            }
            p++;
        }
        for(yy=0; yy<size; yy++) {
            png_write_row(png_ptr, row);
        }
    }
    /* bottom margin */
    memset(row, 0xff, (realwidth + 7) / 8);
    for(y=0; y<margin * size; y++) {
        png_write_row(png_ptr, row);
    }

    png_write_end(png_ptr, info_ptr);
    png_destroy_write_struct(&png_ptr, &info_ptr);

    free(row);
    fflush(fp);

    return 0;
}

static QRcode *
encode(const char *intext)
{
    QRcode *code;

    if(eightbit) {
        code = QRcode_encodeString8bit(intext, version, level);
    } else {
        code = QRcode_encodeString(intext, version, level, hint, casesensitive);
    }

    return code;
}

static int
qrencode(const char *intext)
{
    QRcode *qrcode;
    int ret;

    qrcode = encode(intext);
    if(qrcode == NULL) {
        perror("Failed to encode the input data:");
        exit(EXIT_FAILURE);
    }
    ret = writePNG(qrcode);
    QRcode_free(qrcode);
    return ret;
}

void
cb(struct evhttp_request *req, struct evbuffer *evb,void *ctx)
{
    int ret, fd;
    struct stat st;
    void *pngdata;
    const char *txt;
    struct evkeyvalq args;

    evhttp_parse_query(req->uri, &args);
    txt = evhttp_find_header(&args, "txt");

    ret = qrencode(txt);
    if (ret == 0) {
        fd = fileno(fp);
        fstat(fd, &st);
        pngdata = mmap(0, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
        evhttp_add_header(req->output_headers, "content-type", "image/png");
        evbuffer_add(evb, pngdata, st.st_size);
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
        munmap(pngdata, st.st_size);
    } else {
        evhttp_send_reply(req, HTTP_SERVUNAVAIL, "ERROR", evb);
    }
}

int
main(int argc, char **argv)
{

    char *outfile = "/tmp/qrencode.png";

    define_simplehttp_options();
    option_define_str("temp_file", OPT_OPTIONAL, "/tmp/qrencode.png", &outfile, NULL, NULL);
    if (!option_parse_command_line(argc, argv)){
        return 1;
    }

    fp = fopen(outfile, "a+");
    if(fp == NULL) {
        fprintf(stderr, "Failed to create file: %s\n", outfile);
        perror(NULL);
        exit(EXIT_FAILURE);
    }

    simplehttp_init();
    simplehttp_set_cb("/qr*", cb, NULL);
    simplehttp_main();
    free_options();
    
    fclose(fp);
    return 0;
}
