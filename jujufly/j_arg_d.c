#include "j_arg_d.h"

void j_arg_d_init(j_arg_d *jad)
{
    memset(jad, 0, sizeof(*jad));
    jad->argc = 0;
    jad->size = J_ARG_D_STATIC_SIZE;
    jad->argv = (char **)jad->static_space;
}

int j_arg_d_append(j_arg_d *jad, char *arg)
{
    int i;
    if (jad->argc == jad->size) {
        char **tmpv = (char **)calloc(jad->size * 2, sizeof(char *));
        for (i = 0; i < jad->argc; i++) {
            tmpv[i] = jad->argv[i];
        }
        if (jad->size != J_ARG_D_STATIC_SIZE) {
            free(jad->argv);
        }
        jad->argv = tmpv;
        jad->size *= 2;
    }
    jad->argv[jad->argc++] = arg;
    return jad->argc;
}

void j_arg_d_reset(j_arg_d *jad)
{
    jad->argc = 0;
}

void j_arg_d_print(FILE *out, j_arg_d *jad)
{
    int i;
    fprintf(out, "j_arg_d: %d items %d space\n", jad->argc, jad->size);
    for (i = 0; i < jad->argc; i++) {
        fprintf(out, "\t%d: %s\n", i, jad->argv[i]);
    }
}

void j_arg_d_free(j_arg_d *jad)
{
    if (jad->size != J_ARG_D_STATIC_SIZE) {
        free(jad->argv);
    }
}

