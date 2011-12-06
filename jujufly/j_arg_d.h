/*
 * j_arg_d
 *
 * Dynamic stack oriented array routines.
 *
 * Uses a static space to store arguments on the heap. Past J_ARG_D_STATIC_SIZE
 * this switches to a power of two allocator using malloc. Redefine
 * J_ARG_D_STATIC_SIZE to change the static space for your application.
 *
 */

#ifndef __J_ARG_D_H__
#define  __J_ARG_D_H__

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#ifndef J_ARG_D_STATIC_SIZE
#define J_ARG_D_STATIC_SIZE 32 // set to power of 2 bitches
#endif /* J_ARG_D_STATIC_SIZE */

typedef struct j_arg_d {
    int  argc;
    int  size;
    char **argv;
    char *static_space[J_ARG_D_STATIC_SIZE];
} j_arg_d;

void j_arg_d_init(j_arg_d *jad);
int  j_arg_d_append(j_arg_d *jad, char *arg);
void j_arg_d_reset(j_arg_d *jad);
void j_arg_d_print(FILE *out, j_arg_d *jad);
void j_arg_d_free(j_arg_d *jad);

#endif /* __J_ARG_D_H__ */

