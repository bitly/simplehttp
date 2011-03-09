#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <inttypes.h>

int int_cmp(const void *a, const void *b) 
{
    const uint64_t *ia = (const uint64_t *)a;
    const uint64_t *ib = (const uint64_t *)b;
    
    return *ia  - *ib;
}

uint64_t ninety_five_percent(int64_t *int_array, int length)
{
    uint64_t value;
    int64_t *sorted_requests;
    int index_of_95;
    
    sorted_requests = calloc(length, sizeof(int64_t));
    memcpy(sorted_requests, int_array, length);
    qsort(sorted_requests, length, sizeof(int64_t), int_cmp);
    index_of_95 = (int)ceil(((95.0 / 100.0) * length) + 0.5);
    value = sorted_requests[index_of_95];
    free(sorted_requests);
    
    return value;
}
