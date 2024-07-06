#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HOST 1
#define DEVICE 2

#define SYNC 1
#define ASYNC 2
#define DMA 3

    typedef struct {
        uintptr_t ptr;
        uint32_t flag;
        uint32_t handle;
    } region_t;

    typedef struct {
        region_t dst;
        region_t src;
        uint64_t piece_size;
        uint64_t region_size;
        uint64_t copy_size;
        uint64_t copy_n;
        uint64_t memcpy_mode;
    } params_memcpy_t;

    typedef struct {
        uint64_t comp_handle;
        uint64_t aops_handle;
        uint64_t notify_handle;
        uint64_t w_handle;
        uint64_t s_handle;
        uint64_t call_counter;
        uint64_t notify_done;
        uint64_t use_atomic;
        region_t result;
        region_t sync;
        params_memcpy_t params;
    } ctx_t;

#ifdef __cplusplus
}
#endif
