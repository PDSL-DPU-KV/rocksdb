#include <dpaintrin.h>

#include <doca_dpa_dev.h>
#include <doca_dpa_dev_buf.h>
#include <doca_dpa_dev_rdma.h>

#include "dflush_common.h"
#include "helper.h"


static void* get_base_ptr_of_region(region_t* r) {
    if (r->flag == HOST) {
        return (void*)doca_dpa_dev_mmap_get_external_ptr(r->handle, r->ptr);
    }
    else if (r->flag == DEVICE) {
        return (void*)r->ptr;
    }
    UNREACHABLE_CRIT;
    doca_dpa_dev_thread_finish();
    return 0;
}

__dpa_global__ static void trigger(uintptr_t ctx_ptr, uint64_t n_thread,
                                   uint64_t is_done) {
    ctx_t* t = (ctx_t*)ctx_ptr;
    for (uint64_t i = 0; i < n_thread; i++) {
        if (is_done) {
            t[i].notify_done = is_done;
        }
        doca_dpa_dev_thread_notify(t[i].notify_handle);
    }
}

static uint64_t run_async(ctx_t* t, uint64_t rank) {
    uintptr_t dst_ptr = t->params.dst.ptr + rank * t->params.piece_size;
    uintptr_t src_ptr = t->params.src.ptr + rank * t->params.piece_size;
    uint32_t counter = 0;
    doca_dpa_dev_completion_element_t e;

    TICK0;
    for (uint32_t i = 0; i < t->params.copy_n; i++) {
        // LOG_DBG("%d\n", i);
        doca_dpa_dev_post_memcpy(t->aops_handle, t->params.dst.handle, dst_ptr,
                                 t->params.src.handle, src_ptr, t->params.copy_size,
                                 1);
        dst_ptr += t->params.copy_size;
        src_ptr += t->params.copy_size;
        while (1) {
            int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
            if (got == 1) {
                doca_dpa_dev_completion_type_t et = doca_dpa_dev_get_completion_type(e);
                doca_dpa_dev_completion_ack(t->comp_handle, 1);
                counter++;
                if (et != 0) {
                    UNREACHABLE_CRIT;
                }
                // LOG_DBG("%d poll %d\n", et, counter);
            }
            else {
                break;
            }
        }
    }
    while (counter < t->params.copy_n) {
        int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
        if (got == 1) {
            doca_dpa_dev_completion_type_t et = doca_dpa_dev_get_completion_type(e);
            doca_dpa_dev_completion_ack(t->comp_handle, 1);
            counter++;
            if (et != 0) {
                UNREACHABLE_CRIT;
            }
            // LOG_DBG("%d poll %d\n", et, counter);
        }
    }
    TICK1;
    // PRINT_TICK(1);
    return t1 - t0;
}

__dpa_global__ static void dflush_func(uintptr_t ctx_ptr) {
    ctx_t* t = (ctx_t*)ctx_ptr;
    uint32_t rank = doca_dpa_dev_thread_rank();
    if (t->use_atomic) {
        uint64_t cycle = 0;
        cycle = run_async(t, rank);

        uint64_t* cycles = (uint64_t*)get_base_ptr_of_region(&t->result);
        cycles[rank] = cycle;
        __dpa_thread_window_writeback();

        uint64_t* sync = (uint64_t*)get_base_ptr_of_region(&t->sync);
        sync[rank] = 1;
        __dpa_thread_window_writeback();

        doca_dpa_dev_thread_reschedule();
    }

    doca_dpa_dev_completion_element_t e;
    int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
    if (got == 1) {
        doca_dpa_dev_completion_ack(t->comp_handle, 1);

        // LOG_DBG("%d %d %lx %lu %lu %lu %d %d %lx %d %d %lx %lx %lx\n", rank, et,
        //         (uintptr_t)t, t->params.region_size, t->params.piece_size,
        //         t->params.copy_size, t->params.dst.handle, t->params.dst.flag,
        //         t->params.dst.ptr, t->params.src.handle, t->params.src.flag,
        //         t->params.src.ptr, t->w_handle, t->s_handle);

        uint64_t cycle = 0;
        cycle = run_async(t, rank);

        // LOG_TRACE("write back\n");

        uint64_t* cycles = (uint64_t*)get_base_ptr_of_region(&t->result);
        cycles[rank] = cycle;
        __dpa_thread_window_writeback();

        doca_dpa_dev_sync_event_update_add(t->w_handle, 1);
        doca_dpa_dev_completion_request_notification(t->comp_handle);
    }

    if (t->notify_done == 0) {
        // LOG_DBG("post wait gt %lu\n", t->call_counter);
        doca_dpa_dev_sync_event_post_wait_gt(t->aops_handle, t->s_handle,
                                             t->call_counter);
        ++t->call_counter;
        // LOG_TRACE("reschedule\n");
        doca_dpa_dev_thread_reschedule();
    }
    else {
        // LOG_TRACE("finish\n");
        doca_dpa_dev_thread_finish();
    }
}
