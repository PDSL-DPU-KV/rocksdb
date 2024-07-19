#pragma once
#ifndef _DPATHREADS_FLUSH_H_
#define _DPATHREADS_FLUSH_H_

#include <doca_buf.h>
#include <doca_buf_array.h>
#include <doca_buf_inventory.h>
#include <doca_ctx.h>
#include <doca_dev.h>
#include <doca_dma.h>
#include <doca_dpa.h>
#include <doca_mmap.h>
#include <doca_pe.h>
#include <doca_sync_event.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <chrono>

#include "dflush_common.h"
#include "common.h"

#include "util/work_queue.h"

#if defined(__x86_64__)
#include <immintrin.h>
static inline void relax() { _mm_pause(); }
#elif defined(__aarch64__)
static inline void relax() { asm volatile("yield" ::: "memory"); }
#else
static inline void relax() {}
#endif

static constexpr const uint32_t access_mask =
DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE;

enum class Location : uint32_t {
    Host = HOST,
    Device = DEVICE,
};

extern "C" doca_dpa_app * dflush_app;
extern "C" doca_dpa_func_t dflush_func;
extern "C" doca_dpa_func_t trigger;
extern "C" doca_dpa_func_t dpa_flush;

struct DPAThreads_Flush;

extern ROCKSDB_NAMESPACE::WorkQueue<DPAThreads_Flush*> DPAThreads_Flush_pool;


static std::pair<doca_sync_event*, doca_dpa_dev_sync_event_t> create_se(bool host2dev) {
    doca_sync_event* se;
    doca_dpa_dev_sync_event_t h;
    doca_check(doca_sync_event_create(&se));
    if (host2dev) {
        doca_check(doca_sync_event_add_publisher_location_cpu(se, dev));
        doca_check(doca_sync_event_add_subscriber_location_dpa(se, dpa));
    }
    else {
        doca_check(doca_sync_event_add_publisher_location_dpa(se, dpa));
        doca_check(doca_sync_event_add_subscriber_location_cpu(se, dev));
    }
    doca_check(doca_sync_event_start(se));
    doca_check(doca_sync_event_get_dpa_handle(se, dpa, &h));
    return { se, h };
}

static void destroy_se(doca_sync_event* se) {
    doca_check(doca_sync_event_stop(se));
    doca_check(doca_sync_event_destroy(se));
}

static std::pair<doca_mmap*, region_t> alloc_mem(Location l, uint64_t len) {
    region_t r = { 0,0,0 };
    r.flag = (uint32_t)l;
    doca_mmap* mmap;
    doca_check(doca_mmap_create(&mmap));
    switch (l) {
        case Location::Host: {
                r.ptr = (uintptr_t)aligned_alloc(64, len);
                // r.ptr = (uintptr_t)malloc(len);
                // memset((char *)r.ptr, 'A', len);
                if (dev == nullptr) {
                    printf("dev is nullptr\n");
                }
                doca_check(doca_mmap_add_dev(mmap, dev));
                doca_check(doca_mmap_set_memrange(mmap, (void*)r.ptr, len));
                doca_check(doca_mmap_set_permissions(mmap, access_mask));
            } break;
        case Location::Device: {
                doca_check(doca_dpa_mem_alloc(dpa, len, &r.ptr));
                doca_check(doca_mmap_set_dpa_memrange(mmap, dpa, r.ptr, len));
            } break;
    }
    doca_check(doca_mmap_start(mmap));
    doca_check(doca_mmap_dev_get_dpa_handle(mmap, dev, &r.handle));
    return { mmap, r };
}

static std::pair<doca_mmap*, region_t> alloc_mem_from_export(Location l, uint64_t len, std::string& export_desc) {
    region_t r;
    r.flag = (uint32_t)l;
    doca_mmap* mmap;
    size_t size;
    doca_check(doca_mmap_create_from_export(nullptr, export_desc.data(), export_desc.size(), dev, &mmap));
    doca_check(doca_mmap_get_memrange(mmap, (void**)&r.ptr, &size));
    doca_check(doca_mmap_start(mmap));
    doca_check(doca_mmap_dev_get_dpa_handle(mmap, dev, &r.handle));
    return { mmap, r };
}

static void free_mem(region_t& r, doca_mmap* mmap) {
    doca_check(doca_mmap_stop(mmap));
    switch ((Location)r.flag) {
        case Location::Host: {
                free((void*)r.ptr);
            } break;
        case Location::Device: {
                doca_check(doca_dpa_mem_free(dpa, (uintptr_t)r.ptr));
            } break;
    }
    doca_check(doca_mmap_destroy(mmap));
}

// 新 dpa 路径
struct DPAThread_Flush {
    DPAThread_Flush(doca_dpa_tg* tg, uintptr_t ctx_ptr, uint64_t rank)
        : ctx_ptr(ctx_ptr), rank(rank) {
        doca_check(doca_dpa_thread_create(dpa, &t));
        doca_check(doca_dpa_thread_set_func_arg(t, dpa_flush, (uintptr_t)ctx_ptr));
        doca_check(doca_dpa_thread_group_set_thread(tg, t, rank));
        doca_check(doca_dpa_thread_start(t));

        doca_check(doca_dpa_completion_create(dpa, 4096, &comp));
        doca_check(doca_dpa_completion_set_thread(comp, t));
        doca_check(doca_dpa_completion_start(comp));
        doca_check(doca_dpa_completion_get_dpa_handle(comp, &c.comp_handle));

        doca_check(doca_dpa_notification_completion_create(dpa, t, &notify));
        doca_check(doca_dpa_notification_completion_start(notify));
        doca_check(doca_dpa_notification_completion_get_dpa_handle(notify, &c.notify_handle));

        doca_check(doca_dpa_async_ops_create(dpa, 4096, 0, &aops));
        doca_check(doca_dpa_async_ops_attach(aops, comp));
        doca_check(doca_dpa_async_ops_start(aops));
        doca_check(doca_dpa_async_ops_get_dpa_handle(aops, &c.aops_handle));
    }

    void run(doca_dpa_dev_sync_event_t w_handle, doca_dpa_dev_sync_event_t s_handle,
             const params_memcpy_t& params, region_t sync) {
        c.w_handle = w_handle;
        c.s_handle = s_handle;
        c.params = params;
        c.sync = sync;
        c.call_counter = 0;
        c.notify_done = 0;
        // doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr, &c, sizeof(ctx_t)));
        doca_check(doca_dpa_thread_run(t));
    }

    void set_params(const params_memcpy_t& params, const uint64_t bufarr_handle, uintptr_t node_head, uintptr_t node_end) {
        printf("before c.bufarr_handle:%lx\n", c.bufarr_handle);
        c.params = params;
        c.bufarr_handle = bufarr_handle;
        printf("after c.bufarr_handle:%lx\n", c.bufarr_handle);
        printf("before c.node_head:%lx\n", c.node_head);
        c.node_head = host2dev(node_head);
        printf("after c.node_head:%lx\n", c.node_head);
        c.node_end = host2dev(node_end);
        doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr, &c, sizeof(ctx_t)));
        doca_check(doca_dpa_thread_run(t));
    }

    ~DPAThread_Flush() {
        doca_check(doca_dpa_notification_completion_stop(notify));
        doca_check(doca_dpa_async_ops_stop(aops));
        doca_check(doca_dpa_completion_stop(comp));
        doca_check(doca_dpa_thread_stop(t));

        doca_check(doca_dpa_notification_completion_destroy(notify));
        doca_check(doca_dpa_async_ops_destroy(aops));
        doca_check(doca_dpa_completion_destroy(comp));
        doca_check(doca_dpa_thread_destroy(t));
    }

    doca_dpa_dev_uintptr_t ctx_ptr;
    doca_dpa_thread* t;
    doca_dpa_completion* comp;
    doca_dpa_notification_completion* notify;
    doca_dpa_async_ops* aops;
    ctx_t c;
    uint64_t rank;
};


struct DPAThreads_Flush {
    DPAThreads_Flush(uint64_t n_threads, const params_memcpy_t& params) {
        // 分配通信工具
        std::tie(sync_map, sync_region) = alloc_mem(Location::Host, std::max(64ul, sizeof(uint64_t) * n_threads));
        // 给 dpa 分配参数空间
        doca_check(doca_dpa_mem_alloc(dpa, sizeof(ctx_t) * n_threads, &ctx_ptr));
        doca_check(doca_dpa_thread_group_create(dpa, n_threads, &tg));
        ts.reserve(n_threads);
        for (uint64_t i = 0;i < n_threads;++i) {
            ts.emplace_back(tg, ctx_ptr + i * sizeof(ctx_t), i);
        }
        // 申请同步事件
        doca_dpa_dev_sync_event_t w_handle;
        doca_dpa_dev_sync_event_t s_handle;
        doca_check(doca_dpa_thread_group_start(tg));
        std::tie(w, w_handle) = create_se(true);
        std::tie(s, s_handle) = create_se(true);
        for (uint64_t i = 0;i < n_threads;++i) {
            ts[i].run(w_handle, s_handle, params, sync_region);
        }
    }

    void trigger_all() {
        doca_check(doca_dpa_kernel_launch_update_set(dpa, nullptr, 0, nullptr, 0, 1, trigger, ctx_ptr, ts.size(), 0ull));
    }

    void wait_all() {
        auto sync = (std::atomic_uint64_t*)sync_region.ptr;
        while (true) {
            uint32_t cnt = 0;
            for (uint32_t i = 0; i < ts.size(); i++) {
                cnt += sync[i].load(std::memory_order_relaxed);
            }
            if (cnt == ts.size()) {
                break;
            }
            relax();
        }
        for (uint32_t i = 0; i < ts.size(); i++) {
            sync[i].store(0);
        }
    }

    void set_params(const params_memcpy_t& params, const uint64_t bufarr_handle, std::vector<uintptr_t>& node_heads) {
        uint64_t nthreads = ts.size();
        for (uint64_t i = 0;i < nthreads;++i) {
            printf("DPAThreads_Flush:bufarr_handle:%lx\n", bufarr_handle);
            ts[i].set_params(params, bufarr_handle, node_heads[i], node_heads[i + 1]);
        }
    }

    ~DPAThreads_Flush() {
        doca_check(doca_dpa_kernel_launch_update_set(dpa, nullptr, 0, nullptr, 0, 1, trigger, ctx_ptr, ts.size(), 1ull));
        destroy_se(w);
        destroy_se(s);
        doca_check(doca_dpa_thread_group_stop(tg));
        doca_check(doca_dpa_thread_group_destroy(tg));
        doca_check(doca_dpa_mem_free(dpa, ctx_ptr));
        free_mem(sync_region, sync_map);
    }

    doca_sync_event* w;
    doca_sync_event* s;
    doca_mmap* sync_map;
    region_t sync_region;
    uintptr_t ctx_ptr;
    doca_dpa_tg* tg;
    std::vector<DPAThread_Flush> ts;
};

#endif
