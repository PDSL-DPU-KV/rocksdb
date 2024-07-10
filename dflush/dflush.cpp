#include "common.h"
#include "dflush_common.h"
#include "timer.h"
#include "threadpool.h"
#include "../util/coding.h"

#include <atomic>
#include <string>
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>

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
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <chrono>

#include "../util/coding.h"
#include "../util/work_queue.h"
#include "common.h"
#include "db/builder.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/memtable.h"
#include "db/seqno_to_time_mapping.h"
#include "db/version_edit.h"
#include "dflush_common.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
#include "stdio.h"
#include "table/merging_iterator.h"
#include "threadpool.h"
#include "timer.h"
#include "mutex"

#if defined(__x86_64__)
#include <immintrin.h>
static inline void relax() { _mm_pause(); }
#elif defined(__aarch64__)
static inline void relax() { asm volatile("yield" ::: "memory"); }
#else
static inline void relax() {}
#endif

using namespace std::chrono_literals;

extern "C" doca_dpa_app * dflush_app;
extern "C" doca_dpa_func_t dflush_func;
extern "C" doca_dpa_func_t trigger;

constexpr const uint32_t access_mask =
DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE;

const uint32_t nthreads_task = 32;
const uint32_t nthreads_memcpy = 4;
const bool DPA_MEMCPY = 1;

enum class Location : uint32_t {
    Host = HOST,
    Device = DEVICE,
};

struct DbPath_struct {
    char path[128];
    uint64_t target_size;
};

struct DMAThread;
struct DPAThreads;
ROCKSDB_NAMESPACE::WorkQueue<DMAThread*> DMAThread_pool;
ROCKSDB_NAMESPACE::WorkQueue<DPAThreads*> DPAThreads_pool;

int send_meta(rocksdb::FileMetaData* meta, char* ptr) {
    int send_size = 0;

    std::string send_small_str = (meta->smallest).get_InternalKey();
    *(uint32_t*)ptr = (uint32_t)(send_small_str.size());
    ptr += sizeof(uint32_t);
    send_size += sizeof(uint32_t);
    if ((uint32_t)(send_small_str).size() > 0) {
        memcpy(ptr, (send_small_str).c_str(), (uint32_t)(send_small_str).size());
        ptr += (send_small_str).size();
        send_size += (send_small_str).size();
    }
    printf("meta.smallest.DebugString:%s\n",
           meta->smallest.DebugString(true).c_str());

    std::string send_large_str = (meta->largest).get_InternalKey();
    *(uint32_t*)ptr = (uint32_t)(send_large_str).size();
    ptr += sizeof(uint32_t);
    send_size += sizeof(uint32_t);
    if ((uint32_t)(send_large_str).size() > 0) {
        memcpy(ptr, (send_large_str).c_str(), (uint32_t)(send_large_str).size());
        ptr += (send_large_str).size();
        send_size += (send_large_str).size();
    }
    printf("meta.largest.DebugString:%s\n",
           meta->largest.DebugString(true).c_str());

    printf("SEND: total_size after str: %d\n", send_size);
    *(uint64_t*)ptr = meta->compensated_file_size;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->num_entries;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->num_deletions;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->raw_key_size;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->raw_value_size;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->num_range_deletions;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->compensated_range_deletion_size;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(int*)ptr = meta->refs;
    ptr += sizeof(int);
    send_size += sizeof(int);

    *(bool*)ptr = meta->being_compacted;
    ptr += sizeof(bool);
    send_size += sizeof(bool);

    *(bool*)ptr = meta->init_stats_from_file;
    ptr += sizeof(bool);
    send_size += sizeof(bool);

    *(bool*)ptr = meta->marked_for_compaction;
    ptr += sizeof(bool);
    send_size += sizeof(bool);

    *(uint64_t*)ptr = meta->oldest_blob_file_number;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->oldest_ancester_time;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->file_creation_time;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint64_t*)ptr = meta->epoch_number;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    *(uint32_t*)ptr = (uint32_t)(meta->file_checksum).size();
    ptr += sizeof(uint32_t);
    send_size += sizeof(uint32_t);
    strncpy(ptr, (meta->file_checksum).c_str(), (uint32_t)(meta->file_checksum).size());
    ptr += (meta->file_checksum).size();
    send_size += (meta->file_checksum).size();

    *(uint32_t*)ptr = (uint32_t)(meta->file_checksum_func_name).size();
    ptr += sizeof(uint32_t);
    send_size += sizeof(uint32_t);
    strncpy(ptr, (meta->file_checksum_func_name).c_str(), (uint32_t)(meta->file_checksum_func_name).size());
    ptr += (meta->file_checksum_func_name).size();
    send_size += (meta->file_checksum_func_name).size();

    *(uint64_t*)ptr = meta->tail_size;
    ptr += sizeof(uint64_t);
    send_size += sizeof(uint64_t);

    return send_size;
}

int recv_meta(rocksdb::FileMetaData* meta, char* ptr) {
    int recv_size = 0;

    uint32_t n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    recv_size += sizeof(uint32_t);
    if (n > 0) {
        std::string str(ptr, n);
        str.push_back('\0');
        meta->smallest.set_InternalKey(str);
        ptr += n;
        recv_size += n;
    }
    else {
        meta->smallest.set_InternalKey(std::string());
    }

    n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    recv_size += sizeof(uint32_t);
    if (n > 0) {
        std::string str(ptr, n);
        str.push_back('\0');
        meta->largest.set_InternalKey(str);
        ptr += n;
        recv_size += n;
    }
    else {
        meta->largest.set_InternalKey(std::string());
    }

    meta->compensated_file_size = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->num_entries = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->num_deletions = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->raw_key_size = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->raw_value_size = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->num_range_deletions = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->compensated_range_deletion_size = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->refs = *(int*)ptr;
    ptr += sizeof(int);
    recv_size += sizeof(int);

    meta->being_compacted = *(bool*)ptr;
    ptr += sizeof(bool);
    recv_size += sizeof(bool);

    meta->init_stats_from_file = *(bool*)ptr;
    ptr += sizeof(bool);
    recv_size += sizeof(bool);

    meta->marked_for_compaction = *(bool*)ptr;
    ptr += sizeof(bool);
    recv_size += sizeof(bool);

    meta->oldest_blob_file_number = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->oldest_ancester_time = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->file_creation_time = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    meta->epoch_number = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);


    n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    recv_size += sizeof(uint32_t);
    if (n > 0) {
        std::string str(ptr, n);
        str.push_back('\0');
        meta->file_checksum = str;
        ptr += n;
        recv_size += n;
    }
    else {
        meta->file_checksum = std::string();
    }

    n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    recv_size += sizeof(uint32_t);
    if (n > 0) {
        std::string str(ptr, n);
        str.push_back('\0');
        meta->file_checksum_func_name = str;
        ptr += n;
        recv_size += n;
    }
    else {
        meta->file_checksum_func_name = std::string();
    }

    meta->tail_size = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    recv_size += sizeof(uint64_t);

    return recv_size;
}
typedef doca_error_t(*tasks_check)(struct doca_devinfo*);

doca_error_t dma_task_is_supported(struct doca_devinfo* devinfo) {
    return doca_dma_cap_task_memcpy_is_supported(devinfo);
}

void open_device(tasks_check func) {
    doca_devinfo** dev_list;
    doca_devinfo* dev_info = NULL;
    uint32_t nb_devs;
    doca_check(doca_devinfo_create_list(&dev_list, &nb_devs));
    char ibdev_name_buf[DOCA_DEVINFO_IBDEV_NAME_SIZE];
    doca_error_t result = DOCA_SUCCESS;
    for (uint32_t i = 0; i < nb_devs; i++) {
        result = doca_devinfo_get_ibdev_name(dev_list[i], ibdev_name_buf,
                                             DOCA_DEVINFO_IBDEV_NAME_SIZE);
        if (result == DOCA_SUCCESS &&
            strncmp(ibdev_name, ibdev_name_buf, strlen(ibdev_name)) == 0) {
            if (func != NULL && func(dev_list[i]) != DOCA_SUCCESS)
                continue;
            dev_info = dev_list[i];
            break;
        }
    }
    if (!dev_info) {
        std::abort();
    }
    doca_check(doca_dev_open(dev_info, &dev));
    doca_check(doca_devinfo_destroy_list(dev_list));
}

void close_device() { doca_check(doca_dev_close(dev)); }

std::pair<doca_mmap*, region_t> alloc_mem(Location l, uint64_t len) {
    region_t r = { 0,0,0 };
    r.flag = (uint32_t)l;
    doca_mmap* mmap;
    doca_check(doca_mmap_create(&mmap));
    switch (l) {
        case Location::Host: {
                r.ptr = (uintptr_t)aligned_alloc(64, len);
                // r.ptr = (uintptr_t)malloc(len);
                // memset((char *)r.ptr, 'A', len);
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

std::pair<doca_mmap*, region_t> alloc_mem_from_export(Location l, uint64_t len, std::string& export_desc) {
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

void free_mem(region_t& r, doca_mmap* mmap) {
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

static void dma_memcpy_completed_callback(struct doca_dma_task_memcpy* dma_task,
                                          union doca_data task_user_data,
                                          union doca_data ctx_user_data) {
    struct dma_resources* resources = (struct dma_resources*)ctx_user_data.ptr;
    struct doca_task* task = doca_dma_task_memcpy_as_task(dma_task);
    doca_check(doca_task_get_status(task));

    /* Free task */
    doca_task_free(doca_dma_task_memcpy_as_task(dma_task));
}

static void dma_memcpy_error_callback(struct doca_dma_task_memcpy* dma_task,
                                      union doca_data task_user_data,
                                      union doca_data ctx_user_data) {
    printf("dma error!\n");
    struct dma_resources* resources = (struct dma_resources*)ctx_user_data.ptr;
    struct doca_task* task = doca_dma_task_memcpy_as_task(dma_task);
    doca_check(doca_task_get_status(task));

    /* Free task */
    doca_task_free(task);
}

static void dma_state_changed_callback(const union doca_data user_data,
                                       struct doca_ctx* ctx,
                                       enum doca_ctx_states prev_state,
                                       enum doca_ctx_states next_state) {
    (void)ctx;
    (void)prev_state;

    switch (next_state) {
        case DOCA_CTX_STATE_IDLE:
            printf("DMA context has been stopped\n");
            break;
        case DOCA_CTX_STATE_STARTING:
            printf("DMA context entered into starting state. Unexpected transition\n");
            break;
        case DOCA_CTX_STATE_RUNNING:
            printf("DMA context is running\n");
            break;
        case DOCA_CTX_STATE_STOPPING:
            printf("DMA context entered into stopping state. Any inflight tasks "
                   "will be flushed\n");
            break;
        default:
            break;
    }
}

static uint64_t max_dma_buffer_size() {
    uint64_t max_buffer_size = 0;
    doca_dma_cap_task_memcpy_get_max_buf_size(doca_dev_as_devinfo(dev),
                                              &max_buffer_size);
    return max_buffer_size;
}

struct DMAThread {
    DMAThread(uint32_t max_bufs, uint32_t num_dma_tasks) {
        doca_check(doca_buf_inventory_create(max_bufs, &buf_inv));
        doca_check(doca_buf_inventory_start(buf_inv));
        doca_check(doca_pe_create(&pe));
        doca_check(doca_dma_create(dev, &dma_ctx));
        ctx = doca_dma_as_ctx(dma_ctx);
        doca_check(doca_ctx_set_state_changed_cb(ctx, dma_state_changed_callback));
        doca_check(doca_dma_task_memcpy_set_conf(
            dma_ctx, dma_memcpy_completed_callback, dma_memcpy_error_callback,
            num_dma_tasks));
        doca_check(doca_pe_connect_ctx(pe, ctx));
        doca_check(doca_ctx_start(ctx));
    }

    void run(params_memcpy_t params, doca_mmap* dst_m, doca_mmap* src_m) {
        struct doca_dma_task_memcpy* dma_task = nullptr;
        union doca_data task_user_data = { 0 };
        struct doca_task* task = nullptr;
        std::vector<doca_buf*> dst_bufs;
        std::vector<doca_buf*> src_bufs;

        for (uint64_t i = 0; i < params.copy_n; i++) {
            doca_buf* dst_buf = nullptr;
            doca_buf* src_buf = nullptr;
            auto dst_addr = (char*)params.dst.ptr + i * params.copy_size;
            auto src_addr = (char*)params.src.ptr + i * params.copy_size;
            //   printf("copy %d, size: %ld, dst: %lx, src: %lx\n", i,
            //   params.copy_size,
            //          dst_addr, src_addr);
            doca_check(doca_buf_inventory_buf_get_by_addr(
                buf_inv, dst_m, (void*)dst_addr, params.copy_size, &dst_buf));
            doca_check(doca_buf_inventory_buf_get_by_addr(
                buf_inv, src_m, (void*)src_addr, params.copy_size, &src_buf));
            doca_check(
                doca_buf_set_data(src_buf, (void*)src_addr, params.copy_size));
            dst_bufs.emplace_back(dst_buf);
            src_bufs.emplace_back(src_buf);
            doca_check(doca_dma_task_memcpy_alloc_init(dma_ctx, src_buf, dst_buf,
                task_user_data, &dma_task));
            task = doca_dma_task_memcpy_as_task(dma_task);
            doca_check(doca_task_submit(task));
            //   poll until dma task finished
            while (!doca_pe_progress(pe))
                ;
        }

        for (int i = 0; i < dst_bufs.size(); i++) {
            doca_buf_dec_refcount(dst_bufs[i], NULL);
            doca_buf_dec_refcount(src_bufs[i], NULL);
        }
    }

    ~DMAThread() {
        doca_check(doca_ctx_stop(ctx));
        doca_check(doca_dma_destroy(dma_ctx));
        doca_check(doca_pe_destroy(pe));
        doca_check(doca_buf_inventory_destroy(buf_inv));
    }

    struct doca_buf_inventory* buf_inv;
    struct doca_pe* pe;
    struct doca_dma* dma_ctx;
    struct doca_ctx* ctx;
};

std::pair<doca_sync_event*, doca_dpa_dev_sync_event_t> create_se(bool host2dev) {
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

void destroy_se(doca_sync_event* se) {
    doca_check(doca_sync_event_stop(se));
    doca_check(doca_sync_event_destroy(se));
}

struct DPAThread {
    DPAThread(doca_dpa_tg* tg, uintptr_t ctx_ptr, uint64_t rank)
        : ctx_ptr(ctx_ptr), rank(rank) {
        doca_check(doca_dpa_thread_create(dpa, &t));
        doca_check(
            doca_dpa_thread_set_func_arg(t, dflush_func, (uintptr_t)ctx_ptr));
        doca_check(doca_dpa_thread_group_set_thread(tg, t, rank));
        doca_check(doca_dpa_thread_start(t));

        doca_check(doca_dpa_completion_create(dpa, 4096, &comp));
        doca_check(doca_dpa_completion_set_thread(comp, t));
        doca_check(doca_dpa_completion_start(comp));
        doca_check(doca_dpa_completion_get_dpa_handle(comp, &c.comp_handle));

        doca_check(doca_dpa_notification_completion_create(dpa, t, &notify));
        doca_check(doca_dpa_notification_completion_start(notify));
        doca_check(doca_dpa_notification_completion_get_dpa_handle(
            notify, &c.notify_handle));

        doca_check(doca_dpa_async_ops_create(dpa, 4096, 0, &aops));
        doca_check(doca_dpa_async_ops_attach(aops, comp));
        doca_check(doca_dpa_async_ops_start(aops));
        doca_check(doca_dpa_async_ops_get_dpa_handle(aops, &c.aops_handle));
    }

    void run(doca_dpa_dev_sync_event_t w_handle,
             doca_dpa_dev_sync_event_t s_handle, const params_memcpy_t& params,
             region_t result, region_t sync, bool use_atomic) {
        c.w_handle = w_handle;
        c.s_handle = s_handle;
        c.params = params;
        c.result = result;
        c.sync = sync;
        c.call_counter = 0;
        c.notify_done = 0;
        c.use_atomic = use_atomic;
        doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr, &c, sizeof(ctx_t)));
        doca_check(doca_dpa_thread_run(t));
    }

    void set_params(const params_memcpy_t& params) {
        c.params = params;
        // doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr + sizeof(ctx_t) - sizeof(params_memcpy_t),
        //     &c.params, sizeof(params_memcpy_t)));
        doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr, &c, sizeof(ctx_t)));
    }

    ~DPAThread() {
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



struct DPAThreads {
    DPAThreads(uint64_t n_threads, const params_memcpy_t& params, bool use_atomic)
        : use_atomic(use_atomic) {
        std::tie(cycles_map, cycles_region) =
            alloc_mem(Location::Host, std::max(64ul, sizeof(uint64_t) * n_threads));
        std::tie(sync_map, sync_region) =
            alloc_mem(Location::Host, std::max(64ul, sizeof(uint64_t) * n_threads));
        doca_check(doca_dpa_mem_alloc(dpa, sizeof(ctx_t) * n_threads, &ctx_ptr));
        doca_check(doca_dpa_thread_group_create(dpa, n_threads, &tg));
        ts.reserve(n_threads);
        for (uint64_t i = 0; i < n_threads; i++) {
            ts.emplace_back(tg, ctx_ptr + i * sizeof(ctx_t), i);
        }
        doca_dpa_dev_sync_event_t w_handle;
        doca_dpa_dev_sync_event_t s_handle;
        doca_check(doca_dpa_thread_group_start(tg));
        std::tie(w, w_handle) = create_se(true);
        std::tie(s, s_handle) = create_se(true);
        for (uint64_t i = 0; i < n_threads; i++) {
            ts[i].run(w_handle, s_handle, params, cycles_region, sync_region,
                      use_atomic);
        }
        if (!use_atomic) {
            doca_check(doca_dpa_kernel_launch_update_set(
                dpa, nullptr, 0, nullptr, 0, 1, trigger, ctx_ptr, ts.size(), 0ull));
        }
    }

    void trigger_all() {
        if (use_atomic) {
            doca_check(doca_dpa_kernel_launch_update_set(
                dpa, nullptr, 0, nullptr, 0, 1, trigger, ctx_ptr, ts.size(), 0ull));
        }
        else {
            doca_check(doca_sync_event_update_set(s, ++call_counter));
        }
    }

    void wait_all() {
        if (use_atomic) {
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
        else {
            doca_check(doca_sync_event_wait_eq(w, ts.size(), -1));
            doca_check(doca_sync_event_update_set(w, 0));
        }
    }

    void set_params(const params_memcpy_t& params) {
        uint64_t nthreads = ts.size();
        for (uint64_t i = 0;i < nthreads;++i) {
            ts[i].set_params(params);
        }
    }

    ~DPAThreads() {
        doca_check(doca_dpa_kernel_launch_update_set(
            dpa, nullptr, 0, nullptr, 0, 1, trigger, ctx_ptr, ts.size(), 1ull));
        doca_check(doca_dpa_thread_group_stop(tg));
        destroy_se(w);
        destroy_se(s);
        doca_check(doca_dpa_thread_group_destroy(tg));
        doca_check(doca_dpa_mem_free(dpa, ctx_ptr));
        free_mem(cycles_region, cycles_map);
        free_mem(sync_region, sync_map);
    }


    bool use_atomic;
    uint64_t call_counter = 0;
    doca_sync_event* w; // own
    doca_sync_event* s; // own
    doca_dpa_tg* tg;
    doca_mmap* cycles_map;
    doca_mmap* sync_map;
    region_t cycles_region;
    region_t sync_region;
    uintptr_t ctx_ptr;
    std::vector<DPAThread> ts;
};

void run_memcpy_dma(params_memcpy_t params, doca_mmap* dst_m, doca_mmap* src_m) {
    DMAThread* dt;
    DMAThread_pool.pop(dt);
    assert(params.copy_size <= max_dma_buffer_size());
    dt->run(params, dst_m, src_m);
    DMAThread_pool.push(dt);
}

void run_memcpy_dpa(const params_memcpy_t& params) {
    DPAThreads* threads;
    DPAThreads_pool.pop(threads);
    threads->set_params(params);
    threads->trigger_all();
    threads->wait_all();
    DPAThreads_pool.push(threads);
}

static int PrepareConn(struct sockaddr_in* server_addr) {
    auto server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
        sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    if (bind(server_fd, (struct sockaddr*)server_addr, sizeof(*server_addr)) <
        0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }
    return server_fd;
}

void RunJob(int client_fd) {
    // get message from tcp
    printf("RunJob!\n");
    std::string mmap_desc;
    uintptr_t Node_head;
    uintptr_t mt_buf;
    uint64_t num_entries;
    char buffer[1024];
    read(client_fd, buffer, 1024);
    char* ptr = buffer;

    // mems
    num_entries = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);
    Node_head = *(uintptr_t*)ptr;
    ptr += sizeof(uintptr_t);
    mt_buf = *(uintptr_t*)ptr;
    ptr += sizeof(uintptr_t);
    printf("total_size after mems: %ld\n", ptr - buffer);

    // meta_
    ROCKSDB_NAMESPACE::FileMetaData meta;
    int meta_size = recv_meta(&meta, ptr);
    ptr += meta_size;
    printf("total_size after meta_: %ld\n", ptr - buffer);

    // new_versions_NewFileNumber
    uint64_t new_versions_NewFileNumber = *(uint64_t*)ptr;
    ptr += sizeof(uint64_t);

    // seqno_to_time_mapping
    rocksdb::SeqnoToTimeMapping seqno_to_time_mapping =
        *(rocksdb::SeqnoToTimeMapping*)ptr;
    ptr += sizeof(rocksdb::SeqnoToTimeMapping);

    // kUnknownColumnFamily
    uint32_t kUnknownColumnFamily = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);

    // full_history_ts_low
    std::string* str_full_history_ts_low;
    uint32_t n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    if (n > 0) {
        std::string str(ptr, n);
        str.push_back('\0');
        str_full_history_ts_low = &str;
        ptr += n;
    }
    else {
        str_full_history_ts_low = nullptr;
    }

    // io_priority
    rocksdb::Env::IOPriority* io_priority = (rocksdb::Env::IOPriority*)ptr;
    ptr += sizeof(rocksdb::Env::IOPriority);

    // paranoid_file_checks
    bool paranoid_file_checks = *(bool*)ptr;
    ptr += sizeof(bool);

    // job_id
    int job_id = *(int*)ptr;
    ptr += sizeof(int);

    // snapshots
    n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    std::vector<rocksdb::SequenceNumber> snapshots;
    if (n > 0) {
        snapshots.assign(ptr, ptr + n * sizeof(rocksdb::SequenceNumber));
        ptr += n * sizeof(rocksdb::SequenceNumber);
    }

    // earliest_write_conflict_snapshot
    rocksdb::SequenceNumber earliest_write_conflict_snapshot =
        *(rocksdb::SequenceNumber*)ptr;
    ptr += sizeof(rocksdb::SequenceNumber);

    // job_snapshot
    rocksdb::SequenceNumber job_snapshot = *(rocksdb::SequenceNumber*)ptr;
    ptr += sizeof(rocksdb::SequenceNumber);

    // timestamp_size
    size_t timestamp_size = *(size_t*)ptr;
    ptr += sizeof(size_t);

    // tboptions_ioptions_cf_paths
    n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    std::vector<rocksdb::DbPath> tboptions_ioptions_cf_paths;
    if (n > 0) {
        for (uint32_t i = 0; i < n; i++) {
            rocksdb::DbPath newPath;
            newPath.path = ((DbPath_struct*)ptr)->path;
            newPath.target_size = ((DbPath_struct*)ptr)->target_size;
            tboptions_ioptions_cf_paths.push_back(newPath);
            ptr += sizeof(DbPath_struct);
        }
    }

    rocksdb::SequenceNumber smallest_seqno = *(rocksdb::SequenceNumber*)ptr;
    ptr += sizeof(rocksdb::SequenceNumber);
    rocksdb::SequenceNumber largest_seqno = *(rocksdb::SequenceNumber*)ptr;
    ptr += sizeof(rocksdb::SequenceNumber);

    // cfd_GetName
    std::string* cfd_GetName;
    n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    if (n > 0) {
        std::string str(ptr, n);
        str.push_back('\0');
        cfd_GetName = &str;
        ptr += n;
    }
    else {
        cfd_GetName = nullptr;
    }

    // dbname
    std::string* dbname;
    n = *(uint32_t*)ptr;
    ptr += sizeof(uint32_t);
    if (n > 0) {
        std::string str(ptr, n);
        str.push_back('\0');
        dbname = &str;
        ptr += n;
    }
    else {
        dbname = nullptr;
    }

    printf("total_size after dbname : %ld\n", ptr - buffer);
    auto mmap_desc_size = *(uint64_t*)ptr;
    printf("mmap_desc_size:%ld\n", mmap_desc_size);
    ptr += sizeof(uint64_t);
    mmap_desc.assign(ptr, mmap_desc_size);
    printf("total_size: %ld\n", ptr - buffer);


    params_memcpy_t params;
    if (DPA_MEMCPY) {
        params.copy_size = 140 * 1024; // 单次单线程copy大小为140KB
        params.region_size = 140 * 1024 * 1024;//总共copy大小为140MB
        params.piece_size = params.region_size / nthreads_memcpy;//分给八个线程
        params.copy_n = params.piece_size / params.copy_size;//每个线程的copy次数
        params.memcpy_mode = ASYNC;//copy模式
    }
    else {
        params.copy_size = max_dma_buffer_size(); // 单次单线程copy大小为140KB
        params.region_size = 140 * 1024 * 1024;//总共copy大小为140MB
        params.piece_size = params.region_size;//分给八个线程
        params.copy_n = params.piece_size / params.copy_size;//每个线程的copy次数
        params.memcpy_mode = DMA;//copy模式
    }

    uint64_t offset;
    doca_mmap* dst_m;
    doca_mmap* src_m;

    std::tie(dst_m, params.dst) = alloc_mem(Location::Host, params.region_size);
    std::tie(src_m, params.src) = alloc_mem_from_export(Location::Host, params.region_size, mmap_desc);
    params.src.ptr = mt_buf;
    offset = params.dst.ptr - params.src.ptr;
    auto a = std::chrono::high_resolution_clock::now();
    if (DPA_MEMCPY) {
        run_memcpy_dpa(params);
    }
    else {
        run_memcpy_dma(params, dst_m, src_m);
    }
    auto b = std::chrono::high_resolution_clock::now();
    printf("memcpytime:%lu\n", std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count() / 1000 / 1000);

    rocksdb::Status return_status = rocksdb::Status();
    rocksdb::Arena arena;
    rocksdb::ReadOptions ro;
    ro.total_order_seek = true;
    ro.io_activity = rocksdb::Env::IOActivity::kFlush;

    uint64_t num_input_entries = 0;
    uint64_t memtable_payload_bytes = 0;
    uint64_t memtable_garbage_bytes = 0;
    uint64_t packed_number_and_path_id = 0;
    uint64_t file_size = 0;

    printf("meta.smallest.DebugString:%s\n",
           meta.smallest.DebugString(true).c_str());
    printf("meta.largest.DebugString:%s\n", meta.largest.DebugString(true).c_str());
    auto a_point = std::chrono::high_resolution_clock::now();
    BuildTable_new(  // versions_->current_next_file_number()
        Node_head, offset, num_entries, &meta, new_versions_NewFileNumber, seqno_to_time_mapping,
        kUnknownColumnFamily, paranoid_file_checks, job_id,
        earliest_write_conflict_snapshot, job_snapshot, timestamp_size,
        tboptions_ioptions_cf_paths, *cfd_GetName, *dbname,
        // 以下参数需要回传
        &return_status, &num_input_entries, &memtable_payload_bytes,
        &memtable_garbage_bytes, &packed_number_and_path_id, &file_size,
        &smallest_seqno, &largest_seqno);
    auto b_point = std::chrono::high_resolution_clock::now();
    uint64_t buildtable_time = std::chrono::duration_cast<std::chrono::nanoseconds>(b_point - a_point).count();
    printf("builder over, buildtable time:%lu\n", buildtable_time / 1000 / 1000);
    //返回消息并释放空间

    printf(
        "Flush_result:   num_input_entries:%ld memtable_payload_bytes:%ld "
        "memtable_garbage_bytes:%ld packed_number_and_path_id:%ld file_size:%ld "
        "largest_seqno:%ld smallest_seqno:%ld\n",
        num_input_entries, memtable_payload_bytes, memtable_garbage_bytes,
        packed_number_and_path_id, file_size, largest_seqno, smallest_seqno);
    printf("meta.smallest.DebugString:%s\n",
           meta.smallest.DebugString(true).c_str());
    printf("meta.largest.DebugString:%s\n", meta.largest.DebugString(true).c_str());
    char result_buffer[1024];
    ptr = result_buffer;
    int result_size = 0;

    int send_size = send_meta(&meta, ptr);
    ptr += send_size;
    result_size += send_size;
    printf("SEND: total_size after meta: %ld\n", ptr - result_buffer);

    *(rocksdb::Status*)ptr = return_status;
    ptr += sizeof(rocksdb::Status);
    result_size += sizeof(rocksdb::Status);


    *(uint64_t*)ptr = num_input_entries;
    ptr += sizeof(uint64_t);
    result_size += sizeof(uint64_t);

    *(uint64_t*)ptr = memtable_payload_bytes;
    ptr += sizeof(uint64_t);
    result_size += sizeof(uint64_t);

    *(uint64_t*)ptr = memtable_garbage_bytes;
    ptr += sizeof(uint64_t);
    result_size += sizeof(uint64_t);

    *(uint64_t*)ptr = packed_number_and_path_id;
    ptr += sizeof(uint64_t);
    result_size += sizeof(uint64_t);

    *(uint64_t*)ptr = file_size;
    ptr += sizeof(uint64_t);
    result_size += sizeof(uint64_t);


    *(rocksdb::SequenceNumber*)ptr = smallest_seqno;
    ptr += sizeof(rocksdb::SequenceNumber);
    result_size += sizeof(rocksdb::SequenceNumber);


    *(rocksdb::SequenceNumber*)ptr = largest_seqno;
    ptr += sizeof(rocksdb::SequenceNumber);
    result_size += sizeof(rocksdb::SequenceNumber);

    send(client_fd, result_buffer, result_size, 0);
    free_mem(params.dst, dst_m);
    doca_check(doca_mmap_stop(src_m));
    doca_check(doca_mmap_destroy(src_m));
}

int main() {

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(10086);
    auto server_fd = PrepareConn(&server_addr);

    if (DPA_MEMCPY) {
        // DPA COPY
        attach_device(dflush_app);
        params_memcpy_t params;
        params.copy_size = 140 * 1024; // 单次单线程copy大小为140KB
        params.region_size = 140 * 1024 * 1024;//总共copy大小为140MB
        params.piece_size = params.region_size / nthreads_memcpy;//分给八个线程
        params.copy_n = params.piece_size / params.copy_size;//每个线程的copy次数
        params.memcpy_mode = ASYNC;//copy模式

        DPAThreads_pool.setMaxSize(nthreads_task);
        for (uint64_t i = 0; i < nthreads_task; ++i) {
            DPAThreads_pool.push(new DPAThreads(nthreads_memcpy, params, 1));
        }
    }
    else {
        // DMA COPY
        open_device(&dma_task_is_supported);
        auto num_dma_tasks = 140 * 1024 * 1024 / max_dma_buffer_size();
        auto max_bufs = num_dma_tasks * 2;
        DMAThread_pool.setMaxSize(nthreads_task);
        for (int i = 0;i < nthreads_task;++i) {
            DMAThread_pool.push(new DMAThread(max_bufs, num_dma_tasks));
        }
    }

    ThreadPool tp(nthreads_task);
    while (true) {
        socklen_t address_size = sizeof(server_addr);
        auto client_fd =
            accept(server_fd, (struct sockaddr*)&server_addr, &address_size);
        if (client_fd < 0) {
            printf("fail to accept!\n");
            exit(EXIT_FAILURE);
        }
        tp.enqueue(RunJob, client_fd);
    }

    if (DPA_MEMCPY) {
        detach_device();
    }
    else {
        close_device();
    }
    return 0;
}
