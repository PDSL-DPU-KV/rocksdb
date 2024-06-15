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

#include <doca_dev.h>
#include <doca_dpa.h>
#include <doca_mmap.h>
#include <doca_sync_event.h>
#include <doca_buf_array.h>

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

    const uint32_t nthreads_task = 1;
    const uint32_t nthreads_memcpy = 8;

    enum class Location : uint32_t {
        Host = HOST,
        Device = DEVICE,
    };

    std::pair<doca_mmap*, region_t> alloc_mem(Location l, uint64_t len) {
        region_t r;
        r.flag = (uint32_t)l;
        doca_mmap* mmap;
        doca_check(doca_mmap_create(&mmap));
        switch (l) {
            case Location::Host: {
                    r.ptr = (uintptr_t)aligned_alloc(64, len);
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
        doca_buf_arr* buf_arr;
        size_t size;
        doca_check(doca_mmap_create_from_export(nullptr, export_desc.data(), export_desc.size(), dev, &mmap));
        doca_check(doca_mmap_get_memrange(mmap, (void**)&r.ptr, &size));
        printf("r.ptr:%lx\n", r.ptr);
        doca_check(doca_mmap_dev_get_dpa_handle(mmap, dev, &r.handle));
        return { mmap, r };
    }

    void do_memset(region_t r, uint64_t len, char c) {
        if (r.flag == (uint32_t)Location::Host) {
            memset((char*)r.ptr, c, len);
        }
    }

    void do_memcheck(region_t dst, region_t src, uint64_t len) {
        if (dst.flag == (uint32_t)Location::Host &&
            src.flag == (uint32_t)Location::Host) {
            if (0 != memcmp((void*)dst.ptr, (void*)src.ptr, len)) {
                std::abort();
            }
        }
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

            doca_check(doca_dpa_completion_create(dpa, 1023, &comp));
            doca_check(doca_dpa_completion_set_thread(comp, t));
            doca_check(doca_dpa_completion_start(comp));
            doca_check(doca_dpa_completion_get_dpa_handle(comp, &c.comp_handle));

            doca_check(doca_dpa_notification_completion_create(dpa, t, &notify));
            doca_check(doca_dpa_notification_completion_start(notify));
            doca_check(doca_dpa_notification_completion_get_dpa_handle(
                notify, &c.notify_handle));

            doca_check(doca_dpa_async_ops_create(dpa, 1023, 114514, &aops));
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

    void run_memcpy(const params_memcpy_t& params) {
        double actual_elapsed = 0; //计时用
        auto ts = new DPAThreads(nthreads_memcpy, params, 1);
        auto cycles = (uint64_t*)ts->cycles_region.ptr;
        uint64_t total_elapsed = 0;

        Timer t;
        t.begin();
        ts->trigger_all();
        ts->wait_all();
        t.end();

        total_elapsed += t.elapsed().count();
        uint64_t max_s = 0;
        for (uint32_t j = 0;j < ts->ts.size();++j) {
            max_s = std::max(cycles[j], max_s);
            cycles[j] = 0;
        }
        delete ts;
    }

    void RunJob(int client_fd, int task_id) {
        // 从 tcp 中解析数据
        std::string mmap_desc;
        uintptr_t Node_haed, mt_buf_head;
        char buffer[1024];
        read(client_fd, buffer, 1024);
        char* ptr = buffer;
        Node_haed = *(uintptr_t*)ptr;
        ptr += sizeof(uintptr_t);
        mt_buf_head = *(uintptr_t*)ptr;
        ptr += sizeof(uintptr_t);
        auto mmap_desc_size = *(uint64_t*)ptr;
        ptr += sizeof(uint64_t);
        mmap_desc.assign(ptr, mmap_desc_size);

        params_memcpy_t params;
        params.copy_size = 140 * 1024; // 单次单线程copy大小为140KB
        params.region_size = 140 * 1024 * 1024;//总共copy大小为140MB
        params.piece_size = params.region_size / nthreads_memcpy;//分给八个线程
        params.copy_n = params.piece_size / params.copy_size;//每个线程的copy次数
        params.memcpy_mode = ASYNC;//copy模式

        doca_mmap* dst_m;
        doca_mmap* src_m;
        auto dst_l = Location::Host;
        auto src_l = Location::Host;
        std::tie(dst_m, params.dst) = alloc_mem(dst_l, params.region_size);
        printf("mt_buf:%lx, Node_head:%lx\n", mt_buf_head, Node_haed);
        std::tie(src_m, params.src) = alloc_mem_from_export(src_l, params.region_size, mmap_desc);
        params.src.ptr = mt_buf_head;
        run_memcpy(params);

        /// TODO:对dst的memtable进行剩下的操作

        static uint64_t nums = 0; // 只是两边验证一下
        const uint64_t offset = params.dst.ptr - params.src.ptr;

        // 这里加减offset有点烦人，只是为了验证，后面可以修改
        uintptr_t key1_ptr = Node_haed + sizeof(uintptr_t);
        ROCKSDB_NAMESPACE::Slice tmp = ROCKSDB_NAMESPACE::GetLengthPrefixedSlice((const char*)(key1_ptr + offset));
        key1_ptr = (uintptr_t)tmp.data();
        printf("nums:%ld, head_key_ptr:%lx\n", nums, key1_ptr - offset);
        printf("nums:%ld, key1:%ld\n", nums, *(uint64_t*)(key1_ptr));
        uintptr_t node_next = *(uintptr_t*)(Node_haed + offset);
        uintptr_t key2_ptr = node_next + sizeof(uintptr_t);
        tmp = ROCKSDB_NAMESPACE::GetLengthPrefixedSlice((const char*)(key2_ptr + offset));
        key2_ptr = (uintptr_t)tmp.data();
        printf("nums:%ld, next_key_ptr:%lx\n", nums, key2_ptr - offset);
        printf("nums:%ld, key2:%ld\n", nums++, *(uint64_t*)(key2_ptr));




        // 返回消息并释放空间
        std::string result = "OK";
        send(client_fd, result.data(), result.size(), 0);
        free_mem(params.dst, dst_m); // 目前是这样写的，主要是现用现分配方便，后续看怎么改效率更高
    }

    int main() {

        attach_device(dflush_app); // 启动dpa并附加到dflush_app上
        struct sockaddr_in server_addr;
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(10086);
        auto server_fd = PrepareConn(&server_addr);

        ThreadPool tp(nthreads_task);
        int task_id = 0;
        while (true) {
            socklen_t address_size = sizeof(server_addr);
            auto client_fd = accept(server_fd, (struct sockaddr*)&server_addr, &address_size);
            if (client_fd < 0) {
                printf("fail to accept!\n");
                exit(EXIT_FAILURE);
            }
            tp.enqueue(RunJob, client_fd, task_id++);
        }
    }
