#include <atomic>
#include <vector>

#include "common.h"

#if defined(__x86_64__)
#include <immintrin.h>
static inline void relax() { _mm_pause(); }
#elif defined(__aarch64__)
static inline void relax() { asm volatile("yield" ::: "memory"); }
#else
static inline void relax() {}
#endif

extern "C" doca_dpa_app * dflush_app;
extern "C" doca_dpa_func_t dflush_func;
extern "C" doca_dpa_func_t trigger;
extern "C" doca_dpa_func_t dpa_flush;

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
    // doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr + sizeof(ctx_t) -
    // sizeof(params_memcpy_t),
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
    for (uint64_t i = 0; i < nthreads; ++i) {
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
  doca_sync_event* w;  // own
  doca_sync_event* s;  // own
  doca_dpa_tg* tg;
  doca_mmap* cycles_map;
  doca_mmap* sync_map;
  region_t cycles_region;
  region_t sync_region;
  uintptr_t ctx_ptr;
  std::vector<DPAThread> ts;
};

static uintptr_t host2dev(uintptr_t host_addr) {
  return host_addr | (1ULL << 63);
}

struct DPAFlushThread {
  DPAFlushThread(doca_dpa_tg* tg, uintptr_t ctx_ptr, uint64_t rank)
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
    doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr, &c, sizeof(ctx_t)));
    doca_check(doca_dpa_thread_run(t));
  }

  void set_params(const params_memcpy_t& params, const uint64_t bufarr_handle, uintptr_t node_head, uintptr_t node_end) {
    c.params = params;
    c.bufarr_handle = bufarr_handle;
    c.node_head = host2dev(node_head);
    c.node_end = host2dev(node_end);
    doca_check(doca_dpa_h2d_memcpy(dpa, ctx_ptr, &c, sizeof(ctx_t)));
  }

  ~DPAFlushThread() {
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

struct DPAFlushThreads {
  DPAFlushThreads(uint64_t n_threads, const params_memcpy_t& params) {
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
      ts[i].set_params(params, bufarr_handle, node_heads[i], node_heads[i + 1]);
    }
  }

  ~DPAFlushThreads() {
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
  std::vector<DPAFlushThread> ts;
};
