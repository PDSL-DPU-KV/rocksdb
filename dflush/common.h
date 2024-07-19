#pragma once

#include <doca_dev.h>
#include <doca_dma.h>
#include <doca_dpa.h>
#include <doca_mmap.h>
#include <doca_sync_event.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <string>
#include <utility>

#ifndef NDEBUG
#include <doca_error.h>

#include <cstdio>
#include <cstdlib>
#define doca_check(expr)                                              \
  do {                                                                \
    doca_error_t __doca_check_result__ = expr;                        \
    if (__doca_check_result__ != DOCA_SUCCESS) {                      \
      fprintf(stderr, "%s:%d:%s:" #expr ": %s\n", __FILE__, __LINE__, \
              __func__, doca_error_get_descr(__doca_check_result__)); \
      if (dpa != nullptr) {                                           \
        doca_error_t __doca_dpa_last_error__ =                        \
            doca_dpa_peek_at_last_error(dpa);                         \
        fprintf(stderr, "DPA ERROR: %s\n",                            \
                doca_error_get_descr(__doca_dpa_last_error__));       \
      }                                                               \
      std::abort();                                                   \
    }                                                                 \
  } while (0);
#else
#define doca_check(expr) (void)(expr)
#endif

constexpr const inline char* ibdev_name = "mlx5_1";

extern doca_dev* dev;
extern doca_dpa* dpa;

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

inline void attach_device(doca_dpa_app* app) {
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
      dev_info = dev_list[i];
      break;
    }
  }
  if (!dev_info) {
    std::abort();
  }
  doca_check(doca_dev_open(dev_info, &dev));
  doca_check(doca_devinfo_destroy_list(dev_list));
  doca_check(doca_dpa_create(dev, &dpa));
  doca_check(doca_dpa_set_app(dpa, app));
  doca_check(doca_dpa_set_log_level(dpa, DOCA_DPA_DEV_LOG_LEVEL_DEBUG));
  if (access(".log", F_OK) != 0) {
    int result = mkdir(".log", 0777);
    if (result != 0) {
      fprintf(stderr, "ERROR: create log file? %s\n", strerror(errno));
      std::abort();
    }
  }
  doca_check(doca_dpa_log_file_set_path(dpa, ".log/dpa.log"));
  doca_check(doca_dpa_start(dpa));
}

inline void detach_device() {
  doca_check(doca_dpa_destroy(dpa));
  doca_check(doca_dev_close(dev));
}

typedef doca_error_t (*tasks_check)(struct doca_devinfo*);

inline doca_error_t dma_task_is_supported(struct doca_devinfo* devinfo) {
  return doca_dma_cap_task_memcpy_is_supported(devinfo);
}

inline uint64_t max_dma_buffer_size() {
  uint64_t max_buffer_size = 0;
  doca_dma_cap_task_memcpy_get_max_buf_size(doca_dev_as_devinfo(dev),
                                            &max_buffer_size);
  return max_buffer_size;
}

inline void open_device(tasks_check func) {
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
      if (func != NULL && func(dev_list[i]) != DOCA_SUCCESS) continue;
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

inline void close_device() { doca_check(doca_dev_close(dev)); }

enum class Location : uint32_t {
  Host = HOST,
  Device = DEVICE,
};

constexpr const uint32_t access_mask =
    DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE;

inline std::pair<doca_mmap*, region_t> alloc_mem(Location l, uint64_t len) {
  region_t r = {0, 0, 0};
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
  return {mmap, r};
}

inline std::pair<doca_mmap*, region_t> alloc_mem_from_export(
    Location l, uint64_t len, std::string& export_desc) {
  region_t r;
  r.flag = (uint32_t)l;
  doca_mmap* mmap;
  size_t size;
  doca_check(doca_mmap_create_from_export(nullptr, export_desc.data(),
                                          export_desc.size(), dev, &mmap));
  doca_check(doca_mmap_get_memrange(mmap, (void**)&r.ptr, &size));
  doca_check(doca_mmap_start(mmap));
  doca_check(doca_mmap_dev_get_dpa_handle(mmap, dev, &r.handle));
  return {mmap, r};
}

inline void free_mem(region_t& r, doca_mmap* mmap) {
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

inline std::pair<doca_sync_event*, doca_dpa_dev_sync_event_t> create_se(
    bool host2dev) {
  doca_sync_event* se;
  doca_dpa_dev_sync_event_t h;
  doca_check(doca_sync_event_create(&se));
  if (host2dev) {
    doca_check(doca_sync_event_add_publisher_location_cpu(se, dev));
    doca_check(doca_sync_event_add_subscriber_location_dpa(se, dpa));
  } else {
    doca_check(doca_sync_event_add_publisher_location_dpa(se, dpa));
    doca_check(doca_sync_event_add_subscriber_location_cpu(se, dev));
  }
  doca_check(doca_sync_event_start(se));
  doca_check(doca_sync_event_get_dpa_handle(se, dpa, &h));
  return {se, h};
}

inline void destroy_se(doca_sync_event* se) {
  doca_check(doca_sync_event_stop(se));
  doca_check(doca_sync_event_destroy(se));
}