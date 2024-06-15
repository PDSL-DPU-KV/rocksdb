#pragma once

#include <cstdio>
#include <cerrno>
#include <doca_dev.h>
#include <doca_dpa.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef NDEBUG
#include <cstdio>
#include <cstdlib>
#include <doca_error.h>
#define doca_check(expr)                                                       \
  do {                                                                         \
    doca_error_t __doca_check_result__ = expr;                                 \
    if (__doca_check_result__ != DOCA_SUCCESS) {                               \
      fprintf(stderr, "%s:%d:%s:" #expr ": %s\n", __FILE__, __LINE__,          \
              __func__, doca_error_get_descr(__doca_check_result__));          \
      if (dpa != nullptr) {                                                    \
        doca_error_t __doca_dpa_last_error__ =                                 \
            doca_dpa_peek_at_last_error(dpa);                                  \
        fprintf(stderr, "DPA ERROR: %s\n",                                     \
                doca_error_get_descr(__doca_dpa_last_error__));                \
      }                                                                        \
      std::abort();                                                            \
    }                                                                          \
  } while (0);
#else
#define doca_check(expr) (void)(expr)
#endif

constexpr const inline char* ibdev_name = "mlx5_1";

inline static doca_dev* dev;
inline static doca_dpa* dpa;

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