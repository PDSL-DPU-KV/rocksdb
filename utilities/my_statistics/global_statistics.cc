
#include "rocksdb/utilities/my_statistics/global_statistics.h"

namespace rocksdb {

#ifdef STATISTIC_OPEN
    GLOBAL_STATS global_stats;
#endif

uint64_t get_now_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000000 + tv.tv_usec;
}

uint64_t get_now_nanos() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

}