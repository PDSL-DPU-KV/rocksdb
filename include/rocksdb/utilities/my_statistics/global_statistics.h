#pragma once

#include <stdint.h>
#include <sys/time.h>
#include <stdlib.h>
#include <atomic>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <vector>

#define STATISTIC_OPEN

namespace rocksdb {

#ifdef STATISTIC_OPEN
    struct GLOBAL_STATS {
        uint64_t compaction_num;
        uint64_t flush_num;
        uint64_t pick_compaction_time;
        uint64_t start_time;
        std::vector<std::map<uint64_t, uint64_t>*> file_access_freq;

        GLOBAL_STATS()
            : file_access_freq(7) {
            compaction_num = 0;
            flush_num = 0;
            pick_compaction_time = 0;
            start_time = 0;
            for (int i = 0; i < 7; i++) {
                file_access_freq[i] = new std::map<uint64_t, uint64_t>;
            }
        }
    };
    extern struct GLOBAL_STATS global_stats;
#endif

uint64_t get_now_micros();

uint64_t get_now_nanos();

}