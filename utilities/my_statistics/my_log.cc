#include "rocksdb/utilities/my_statistics/my_log.h"

namespace rocksdb {

void DC_LOG(int file_num, const char* format, ...) {
    va_list ap;
    va_start(ap, format);
    char buf[81920];
    vsprintf(buf, format, ap);
    va_end(ap);

    const std::string* log_file;
    switch (file_num) {
        case 0:
        log_file = &log_file0;
        break;
        case 1:
        log_file = &log_file1;
        break;
        case 2:
        log_file = &log_file2;
        break;
        default:
        return;
    }

    FILE* fp = fopen(log_file->c_str(), "a");
    if (fp == nullptr) {
        printf("%s: log failed 7, reason:%d, %s\n", log_file->c_str(), errno, strerror(errno));
        exit(1);
    }
    fprintf(fp, "%s", buf);
    fclose(fp);
}

void init_mylog_file() {
    FILE* fp;
#ifdef DC_INFO
    fp = fopen(log_file0.c_str(), "w");
    if (fp == nullptr) {
        printf("failed to open log file %s!\n", log_file0.c_str());
        exit(1);
    }
    fclose(fp);
    RECORD_INFO(0,"compaction,read(MB),write(MB),time(s),cputime(s),compress(s),decompress(s),readtime(s),writetime(s),level,thread\n");

    fp = fopen(log_file1.c_str(), "w");
    if (fp == nullptr) {
        printf("failed to open log file %s!\n", log_file1.c_str());
        exit(1);
    }
    fclose(fp);
    RECORD_INFO(1,"flush,write(MB),time(s),cputime(s),compress(s),decompress(s),readtime(s),writetime(s),thread\n");

    fp = fopen(log_file2.c_str(), "w");
    if (fp == nullptr) {
        printf("failed to open log file %s!\n", log_file2.c_str());
        exit(1);
    }
    fclose(fp);
    RECORD_INFO(2,"file,offset,level,count\n");
#endif
}

}