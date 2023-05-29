#pragma once

#include <string>
#include <stdarg.h>
#include <string.h>
#include <errno.h>

namespace rocksdb {

#define DC_INFO

#ifdef DC_INFO
#define RECORD_INFO(file_num,format,...) DC_LOG(file_num,format,##__VA_ARGS__)
#else
#define RECORD_INFO(file_num,format,...)
#endif

const std::string log_file0("compaction.csv");
const std::string log_file1("flush.csv");
const std::string log_file2("file_access.csv");

extern void init_mylog_file();

extern void DC_LOG(int file_num, const char *format, ...);

}