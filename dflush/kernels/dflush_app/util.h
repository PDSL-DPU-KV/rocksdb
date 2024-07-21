#ifndef _UTIL_H_
#define _UTIL_H_

#include "slice.h"

uintptr_t dev2host(uintptr_t dev_addr) {
    return dev_addr & ((1ULL << 63) - 1);
}

static uintptr_t host2dev(uintptr_t host_addr) {
    return host_addr | (1ULL << 63);
}

doca_dpa_dev_uintptr_t update_dpa_ptr(doca_dpa_dev_buf_arr_t buf_arr) {
    doca_dpa_dev_buf_t buf = doca_dpa_dev_buf_array_get_buf(buf_arr, 0);
    doca_dpa_dev_uintptr_t buf_addr = doca_dpa_dev_buf_get_external_ptr(buf);
    return buf_addr;
}

static const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                        uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint8_t byte = *(const unsigned char*)p;
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        }
        else {
            result |= (byte << shift);
            *value = result;
            return (const char*)p;
        }
    }
    return NULL;
}

static const char* GetVarint32Ptr(const char* p, const char* limit,
                                uint32_t* value) {
    if (p < limit) {
        uint8_t result = *(const unsigned char*)p;
        if ((result & 128) == 0) {
            *value = result;
            return p + 1;
        }
    }
    return GetVarint32PtrFallback(p, limit, value);
}

inline int VarintLength(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

static struct Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len = 0;
    // +5: we assume "data" is not corrupted
    // unsigned char is 7 bits, uint32_t is 32 bits, need 5 unsigned char
    const char* p = GetVarint32Ptr(data, data + 5 /* limit */, &len);
    struct Slice result;
    result.data_ = p;
    result.size_ = len;
    return result;
}

static uintptr_t Next_Memtable(uintptr_t current_addr) {
    uintptr_t next = *(uintptr_t*)current_addr;
    return host2dev(next);
}

static struct Slice GetNewFormatKey(uintptr_t addr) {
    const char* data = (char*)addr + sizeof(uintptr_t);
    uint32_t key_size = 0, val_size = 0;
    const char* p = GetVarint32Ptr(data + 1, data + 6, &key_size);
    const char* q = GetVarint32Ptr(p, p + 5, &val_size);
    struct Slice result;
    result.data_ = q;
    result.size_ = key_size;
    return result;
}

static struct Slice GetNewFormatValue(uintptr_t addr) {
    const char* data = (char*)addr + sizeof(uintptr_t);
    uint32_t key_size = 0, val_size = 0;
    const char* p = GetVarint32Ptr(data + 1, data + 6, &key_size);
    const char* q = GetVarint32Ptr(p, p + 5, &val_size);
    struct Slice result;
    result.data_ = q + key_size;
    result.size_ = val_size;
    return result;
}

static struct Slice Key_Memtable(uintptr_t addr) {
    const char* key_data = (char*)addr + sizeof(uintptr_t);
    return GetLengthPrefixedSlice(key_data);
}

static struct Slice Value_Memtable(struct Slice key_slice) {
    return GetLengthPrefixedSlice(key_slice.data_ + key_slice.size_);
}

static size_t difference_offset(const struct Slice a, const struct Slice b) {
    size_t off = 0;
    const size_t len = (a.size_ < b.size_) ? a.size_ : b.size_;
    for (;off < len;++off) {
        if (a.data_[off] != b.data_[off]) break;
    }
    return off;
}

static uint64_t DecodeFixed64(const char* ptr) {
    const int distance = (uint64_t)ptr & 7;
    if (distance) {
        uint64_t last = *(uint64_t*)(ptr - distance);
        uint64_t next = *(uint64_t*)(ptr + 8 - distance);
        return last >> (distance << 3) | next << ((8 - distance) << 3);
    }
    else {
        return *(uint64_t*)ptr;
    }
}

static uint8_t ParseInternalKey(struct Slice internal_key,
                           struct ParsedInternalKey* result) {
    const size_t n = internal_key.size_;
    if (n < 8) return 0;
    // LOG_DBG("addr:%lx, n:%ld\n", internal_key.data_, n);
    uint64_t num = DecodeFixed64(internal_key.data_ + n - 8);
    // LOG_DBG("num:%lx\n", num);
    uint8_t c = num & 0xff;
    result->sequence = num >> 8;
    result->type = (enum ValueType)(c);
    result->user_key.data_ = internal_key.data_;
    result->user_key.size_ = n - 8;
    return (c <= 1);
}

#endif