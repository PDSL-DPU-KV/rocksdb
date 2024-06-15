#pragma once
#include <algorithm>
#include <string>

#include "slice.h"

const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        }
        else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}


inline const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* value) {
    if (p < limit) {
        uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
        if ((result & 128) == 0) {
            *value = result;
            return p + 1;
        }
    }
    return GetVarint32PtrFallback(p, limit, value);
}


inline Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len = 0;
    // +5: we assume "data" is not corrupted
    // unsigned char is 7 bits, uint32_t is 32 bits, need 5 unsigned char
    auto p = GetVarint32Ptr(data, data + 5 /* limit */, &len);
    return Slice(p, len);
}