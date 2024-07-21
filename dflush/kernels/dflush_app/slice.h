#ifndef _SLICE_H_
#define _SLICE_H_

#include <doca_dpa_dev.h>
#include <doca_dpa_dev_buf.h>

enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };
enum CompressionType { kNoCompression = 0x0, kSnappyCompression = 0x1 };

typedef uint64_t SequenceNumber;

struct Slice {
    const char* data_;
    size_t size_;
};

struct ParsedInternalKey {
    struct Slice user_key;
    SequenceNumber sequence;
    enum ValueType type;
};

union types {
    uint8_t* u8;
    uint64_t* u64;
    uintptr_t uptr;
};

union const_types {
    const uint8_t* u8;
    const uint64_t* u64;
};


void memcpy(void* dst, const void* src, size_t count) {
    const int mask = 7;
    const int distance = (src - dst) & mask;
    union const_types s = { .u8 = src };
    union types d = { .u8 = dst };

    if (count < 16) goto copy_remainder;

    for (;count && d.uptr & mask;count--)
        *d.u8++ = *s.u8++;

    if (distance) {
        uint64_t last, next;
        s.u8 -= distance;
        uint64_t l = distance << 3;
        uint64_t r = (8 - distance) << 3;
        for (next = s.u64[0];count >= 15;count -= 8) {
            last = next;
            next = s.u64[1];
            d.u64[0] = last >> l | next << r;
            d.u64++;
            s.u64++;
        }
        s.u8 += distance;
    }
    else {
        for (;count >= 8;count -= 8)
            *d.u64++ = *s.u64++;
    }
copy_remainder:
    while (count--)
        *d.u8++ = *s.u8++;
}

int memcmp(const void* cs, const void* ct, size_t count) {
    const unsigned char* su1 = (const unsigned char*)cs;
    const unsigned char* su2 = (const unsigned char*)ct;
    int res = 0;
    for (; 0 < count; ++su1, ++su2, count--)
        if ((res = *su1 - *su2) != 0)
            break;
    return res;
}

int Compare(struct Slice akey, struct Slice bkey) {
    const size_t min_len = (akey.size_ < bkey.size_) ? akey.size_ : bkey.size_;
    int r = memcmp(akey.data_, bkey.data_, min_len);
    if (r == 0) {
        if (akey.size_ < bkey.size_)
            r = -1;
        else if (akey.size_ > bkey.size_)
            r = +1;
    }
    return r;
}

void Resize(struct Slice* s, size_t size) { s->size_ = size; }

void ResetNext(struct Slice* s) {
    s->data_ = s->data_ + s->size_;
    s->size_ = 0;
}
void Reset(struct Slice* s, const char* data, size_t size) {
    s->data_ = data;
    s->size_ = size;
}

// dst need to preallocate enough size for append
void Append(struct Slice* s, const char* p, size_t size) {
    memcpy((s->data_ + s->size_), p, size);
    s->size_ += size;
}

#endif