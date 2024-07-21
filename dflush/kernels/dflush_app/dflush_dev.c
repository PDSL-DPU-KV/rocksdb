#include <dpaintrin.h>

#include <doca_dpa_dev.h>
#include <doca_dpa_dev_buf.h>
#include <doca_dpa_dev_rdma.h>

#include "dflush_common.h"
#include "helper.h"
#include "util.h"


static void* get_offset_ptr_of_region(region_t* r, uintptr_t r_ptr) {
    if (r->flag == HOST) {
        return (void*)doca_dpa_dev_mmap_get_external_ptr(r->handle, r_ptr);
    }
    else {
        return (void*)r_ptr;
    }
    UNREACHABLE_CRIT;
    doca_dpa_dev_thread_finish();
    return 0;
}

static void* get_base_ptr_of_region(region_t* r) {
    if (r->flag == HOST) {
        return (void*)doca_dpa_dev_mmap_get_external_ptr(r->handle, r->ptr);
    }
    else if (r->flag == DEVICE) {
        return (void*)r->ptr;
    }
    UNREACHABLE_CRIT;
    doca_dpa_dev_thread_finish();
    return 0;
}

__dpa_global__ static void trigger(uintptr_t ctx_ptr, uint64_t n_thread,
                                   uint64_t is_done) {
    ctx_t* t = (ctx_t*)ctx_ptr;
    for (uint64_t i = 0; i < n_thread; i++) {
        if (is_done) {
            t[i].notify_done = is_done;
        }
        doca_dpa_dev_thread_notify(t[i].notify_handle);
    }
}

static uint64_t run_async(ctx_t* t, uint64_t rank) {
    uintptr_t dst_ptr = t->params.dst.ptr + rank * t->params.piece_size;
    uintptr_t src_ptr = t->params.src.ptr + rank * t->params.piece_size;
    uint32_t counter = 0;
    doca_dpa_dev_completion_element_t e;

    // TICK0;
    // LOG_DBG("rank:%u run memcpy\n", rank);
    for (uint32_t i = 0; i < t->params.copy_n; i++) {
        // LOG_DBG("%d\n", i);
        doca_dpa_dev_post_memcpy(t->aops_handle, t->params.dst.handle, dst_ptr,
                     t->params.src.handle, src_ptr, t->params.copy_size,
                     1);
        dst_ptr += t->params.copy_size;
        src_ptr += t->params.copy_size;
        while (1) {
            // LOG_DBG("rank:%u circle!\n", rank);
            int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
            if (got == 1) {
                doca_dpa_dev_completion_type_t et = doca_dpa_dev_get_completion_type(e);
                doca_dpa_dev_completion_ack(t->comp_handle, 1);
                counter++;
                if (et != 0) {
                    UNREACHABLE_CRIT;
                }
                break;
                // LOG_DBG("et:%d poll %d\n", et, counter);
            }
            // else {
            //     break;
            // }
        }
    }
    // while (counter < t->params.copy_n) {
    //     // LOG_DBG("rank:%u circle!\n", rank);
    //     int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
    //     if (got == 1) {
    //         doca_dpa_dev_completion_type_t et = doca_dpa_dev_get_completion_type(e);
    //         doca_dpa_dev_completion_ack(t->comp_handle, 1);
    //         counter++;
    //         if (et != 0) {
    //             UNREACHABLE_CRIT;
    //         }
    //         // LOG_DBG("%d poll %d\n", et, counter);
    //     }
    // }
    // LOG_DBG("rank:%u end memcpy\n", rank);
    // TICK1;
    // PRINT_TICK(1);
    return 0;
}

__dpa_global__ static void dflush_func(uintptr_t ctx_ptr) {
    ctx_t* t = (ctx_t*)ctx_ptr;
    uint32_t rank = doca_dpa_dev_thread_rank();
    // LOG_DBG("rank:%u start\n", rank);
    if (t->use_atomic) {
        // uint64_t cycle = 0;
        // cycle = run_async(t, rank);
        run_async(t, rank);

        // uint64_t* cycles = (uint64_t*)get_base_ptr_of_region(&t->result);
        get_base_ptr_of_region(&t->result);
        // cycles[rank] = cycle;
        __dpa_thread_window_writeback();

        uint64_t* sync = (uint64_t*)get_base_ptr_of_region(&t->sync);
        sync[rank] = 1;
        __dpa_thread_window_writeback();

        doca_dpa_dev_thread_reschedule();
    }

    doca_dpa_dev_completion_element_t e;
    int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
    if (got == 1) {
        doca_dpa_dev_completion_ack(t->comp_handle, 1);

        // LOG_DBG("%d %lx %lu %lu %lu %d %d %lx %d %d %lx %lx %lx\n", rank,
        //         (uintptr_t)t, t->params.region_size, t->params.piece_size,
        //         t->params.copy_size, t->params.dst.handle, t->params.dst.flag,
        //         t->params.dst.ptr, t->params.src.handle, t->params.src.flag,
        //         t->params.src.ptr, t->w_handle, t->s_handle);

        // uint64_t cycle = 0;
        run_async(t, rank);

        // LOG_TRACE("write back\n");

        // uint64_t* cycles = (uint64_t*)get_base_ptr_of_region(&t->result);
        // cycles[rank] = cycle;
        __dpa_thread_window_writeback();
        // LOG_DBG("rank:%u before add", rank);
        doca_dpa_dev_sync_event_update_add(t->w_handle, 1);
        // LOG_DBG("rank:%u add\n", rank);
        doca_dpa_dev_completion_request_notification(t->comp_handle);
    }

    if (t->notify_done == 0) {
        // LOG_DBG("post wait gt %lu\n", t->call_counter);
        doca_dpa_dev_sync_event_post_wait_gt(t->aops_handle, t->s_handle,
                                             t->call_counter);
        ++t->call_counter;
        // LOG_DBG("rank:%u, reschedule\n", rank);
        doca_dpa_dev_thread_reschedule();
    }
    else {
        // LOG_DBG("rank:%u, finish\n", rank);
        doca_dpa_dev_thread_finish();
    }
}

// add 所需要的常量
const uint64_t datablock_size = 4096;

__dpa_global__ void dpa_flush(uintptr_t ctx_ptr) {
    ctx_t* t = (ctx_t*)ctx_ptr;
    uint32_t rank = doca_dpa_dev_thread_rank();
    doca_dpa_dev_completion_element_t e;

    if (t->call_counter != 0) {
        int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
        if (got != 1) {
            doca_dpa_dev_completion_type_t et = doca_dpa_dev_get_completion_type(e);
            doca_dpa_dev_completion_ack(t->comp_handle, 1);
            if (et != 0) {
                UNREACHABLE_CRIT;
            }
            doca_dpa_dev_thread_reschedule();
        }
    }
    t->call_counter++;

    if (t->notify_done) {
        doca_dpa_dev_thread_finish();
    }

    update_dpa_ptr(t->bufarr_handle);

    // 设置起始和终止条件
    uintptr_t head_addr = t->node_head;
    uintptr_t tail_addr = t->node_end;
    // uintptr_t tail_addr = host2dev(0ul);
    uintptr_t cur_addr = head_addr;

    // iter所需变量
    struct Slice key, value, current_user_key;
    struct ParsedInternalKey ikey;
    uint8_t has_current_user_key = 0;
    uint64_t smallest_seqno = 0xffffffff;
    uint64_t largest_seqno = 0;
    char smallest_char[32];
    char largest_char[32];
    struct Slice smallest = { .data_ = (const char*)smallest_char, .size_ = 0 };
    struct Slice largest = { .data_ = (const char*)largest_char, .size_ = 0 };

    // add 所需变量
    uint64_t props_num_entries = 0;
    uint64_t props_raw_key_size = 0;
    uint64_t props_raw_value_size = 0;
    char first_key_in_next_block_char[32];
    struct Slice first_key_in_next_block = { .data_ = (const char*)first_key_in_next_block_char, .size_ = 0 };
    uint64_t buffer_size = 0;
    uint64_t key_nums = 0;
    uint64_t estimate = 0;

    // 设置 arm内存存放位置
    uint64_t datablock_nums = 0; // 目前这个线程已经生成的 datablock 数量 必须小于 t->params.copy_n
    uintptr_t dst_ptr = t->params.dst.ptr + rank * t->params.piece_size; // dst 起始地址
    uintptr_t cur_ptr = dst_ptr; // 当前 block 首地址
    uintptr_t keys_ptr = cur_ptr + 128; // Keys 的首地址
    uintptr_t buffer_ptr = keys_ptr + 896; // buffer 的首地址
    // 前 128B 内存构造
    // flag (uint64_t) | key_nums(uint64_t)  | key_size(uint64_t) | buffer_size(uint64_t) | firstkey in next block(slice without size) |
    // Keys内存构造(896B)
    // key1 .. keyn 
    // Buffer 内存构造 (4KB)
    // shared(varint32) nonshared(varint32) val_size(varint32) key value | ... | ...

    // 第二种内存构造，用于最后结束
    // | flag(uint64_t) | key_size(uint64_t) | 三个 prop (uint64) | smallest/largeset sqn (uint64_t) | smallest/largest key(slice without size) |

    LOG_DBG("rank:%u, bufferhandle:%lx, node_head:%lx, node_end:%lx\n", rank, t->bufarr_handle, t->node_head, t->node_end);
    LOG_DBG("rank:%u, cur_ptr:%lx, keys_ptr:%lx, buffer_ptr:%lx\n", rank, cur_ptr, keys_ptr, buffer_ptr);

    // used for post memcpy 
    uint32_t counter = 0;

    // 计数用
    uint64_t iter_num = 0;
    uint64_t drop_num = 0;
    while (cur_addr != tail_addr) {
        iter_num++;
        key = Key_Memtable(cur_addr);
        uint8_t drop = 0;
        if (!ParseInternalKey(key, &ikey)) {
            current_user_key.size_ = 0;
            has_current_user_key = 0;
            break;
        }
        else {
            if (!has_current_user_key || Compare(ikey.user_key, current_user_key) != 0) {
                current_user_key.data_ = ikey.user_key.data_;
                current_user_key.size_ = ikey.user_key.size_;
                has_current_user_key = 1;
            }
            else {
                drop_num++;
                drop = 1;
            }
        }
        if (!drop) {
            value = Value_Memtable(key);

            // 执行 add
            if (estimate + key.size_ + value.size_ + 3 * sizeof(uint32_t) > datablock_size) {
                // 满足一个datablock的条件
                // 在dpa的栈里面中转一下key
                memcpy((void*)first_key_in_next_block.data_, (void*)key.data_, key.size_);
                first_key_in_next_block.size_ = key.size_;
                char* buf = (char*)get_offset_ptr_of_region(&t->params.dst, cur_ptr);
                char* p = buf + sizeof(uint64_t); // 第一个8字节是flag

                *(uint64_t*)p = key_nums;
                p += sizeof(uint64_t);
                *(uint64_t*)p = key.size_;
                p += sizeof(uint64_t);
                *(uint64_t*)p = estimate;
                p += sizeof(uint64_t);
                memcpy((void*)p, (void*)first_key_in_next_block.data_, first_key_in_next_block.size_);

                *(uint64_t*)buf = 1; // 将 flag 置为 1 ，后续可能会有 flag 为 2 的情况

                key_nums = 0;
                estimate = 0;
                datablock_nums++;
                cur_ptr += t->params.copy_size;
                keys_ptr = cur_ptr + 128;
                buffer_ptr = keys_ptr + 896;

                update_dpa_ptr(t->bufarr_handle);
            }
            // 将 key 插入到  Keys 中
            doca_dpa_dev_post_memcpy(t->aops_handle, t->params.dst.handle, keys_ptr, t->params.src.handle, dev2host((uintptr_t)key.data_), key.size_, 1);
            keys_ptr += key.size_;
            key_nums++;
            // 将 key value插入到 buffer 中
            uint64_t prefix_length = VarintLength(0) + VarintLength(key.size_) + VarintLength(value.size_);
            doca_dpa_dev_post_memcpy(t->aops_handle, t->params.dst.handle, buffer_ptr, t->params.src.handle, dev2host((uintptr_t)(value.data_ + value.size_)), prefix_length, 1);
            buffer_ptr += prefix_length;
            doca_dpa_dev_post_memcpy(t->aops_handle, t->params.dst.handle, buffer_ptr, t->params.src.handle, dev2host((uintptr_t)key.data_), key.size_, 1);
            buffer_ptr += key.size_;
            doca_dpa_dev_post_memcpy(t->aops_handle, t->params.dst.handle, buffer_ptr, t->params.src.handle, dev2host((uintptr_t)value.data_), value.size_, 1);
            buffer_ptr += value.size_;

            // 等待四次memcpy完成
            while (counter < 4) {
                int got = doca_dpa_dev_get_completion(t->comp_handle, &e);
                if (got == 1) {
                    doca_dpa_dev_completion_type_t et = doca_dpa_dev_get_completion_type(e);
                    doca_dpa_dev_completion_ack(t->comp_handle, 1);
                    counter++;
                    if (et != 0) {
                        UNREACHABLE_CRIT;
                    }
                }
            }
            counter = 0;

            // 更新部分数据
            estimate += prefix_length + key.size_ + value.size_;
            props_num_entries++;
            props_raw_key_size += key.size_;
            props_raw_value_size += value.size_;

            if (rank == 0 && smallest.size_ == 0) {
                memcpy((void*)smallest.data_, (void*)key.data_, key.size_);
                smallest.size_ = key.size_;
            }
            smallest_seqno = smallest_seqno < ikey.sequence ? smallest_seqno : ikey.sequence;
            largest_seqno = largest_seqno > ikey.sequence ? largest_seqno : ikey.sequence;
        }
        cur_addr = Next_Memtable(cur_addr);
    }
    LOG_DBG("rank:%u, iter_num:%lu, drop_num:%lu\n", rank, iter_num, drop_num);
    memcpy((void*)largest.data_, (void*)key.data_, key.size_);
    largest.size_ = key.size_;

    // 将最后添加的buffer填入
    char* buf = (char*)get_offset_ptr_of_region(&t->params.dst, cur_ptr);
    char* p = buf + sizeof(uint64_t); // 第一个8字节是flag

    *(uint64_t*)p = key_nums;
    p += sizeof(uint64_t);
    *(uint64_t*)p = key.size_;
    p += sizeof(uint64_t);
    *(uint64_t*)p = estimate;
    p += sizeof(uint64_t);
    memcpy((void*)p, (void*)largest.data_, largest.size_);

    *(uint64_t*)buf = 1;

    datablock_nums++;
    cur_ptr += t->params.copy_size;

    buf = (char*)get_offset_ptr_of_region(&t->params.dst, cur_ptr);
    p = buf + sizeof(uint64_t);

    *(uint64_t*)p = key.size_;
    p += sizeof(uint64_t);
    *(uint64_t*)p = props_num_entries;
    p += sizeof(uint64_t);
    *(uint64_t*)p = props_raw_key_size;
    p += sizeof(uint64_t);
    *(uint64_t*)p = props_raw_value_size;
    p += sizeof(uint64_t);
    *(uint64_t*)p = smallest_seqno;
    p += sizeof(uint64_t);
    *(uint64_t*)p = largest_seqno;
    p += sizeof(uint64_t);
    if (smallest.size_) {
        memcpy((void*)p, (void*)smallest.data_, smallest.size_);
    }
    p += key.size_;
    memcpy((void*)p, (void*)largest.data_, largest.size_);

    *(uint64_t*)buf = 2; // 值为 2 的终止条件

    // 全部结束，变更终止条件
    uint64_t* sync = (uint64_t*)get_base_ptr_of_region(&t->sync);
    sync[rank] = 1;

    __dpa_thread_window_writeback();

    doca_dpa_dev_completion_request_notification(t->comp_handle);
    doca_dpa_dev_thread_reschedule();
}
