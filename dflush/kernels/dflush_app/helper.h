#pragma once

#include <dpaintrin.h>

#include <doca_dpa_dev.h>

typedef union {
  uint8_t* u8;
  uint16_t* u16;
  uint32_t* u32;
  uint64_t* u64;
  uintptr_t p;
  void* raw;
} ptr_t;

#define UNREACHABLE_CRIT                                                       \
  DOCA_DPA_DEV_LOG_CRIT("%s:%d %s: unreachable\n", __FILE__, __LINE__,         \
                        __func__);

#define UNUSED __attribute__((unused))

#define align_up_8(x) (char *)((((uintptr_t)x) + 7) & (~7ull))
#define h2d(a) ((void *)((a) | (1ull << 63)))
#define d2h(a) (((uint64_t)(a)) & (~(1ull < 63)))

#ifndef DEBUG
#define LOG_DBG(fmt, ...)                                                      \
  DOCA_DPA_DEV_LOG_DBG("%s:%d %s: " fmt, __FILE__, __LINE__, __func__,         \
                       __VA_ARGS__)
#define LOG_TRACE(info)                                                        \
  DOCA_DPA_DEV_LOG_INFO("%s:%d %s: %s", __FILE__, __LINE__, __func__, info)
#else
#define LOG_DBG(fmt, ...) ((void)0)
#define LOG_TRACE(info) ((void)0)
#endif

#ifdef NO_TICK
#define TICK(t) ((void)0)
#else
#define TICK(t) uint64_t t = __dpa_thread_cycles()
#endif

#define TICK0 TICK(t0)
#define TICK1 TICK(t1)
#define TICK2 TICK(t2)
#define TICK3 TICK(t3)
#define TICK4 TICK(t4)
#define TICK5 TICK(t5)
#define TICK6 TICK(t6)
#define STR(x) #x
#define STR_EXPAND(x) STR(x)
#define CAT(x, y) x##y
#define GLUE(x, y) CAT(x, y)
#define REP_0(x)
#define REP_1(x) x
#define REP_2(x) REP_1(x) " " x
#define REP_3(x) REP_2(x) " " x
#define REP_4(x) REP_3(x) " " x
#define REP_5(x) REP_4(x) " " x
#define REP_6(x) REP_5(x) " " x
#define REP_N(n, x) GLUE(REP_, n)(x)
#define SEG_0
#define SEG_1 SEG_0, (t1 - t0)
#define SEG_2 SEG_1, (t2 - t1)
#define SEG_3 SEG_2, (t3 - t2)
#define SEG_4 SEG_3, (t4 - t3)
#define SEG_5 SEG_4, (t5 - t4)
#define SEG_6 SEG_5, (t6 - t5)
#define SEG_N(n) GLUE(SEG_, n)

#ifdef NO_TICK
#define PRINT_TICK(n) ((void)0)
#else
#define PRINT_TICK(n)                                                          \
  DOCA_DPA_DEV_LOG_INFO("%s:%d %s: " REP_N(n, "%lu") "\n", __FILE__, __LINE__,  \
                        __func__ SEG_N(n))
#endif
