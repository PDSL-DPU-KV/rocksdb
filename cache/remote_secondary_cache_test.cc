//  Author: Ding Chen

#include "cache/remote_secondary_cache.h"

#include <gtest/gtest.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>

#include <memory>

#include "port/stack_trace.h"
#include "test_util/secondary_cache_test_util.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/spdlogger.h"

namespace ROCKSDB_NAMESPACE {

using secondary_cache_test_util::GetTestingCacheTypes;
using secondary_cache_test_util::WithCacheType;

// 16 bytes for HCC compatibility
const std::string key0 = "____    ____key0";
const std::string key1 = "____    ____key1";
const std::string key2 = "____    ____key2";
const std::string key3 = "____    ____key3";

class RemoteSecondaryCacheTestBase : public testing::Test,
                                     public WithCacheType {
 public:
  RemoteSecondaryCacheTestBase() {}
  ~RemoteSecondaryCacheTestBase() override = default;

 protected:
  void BasicTestHelper(std::shared_ptr<SecondaryCache> sec_cache) {
    bool kept_in_sec_cache{true};
    // Lookup an non-existent key.
    std::unique_ptr<SecondaryCacheResultHandle> handle0 =
        sec_cache->Lookup(key0, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_EQ(handle0, nullptr);

    Random rnd(301);
    // Insert and Lookup the item k1 for the first time.
    std::string str1(rnd.RandomString(1000));
    TestItem item1(str1.data(), str1.length());
    // A dummy handle is inserted if the item is inserted for the first time.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));

    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);

    // Insert and Lookup the item k1 for the second time and advise erasing it.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));

    std::unique_ptr<SecondaryCacheResultHandle> handle1_2 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_NE(handle1_2, nullptr);
    ASSERT_FALSE(kept_in_sec_cache);

    std::unique_ptr<TestItem> val1 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle1_2->Value()));
    ASSERT_NE(val1, nullptr);
    ASSERT_EQ(memcmp(val1->Buf(), item1.Buf(), item1.Size()), 0);

    // Lookup the item k1 again.
    std::unique_ptr<SecondaryCacheResultHandle> handle1_3 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1_3, nullptr);

    // Insert and Lookup the item k2.
    std::string str2(rnd.RandomString(1000));
    TestItem item2(str2.data(), str2.length());
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));
    std::unique_ptr<SecondaryCacheResultHandle> handle2_1 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle2_1, nullptr);

    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));

    std::unique_ptr<SecondaryCacheResultHandle> handle2_2 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_NE(handle2_2, nullptr);
    std::unique_ptr<TestItem> val2 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle2_2->Value()));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    std::vector<SecondaryCacheResultHandle*> handles = {handle1_2.get(),
                                                        handle2_2.get()};
    sec_cache->WaitAll(handles);

    sec_cache.reset();
  }

  void BasicTest() {
    RemoteSecondaryCacheOptions opts;
    opts.capacity = 2048;
    opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> sec_cache = NewRemoteSecondaryCache(opts);
    BasicTestHelper(sec_cache);
  }

  void FailsTest() {
    RemoteSecondaryCacheOptions remote_cache_opts;

    remote_cache_opts.capacity = 1100;
    remote_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> sec_cache =
        NewRemoteSecondaryCache(remote_cache_opts);

    // Insert and Lookup the first item.
    Random rnd(301);
    std::string str1(rnd.RandomString(1000));
    TestItem item1(str1.data(), str1.length());
    // Insert a dummy handle.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));
    // Insert k1.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));

    // Insert and Lookup the second item.
    std::string str2(rnd.RandomString(200));
    TestItem item2(str2.data(), str2.length());
    // Insert a dummy handle, k1 is not evicted.
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));
    bool kept_in_sec_cache{false};
    std::unique_ptr<SecondaryCacheResultHandle> handle1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1, nullptr);

    // Insert k2 and k1 is evicted.
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper()));
    std::unique_ptr<SecondaryCacheResultHandle> handle2 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_NE(handle2, nullptr);
    std::unique_ptr<TestItem> val2 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle2->Value()));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    // Insert k1 again and a dummy handle is inserted.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper()));

    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          kept_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);

    // Create Fails.
    SetFailCreate(true);
    std::unique_ptr<SecondaryCacheResultHandle> handle2_1 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/true,
                          kept_in_sec_cache);
    ASSERT_EQ(handle2_1, nullptr);

    // Save Fails.
    std::string str3 = rnd.RandomString(10);
    TestItem item3(str3.data(), str3.length());
    // The Status is OK because a dummy handle is inserted.
    ASSERT_OK(sec_cache->Insert(key3, &item3, GetHelperFail()));
    ASSERT_NOK(sec_cache->Insert(key3, &item3, GetHelperFail()));

    sec_cache.reset();
  }

  void BasicIntegrationTest() {
    spdlog::set_level(spdlog::level::debug);
    RemoteSecondaryCacheOptions remote_cache_opts;

    remote_cache_opts.max_value_size = 1500;
    remote_cache_opts.threads = 1;
    remote_cache_opts.addr = "192.168.200.53";
    remote_cache_opts.port = "10086";
    remote_cache_opts.capacity = 6000;
    remote_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewRemoteSecondaryCache(remote_cache_opts);
    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity =*/1300, /*_num_shard_bits =*/0,
        /*_strict_capacity_limit =*/true, secondary_cache);
    std::shared_ptr<Statistics> stats = CreateDBStatistics();
    Cache::Handle* handle;
    auto sec = std::dynamic_pointer_cast<RemoteSecondaryCache>(secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    DEBUG("str1 {}", Slice(str1).ToASCII());
    auto item1_1 = new TestItem(str1.data(), str1.length());
    INFO("---------------------insert key1, 1_1-----------------------------");
    ASSERT_OK(cache->Insert(key1, item1_1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1012);
    DEBUG("str2 {}", Slice(str2).ToASCII());
    auto item2_1 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    INFO("---------------------insert key2, 2_1-----------------------------");
    ASSERT_OK(cache->Insert(key2, item2_1, GetHelper(), str2.length()));

    std::string str3 = rnd.RandomString(1024);
    DEBUG("str3 {}", Slice(str3).ToASCII());
    auto item3_1 = new TestItem(str3.data(), str3.length());
    // After this Insert, primary cache contains k3 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    INFO("---------------------insert key3, 3_1-----------------------------");
    ASSERT_OK(cache->Insert(key3, item3_1, GetHelper(), str3.length()));

    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item, k2's dummy item, and k3's dummy item.
    auto item1_2 = new TestItem(str1.data(), str1.length());
    INFO("---------------------insert key1, 1_2-----------------------------");
    ASSERT_OK(cache->Insert(key1, item1_2, GetHelper(), str1.length()));

    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's item, k2's dummy item, and k3's dummy item.
    INFO("---------------------insert key2, 2_2-----------------------------");
    auto item2_2 = new TestItem(str2.data(), str2.length());
    ASSERT_OK(cache->Insert(key2, item2_2, GetHelper(), str2.length()));

    // After this Insert, primary cache contains k3 and secondary cache contains
    // k1's item and k2's item.
    INFO("---------------------insert key3, 3_2-----------------------------");
    auto item3_2 = new TestItem(str3.data(), str3.length());
    ASSERT_OK(cache->Insert(key3, item3_2, GetHelper(), str3.length()));

    INFO("---------------------lookup key3,    -----------------------------");
    handle = cache->Lookup(key3, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    auto val3 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val3, nullptr);
    ASSERT_EQ(memcmp(val3->Buf(), item3_2->Buf(), item3_2->Size()), 0);
    ASSERT_EQ(0, sec->num_lookups());
    ASSERT_EQ(2, sec->num_inserts());
    cache->Release(handle);

    // Lookup an non-existent key.
    INFO("---------------------lookup key0,    -----------------------------");
    handle = cache->Lookup(key0, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_EQ(handle, nullptr);
    ASSERT_EQ(1, sec->num_lookups());
    ASSERT_EQ(2, sec->num_inserts());

    // This Lookup should just insert a dummy handle in the primary cache
    // and the k1 is still in the secondary cache.
    // With k1 standalone entry, k3 is demoted to secondary cache.
    INFO("---------------------lookup key1,    -----------------------------");
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    auto val1_1 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val1_1, nullptr);
    DEBUG("val1_1 {}", val1_1->Size());
    DEBUG("str1 {}", str1.length());
    ASSERT_EQ(memcmp(val1_1->Buf(), str1.data(), str1.size()), 0);
    ASSERT_EQ(2, sec->num_lookups());
    ASSERT_EQ(3, sec->num_inserts());
    cache->Release(handle);

    // This Lookup should erase k1 from the secondary cache and insert
    // it into primary cache; then k3 is demoted.
    // k2 and k3 are in secondary cache.
    // First remove the k1 dummy entry from primary,
    INFO("---------------------lookup key1,    -----------------------------");
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    ASSERT_EQ(3, sec->num_lookups());
    ASSERT_EQ(3, sec->num_inserts());

    // k2 is still in secondary cache.
    // But k1 must be demoted
    INFO("---------------------lookup key2,    -----------------------------");
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    auto val2 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), str2.data(), str2.length()), 0);
    cache->Release(handle);
    ASSERT_EQ(4, sec->num_lookups());
    ASSERT_EQ(4, sec->num_inserts());

    // Testing SetCapacity().
    ASSERT_OK(secondary_cache->SetCapacity(0));
    INFO("---------------------lookup key3, c 0 -----------------------------");
    handle = cache->Lookup(key3, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_EQ(handle, nullptr);

    ASSERT_OK(secondary_cache->SetCapacity(7000));
    size_t capacity;
    ASSERT_OK(secondary_cache->GetCapacity(capacity));
    ASSERT_EQ(capacity, 7000);
    auto item1_3 = new TestItem(str1.data(), str1.length());
    // After this Insert, primary cache contains k1.
    INFO("---------------------insert key1, 1_3-----------------------------");
    ASSERT_OK(cache->Insert(key1, item1_3, GetHelper(), str2.length()));

    auto item2_3 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    INFO("---------------------insert key2, 2_3-----------------------------");
    ASSERT_OK(cache->Insert(key2, item2_3, GetHelper(), str1.length()));

    auto item1_4 = new TestItem(str1.data(), str1.length());
    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    INFO("---------------------insert key1, 1_4-----------------------------");
    ASSERT_OK(cache->Insert(key1, item1_4, GetHelper(), str2.length()));

    auto item2_4 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's real item and k2's dummy item.
    INFO("---------------------insert key2, 2_4-----------------------------");
    ASSERT_OK(cache->Insert(key2, item2_4, GetHelper(), str2.length()));
    // This Lookup should just insert a dummy handle in the primary cache
    // and the k1 is still in the secondary cache.
    INFO("---------------------lookup key1,    -----------------------------");
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());

    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void BasicIntegrationFailTest() {
    RemoteSecondaryCacheOptions remote_cache_opts;

    remote_cache_opts.capacity = 6000;
    remote_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewRemoteSecondaryCache(remote_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/false, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = std::make_unique<TestItem>(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1.get(), GetHelper(), str1.length()));
    item1.release();  // Appease clang-analyze "potential memory leak"

    Cache::Handle* handle;
    handle = cache->Lookup(key2, nullptr, this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);

    Cache::AsyncLookupHandle ah;
    ah.key = key2;
    ah.helper = GetHelper();
    ah.create_context = this;
    ah.priority = Cache::Priority::LOW;
    cache->StartAsyncLookup(ah);
    cache->Wait(ah);
    ASSERT_EQ(ah.Result(), nullptr);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationSaveFailTest() {
    RemoteSecondaryCacheOptions remote_cache_opts;

    remote_cache_opts.capacity = 6000;
    remote_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewRemoteSecondaryCache(remote_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/true, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1, GetHelperFail(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    auto item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert(key2, item2, GetHelperFail(), str2.length()));

    Cache::Handle* handle;
    handle = cache->Lookup(key2, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 demotion would have failed.
    handle = cache->Lookup(key1, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    // Since k1 was not promoted, k2 should still be in cache.
    handle = cache->Lookup(key2, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationCreateFailTest() {
    RemoteSecondaryCacheOptions remote_cache_opts;

    remote_cache_opts.capacity = 6000;
    remote_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewRemoteSecondaryCache(remote_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/true, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    auto item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert(key2, item2, GetHelper(), str2.length()));

    Cache::Handle* handle;
    SetFailCreate(true);
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 creation would have failed
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    // Since k1 didn't get promoted, k2 should still be in cache
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationFullCapacityTest() {
    RemoteSecondaryCacheOptions remote_cache_opts;

    remote_cache_opts.capacity = 6000;
    remote_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewRemoteSecondaryCache(remote_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/false, secondary_cache);
    CompressedSecondaryCacheOptions secondary_cache_opts;

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1_1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    std::string str2_clone{str2};
    auto item2 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    ASSERT_OK(cache->Insert(key2, item2, GetHelper(), str2.length()));

    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    auto item1_2 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_2, GetHelper(), str1.length()));

    auto item2_2 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's item and k2's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_2, GetHelper(), str2.length()));

    Cache::Handle* handle2;
    handle2 = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);

    // k1 promotion should fail because cache is at capacity and
    // strict_capacity_limit is true, but the lookup should still succeed.
    // A k1's dummy item is inserted into primary cache.
    Cache::Handle* handle1;
    handle1 = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle1, nullptr);
    cache->Release(handle1);

    // Since k1 didn't get inserted, k2 should still be in cache
    handle2 = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);

    cache.reset();
    secondary_cache.reset();
  }
};

class RemoteSecondaryCacheTest
    : public RemoteSecondaryCacheTestBase,
      public testing::WithParamInterface<std::string> {
  const std::string& Type() override { return GetParam(); }
};

INSTANTIATE_TEST_CASE_P(RemoteSecondaryCacheTest, RemoteSecondaryCacheTest,
                        GetTestingCacheTypes());

TEST_P(RemoteSecondaryCacheTest, BasicTes) { BasicTest(); }

TEST_P(RemoteSecondaryCacheTest, FailsTest) { FailsTest(); }

TEST_P(RemoteSecondaryCacheTest, BasicIntegrationTest) {
  if (GetParam() == kHyperClock) {
    ROCKSDB_GTEST_BYPASS("Test depends on LRUCache-specific behaviors");
    return;
  }
  BasicIntegrationTest();
}

TEST_P(RemoteSecondaryCacheTest, BasicIntegrationFailTest) {
  BasicIntegrationFailTest();
}

TEST_P(RemoteSecondaryCacheTest, IntegrationSaveFailTest) {
  IntegrationSaveFailTest();
}

TEST_P(RemoteSecondaryCacheTest, IntegrationCreateFailTest) {
  IntegrationCreateFailTest();
}

TEST_P(RemoteSecondaryCacheTest, IntegrationFullCapacityTest) {
  IntegrationFullCapacityTest();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}