#include <future>

#include "clickhouse_reflect/ch_cluster_connection.hpp"
#include "faststorage.hpp"
#include "gtest/gtest.h"
#include "sql.hpp"

using namespace sqlcpp;
using namespace std::chrono_literals;

class ch_cluster_insert_ut : public testing::TestWithParam<std::vector<ch::cluster::ch_shard>> {
public:
    std::vector<std::unique_ptr<ch::ch_connection>> ch_conns_;
    const std::string table_name_ = "default.sql_log";
    std::atomic<bool> run_ = true;

protected:
    void SetUp() override {
        for (auto& ch_shard : GetParam()) {
            for (auto& replica : ch_shard.replicas) {
                auto ch_conn = std::make_unique<ch::ch_connection>(replica.ip, replica.port,
                                                                   replica.user, replica.passwd);
                ch_conn->get_raw_conn()->Execute("DROP TABLE IF EXISTS default.sql_log");
                ch_conn->get_raw_conn()->Execute(sql);
                ch_conns_.emplace_back(std::move(ch_conn));
            }
        }
    }

    void TearDown() override {
        for (auto& ch_conn : ch_conns_) {
            ch_conn->get_raw_conn()->Execute("DROP TABLE IF EXISTS default.sql_log");
            ch_conn.reset();
        }
    }

    template <typename Store>
    auto async_result(Store&& store, size_t expect_value, std::chrono::milliseconds timeout) {
        auto fu = std::async([this, &store, &expect_value]() {
            while (run_) {
                std::this_thread::sleep_for(1ms);
                if (store->storage_statistics().success == expect_value) {
                    break;
                }
            }
            return store->storage_statistics().success;
        });

        fu.wait_for(timeout);
        run_ = false;
        return fu.get();
    }
};

TEST_P(ch_cluster_insert_ut, load_balance) {
    constexpr size_t batch = 100000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::unique_ptr<storage_context>>>();
    constexpr auto commit_batch = batch / 10;
    store->set_batch_commit(batch / 10);
    store->set_timeout_commit(10000);

    store->init_storage(GetParam());
    store->reserve_block(batch, 3, 3);

    constexpr size_t expect_count = batch;
    for (size_t i = 1; i <= batch; ++i) {
        store->storage(std::make_unique<storage_context>(), "default.sql_log");
        if (i % commit_batch == 0) {
            std::this_thread::sleep_for(500ms);
        }
    }
    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));

    size_t count1 = 0;
    ch_conns_[0]->get_raw_conn()->Select(
        "select count(*) from default.sql_log", [&count1](const clickhouse::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            count1 += (*block[0]->As<clickhouse::ColumnUInt64>())[i];
        }
    });
    EXPECT_EQ(count1, batch / 10 * 6);

    size_t count2 = 0;
    ch_conns_[2]->get_raw_conn()->Select(
        "select count(*) from default.sql_log", [&count2](const clickhouse::Block& block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
            count2 += (*block[0]->As<clickhouse::ColumnUInt64>())[i];
        }
    });
    EXPECT_EQ(count2, batch / 10 * 4);
};

TEST_P(ch_cluster_insert_ut, cluster_hot_rebuild) {
    constexpr size_t batch = 100000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::unique_ptr<storage_context>>>();
    constexpr auto commit_batch = batch / 10;
    store->set_batch_commit(batch / 10);
    store->set_timeout_commit(10000);

    store->init_storage(GetParam());
    store->reserve_block(batch, 3, 3);

    // change priority
    auto new_info = std::vector<ch::cluster::ch_shard>{
        {std::multiset<ch::cluster::ch_replica>{
             ch::cluster::ch_replica{"172.18.0.3", (uint16_t)9000, "default", "", 2},
             ch::cluster::ch_replica{"172.18.0.4", (uint16_t)9000, "default", "", 1}},
         6},
        {std::multiset<ch::cluster::ch_replica>{
             ch::cluster::ch_replica{"172.18.0.5", (uint16_t)9000, "default", "", 9},
             ch::cluster::ch_replica{"172.18.0.6", (uint16_t)9000, "default", "", 3},
         },
         4}
    };
    std::thread([store, &new_info]() {
        std::this_thread::sleep_for(1500ms);
        store->update_servers(new_info);
    }).detach();

    constexpr size_t expect_count = batch;
    for (size_t i = 1; i <= batch; ++i) {
        store->storage(std::make_unique<storage_context>(), "default.sql_log");
        if (i % commit_batch == 0) {
            std::this_thread::sleep_for(500ms);
        }
    }
    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, insert_performace) {
    constexpr size_t batch = 400000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::unique_ptr<storage_context>>>();
    store->set_batch_commit(batch);
    store->set_timeout_commit(10000);

    store->init_storage(GetParam());
    store->reserve_block(batch, 3, 3);

    constexpr size_t expect_count = batch;
    for (size_t i = 1; i <= batch; ++i) {
        store->storage(std::make_unique<storage_context>(), "default.sql_log");
    }
    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, batch_insert) {
    constexpr size_t batch = 10000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::shared_ptr<storage_context>>>();
    store->set_batch_commit(batch);

    store->init_storage(GetParam());
    store->reserve_block(batch, 3, 3);

    constexpr size_t expect_count = batch;
    auto ptr = std::make_shared<storage_context>();
    for (size_t i = 0; i < batch; ++i) {
        store->storage(ptr, "default.sql_log");
    }

    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, timeout_insert) {
    constexpr size_t batch = 10000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::shared_ptr<storage_context>>>();
    store->set_batch_commit(batch);
    store->set_timeout_commit(1000);

    store->init_storage(GetParam());
    store->reserve_block(batch, 5, 3);

    constexpr size_t expect_count = batch / 2;
    auto ptr = std::make_shared<storage_context>();
    for (size_t i = 0; i < expect_count; ++i) {
        store->storage(ptr, "default.sql_log");
    }

    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, 4_thread_storage) {
    constexpr size_t batch = 10000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::shared_ptr<storage_context>>>();
    store->set_batch_commit(batch);

    store->init_storage(GetParam());
    store->reserve_block(batch, 5, 3);

    static constexpr size_t expect_count = batch;
    for (size_t i = 0; i < 4; ++i) {
        std::thread([&store]() {
            constexpr size_t insert_count = expect_count / 4;
            auto ptr = std::make_shared<storage_context>();
            for (size_t i = 0; i < insert_count; ++i) {
                store->storage(ptr, "default.sql_log");
            }
        }).detach();
    }

    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, 4_thread_insert) {
    constexpr size_t batch = 10000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::unique_ptr<storage_context>, 4>>();
    store->set_batch_commit(batch);

    store->init_storage(GetParam());
    store->reserve_block(batch, 5, 3);

    constexpr size_t expect_count = batch * 4;
    for (size_t i = 0; i < expect_count; ++i) {
        store->storage(std::make_unique<storage_context>(), "default.sql_log");
    }

    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, 16thread_storage_and_16thread_insert) {
    static constexpr size_t thread_count = 16;
    constexpr size_t batch = 10000;
    auto store =
        std::make_shared<fast::faststorage<ch::ch_cluster_connection,
        std::unique_ptr<storage_context>, thread_count>>();
    store->set_batch_commit(batch);

    store->init_storage(GetParam());
    store->reserve_block(batch, 5, 3);

    static constexpr size_t expect_count = batch * thread_count;
    for (size_t i = 0; i < thread_count; ++i) {
        std::thread([&store]() {
            constexpr size_t insert_count = expect_count / thread_count;
            for (size_t i = 0; i < insert_count; ++i) {
                store->storage(std::make_unique<storage_context>(), "default.sql_log");
            }
        }).detach();
    }

    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, disk_cache_and_4thread_storage_insert) {
    constexpr size_t batch = 10000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::unique_ptr<storage_context>, 4>>();
    constexpr size_t commit_batch = batch / 2;
    store->set_batch_commit(commit_batch);
    store->enable_disk_cache("./disk_cache/");

    store->init_storage(GetParam());
    store->reserve_block(batch, 5, 3);

    static constexpr size_t expect_count = batch * 4;
    for (size_t i = 0; i < 4; ++i) {
        std::thread([&store]() {
            constexpr size_t insert_count = expect_count / 4;
            for (size_t i = 0; i < insert_count; ++i) {
                store->storage(std::make_unique<storage_context>(), "default.sql_log");
            }
        }).detach();
    }

    EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_cluster_insert_ut, disk_cache_and_disable) {
    // GTEST_SKIP();
    constexpr size_t batch = 100000;
    auto store = std::make_shared<
        fast::faststorage<ch::ch_cluster_connection, std::unique_ptr<storage_context>, 2>>();
    constexpr size_t commit_batch = batch / 2;
    store->set_batch_commit(commit_batch);
    store->enable_disk_cache("./disk_cache/");

    store->init_storage(GetParam());
    store->reserve_block(batch, 5, 3);

    std::thread([&store]() {
        std::this_thread::sleep_for(100ms);
        store->disable_disk_cache();
    }).detach();

    static constexpr size_t expect_count = batch * 4;
    for (size_t i = 0; i < 4; ++i) {
        std::thread([&store]() {
            constexpr size_t insert_count = expect_count / 4;
            for (size_t i = 0; i < insert_count; ++i) {
                store->storage(std::make_unique<storage_context>(), "default.sql_log");
            }
        }).detach();
    }

    ASSERT_GT(expect_count, async_result(store, expect_count, 10s));
};

INSTANTIATE_TEST_SUITE_P(ch_cluster_insert_set, ch_cluster_insert_ut, ::testing::Values(
    std::vector<ch::cluster::ch_shard>
{
    {
        {
            { "172.18.0.3", (uint16_t)9000, "default", "", 1 },
            { "172.18.0.4", (uint16_t)9000, "default", "", 2 }
        },
            6
    },
    {
        {
            {"172.18.0.5", (uint16_t)9000, "default", "", 3},
            {"172.18.0.6", (uint16_t)9000, "default", "", 9},
        },
        4
    }
}));