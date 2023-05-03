#include <future>

#include "clickhouse_reflect/ch_connection.hpp"
#include "faststorage.hpp"
#include "gtest/gtest.h"
#include "sql.hpp"

using namespace sqlcpp;
using namespace std::chrono_literals;

struct ch_info {
	std::string ip;
	uint16_t port;
	std::string user;
	std::string passwd;
};

class ch_insert : public testing::TestWithParam<ch_info> {
private:
	std::unique_ptr<ch::ch_connection> ch_conn_;
	const std::string table_name_ = "default.sql_log";
	std::atomic<bool> run_ = true;

protected:
	void SetUp() override {
		auto p = GetParam();
		ch_conn_ = std::make_unique<ch::ch_connection>(
			GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
		ch_conn_->get_raw_conn()->Execute(sql);
	}

	void TearDown() override {
		ch_conn_->get_raw_conn()->Execute("DROP TABLE IF EXISTS default.sql_log");
		ch_conn_.reset();
	}

	template <typename Store>
	auto async_result(Store&& store, size_t expect_value, std::chrono::nanoseconds timeout) {
		auto fu = std::async([this, &store, &expect_value]() {
			while (run_) {
				std::this_thread::sleep_for(1us);
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

TEST_P(ch_insert, insert_performace) {
	constexpr size_t batch = 500000;
	auto store = std::make_shared<
		fast::faststorage<ch::ch_connection, std::unique_ptr<storage_context>, 16>>();
	store->set_batch_commit(batch / 16);
	store->set_timeout_commit(10000);

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
	store->reserve_block(batch, 3, 3);
	constexpr size_t expect_count = batch;
	for (size_t i = 0; i < batch; ++i) {
		store->storage(std::make_unique<storage_context>(), "default.sql_log");
	}
	EXPECT_EQ(expect_count, async_result(store, expect_count, 5s));
};

TEST_P(ch_insert, batch_insert) {
	constexpr size_t batch = 10000;
	auto store =
		std::make_shared<fast::faststorage<ch::ch_connection, std::shared_ptr<storage_context>>>();
	store->set_batch_commit(batch);

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
	store->reserve_block(batch, 5, 3);

	constexpr size_t expect_count = batch;
	auto ptr = std::make_shared<storage_context>();
	for (size_t i = 0; i < batch; ++i) {
		store->storage(ptr, "default.sql_log");
	}

	EXPECT_EQ(expect_count, async_result(store, expect_count, 2000ms));
};

TEST_P(ch_insert, timeout_insert) {
	constexpr size_t batch = 10000;
	auto store =
		std::make_shared<fast::faststorage<ch::ch_connection, std::shared_ptr<storage_context>>>();
	store->set_batch_commit(batch);
	store->set_timeout_commit(1000);

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
	store->reserve_block(batch, 5, 3);

	constexpr size_t expect_count = batch / 2;
	auto ptr = std::make_shared<storage_context>();
	for (size_t i = 0; i < expect_count; ++i) {
		store->storage(ptr, "default.sql_log");
	}

	EXPECT_EQ(expect_count, async_result(store, expect_count, 3000ms));
};

TEST_P(ch_insert, 4_thread_storage) {
	constexpr size_t batch = 100000;
	auto store =
		std::make_shared<fast::faststorage<ch::ch_connection, std::shared_ptr<storage_context>>>();
	store->set_batch_commit(batch);

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
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

	EXPECT_EQ(expect_count, async_result(store, expect_count, 3000ms));
};

TEST_P(ch_insert, 4_thread_insert) {
	constexpr size_t batch = 100000;
	auto store = std::make_shared<
		fast::faststorage<ch::ch_connection, std::unique_ptr<storage_context>, 4>>();
	store->set_batch_commit(batch);

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
	store->reserve_block(batch, 5, 3);

	constexpr size_t expect_count = batch * 4;
	for (size_t i = 0; i < expect_count; ++i) {
		store->storage(std::make_unique<storage_context>(), "default.sql_log");
	}

	EXPECT_EQ(expect_count, async_result(store, expect_count, 10s));
};

TEST_P(ch_insert, 16thread_storage_and_16thread_insert) {
	static constexpr size_t thread_count = 16;
	constexpr size_t batch = 50000;
	auto store = std::make_shared<
		fast::faststorage<ch::ch_connection, std::unique_ptr<storage_context>, thread_count>>();
	store->set_batch_commit(batch);

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
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

	EXPECT_EQ(expect_count, async_result(store, expect_count, 15s));
};

TEST_P(ch_insert, disk_cache_and_4thread_storage_insert) {
	constexpr size_t batch = 100000;
	auto store = std::make_shared<
		fast::faststorage<ch::ch_connection, std::unique_ptr<storage_context>, 4>>();
	constexpr size_t commit_batch = batch / 2;
	store->set_batch_commit(commit_batch);
	store->enable_disk_cache("./disk_cache/");

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
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

	EXPECT_EQ(expect_count, async_result(store, expect_count, 20s));
};

TEST_P(ch_insert, disk_cache_and_disable) {
	// GTEST_SKIP();
	constexpr size_t batch = 100000;
	auto store = std::make_shared<
		fast::faststorage<ch::ch_connection, std::unique_ptr<storage_context>, 2>>();
	constexpr size_t commit_batch = batch / 2;
	store->set_batch_commit(commit_batch);
	store->enable_disk_cache("./disk_cache/");

	store->init_storage(GetParam().ip, GetParam().port, GetParam().user, GetParam().passwd);
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

	ASSERT_GT(expect_count, async_result(store, expect_count, 15s));
};

INSTANTIATE_TEST_SUITE_P(ch_insert_set, ch_insert,
						 ::testing::Values(ch_info{ "172.18.0.3", 9000, "default", "" }));
