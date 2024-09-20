#pragma once
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "concurrentqueue.h"

#include "disk_cache/disk_cache.hpp"
#include "reflection/reflect_meta.hpp"

#ifndef _WIN32
#include <sys/prctl.h>
#endif

namespace sqlcpp::ch::inner {
struct clickhouse_tag;
}

namespace fast {

template<typename T>
concept clickhouse_concept = std::is_same_v<
	sqlcpp::ch::inner::clickhouse_tag, typename T::engine_type>;


/**
 * @brief Parallel-insert is 1 thread by default. Set it to be greater than 1 if
 * you want parallel. Each parallel will has a connection.
 *
 * @tparam StorageEngine can be ch_connection, ch_cluster_connection etc.
 * @tparam StorageType is the type of inserted data.
 * @tparam Parallel is the insert-thread count.
 * different entry
 */
template <typename StorageEngine, typename StorageType, size_t Parallel = 1>
class faststorage {
public:
	using engine_type = typename StorageEngine::engine_type;
	using storage_pair = std::pair<std::string, StorageType>;
	using queue = moodycamel::ConcurrentQueue<StorageType>;

	struct task_context {
		std::thread thd;
		std::unique_ptr<StorageEngine> engine;
		std::condition_variable cv;
		std::mutex mtx;  // just For cv
		queue q;
		std::atomic<uint64_t> cached_count = 0;
	};

private:
	std::atomic<uint64_t> batch_commit_{ 100000 };
	std::atomic<uint32_t> timeout_commit_{ 5000 };
	std::string dest_;

	std::array<task_context, Parallel> task_group_;
	std::atomic<bool> run_ = true;
	std::atomic<uint16_t> cur_index_ = 0;

	// for disk cache
	std::unique_ptr<disk::disk_cache<StorageType>> disk_cache_;
	std::unique_ptr<StorageEngine> engine_for_disk_;
	static constexpr int max_retry_ = 10;

	// for statistics
	std::atomic<uint64_t> total_item_{ 0 };
	std::atomic<uint64_t> successful_item_{ 0 };
	std::atomic<uint64_t> failed_item_{ 0 };
	std::atomic<uint64_t> drop_item_{ 0 };

public:
	~faststorage() {
		run_ = false;
		for (auto& tg : task_group_) {
			tg.cv.notify_one();
			if (tg.thd.joinable()) {
				tg.thd.join();
			}
		}
	}

	/**
	 * @brief Make connections and setup work thread.
	 * @tparam ...Args
	 * @param ...args for clickhouse, the args is ip, port and passwd
	 */
	template <typename... Args>
	void init_storage(Args &&...args) {
		for (size_t i = 0; i < Parallel; ++i) {
			task_group_[i].thd = std::thread(&faststorage::do_task, this, i);
			task_group_[i].engine = std::make_unique<StorageEngine>(args...);
		}
		engine_for_disk_ = std::make_unique<StorageEngine>(std::forward<Args>(args)...);
	}

	/**
	 * @brief
	 * @param dest Table. Ex: default.sql_log
	 */
	void set_insert_destination(std::string_view dest) {
		dest_ = dest;
	}

	/**
	 * @brief If servers change, need to rebuild the connections
	 * @tparam ...Args
	 * @param ...args New servers info
	 */
	template <typename... Args>
	void update_servers(Args &&...args) {
		for (size_t i = 0; i < Parallel; ++i) {
			task_group_[i].engine->update_servers(args...);
		}
		engine_for_disk_->update_servers(std::forward<Args>(args)...);
	}

	/**
	 * @brief Thread-safe. Set max-cache count before sending to server.
	 * @param count
	 */
	void set_batch_commit(uint64_t count) {
		batch_commit_ = count;
	}

	/**
	 * @brief Thread-safe. Set timeout (millisecond) before sending to server.
	 * @param timeout_ms
	 */
	void set_timeout_commit(uint32_t timeout_ms) {
		timeout_commit_ = timeout_ms;
	}

	/**
	 * @brief This func means data may be lost, but more efficient for storage.
	 * Default is disabled
	 */
	void disable_disk_cache() {
		disk_cache_.reset();
	}

	/**
	 * @brief The data will be written to disk if too much data for sever now.
	 * All disk data can be sure to send to sever.
	 *
	 * @param path must be unique path for multi-process
	 */
	void enable_disk_cache(std::string path) {
		if (disk_cache_) {
			return;
		}
		disk_cache_ = std::make_unique<typename decltype(disk_cache_)::element_type>(std::move(path));
		disk_cache_->subscribe([this](auto&& data) {
			this->bulk_insert<true>(std::move(data), engine_for_disk_);
		});
	}

	/**
	 * @brief The statistics metrics. Include total, success, failed and drop.
	 * @return struct of statistics
	 */
	auto storage_statistics() {
		struct statistics {
			uint64_t total;
			uint64_t success;
			uint64_t failed;
			uint64_t drop;
		};

		statistics s{};
		s.total = total_item_.load();
		s.success = successful_item_.load();
		s.failed = failed_item_.load();
		s.drop = drop_item_.load();
		return s;
	}

	/**
	 * @brief Thread-safe. Data will be written into memory, then send to server
	 * if catch batch-count or timeout.
	 *
	 * @tparam Datatype
	 * @tparam String
	 * @param data type should be same as StorageType, or comile error.
	 * Smart-ptr is necessary for clickhouse.
	 */
	template <typename Datatype> 
		requires clickhouse_concept<StorageEngine>
	void storage(Datatype&& data) {
		if constexpr (!std::is_same_v<StorageType, std::decay_t<Datatype>>) {
			static_assert(reflection::always_false_v<Datatype>, "Datatype not match StorageType");
			using ch_tag = sqlcpp::ch::inner::clickhouse_tag;
			if constexpr (std::is_same_v<engine_type, ch_tag>) {
				static_assert(reflection::is_std_sharedptr_v<Datatype>,
					"clickhouse storage must use smart_ptr for high performance");
			}
		}

		total_item_++;
		if (!has_workable()) {
			if (disk_cache_) {  // need disk cache
				disk_cache_->produce(std::forward<Datatype>(data));
			}
			else {
				drop_item_++;
			}
			return;
		}

		// dispatch storage
		auto index = cur_index_.load();
		auto& q = task_group_[index].q;
		auto& cached_count = task_group_[index].cached_count;
		auto& cv = task_group_[index].cv;

		q.enqueue(std::forward<Datatype>(data));
		cached_count++;
		if (cached_count >= batch_commit_.load()) {
			cv.notify_one();
		}
	}

private:
	bool has_workable() {
		if (task_group_[cur_index_].cached_count < batch_commit_) {
			return true;
		}

		if constexpr (Parallel > 1) {
			for (size_t i = 0; i < Parallel; ++i) {
				if (task_group_[i].cached_count < batch_commit_) {
					cur_index_ = static_cast<uint16_t>(i);
					return true;
				}
			}
		}
		return false;
	}

	void do_task(size_t index) {
#ifndef _WIN32
		prctl(PR_SET_NAME, "storage");
#endif
		auto& cv = task_group_[index].cv;
		auto& q = task_group_[index].q;
		auto& mtx = task_group_[index].mtx;
		auto& engine = task_group_[index].engine;
		auto& cached_count = task_group_[index].cached_count;

		while (run_) {
			{
				std::unique_lock lock(mtx);
				cv.wait_for(lock, std::chrono::milliseconds(timeout_commit_.load()));
			}
			if (cached_count == 0) {
				continue;
			}

			auto count = cached_count.load();
			std::vector<StorageType> bulk;
			bulk.resize(count);
			auto size = q.try_dequeue_bulk(bulk.begin(), count);
			this->bulk_insert<false>(std::move(bulk), engine);
			cached_count -= size;
		}

		// for last data
		if (cached_count > 0) {
			auto count = cached_count.load();
			std::vector<StorageType> bulk;
			bulk.resize(count);
			q.try_dequeue_bulk(bulk.begin(), count);
			this->bulk_insert<false>(std::move(bulk), engine);
		}
	}

	template <bool DiskCache, typename Data, typename E>
	void bulk_insert(Data&& data, E&& engine) {
		auto size = data.size();
		if constexpr (DiskCache) {
			for (int i = 0; i < max_retry_; i++) {
				auto ok = engine->insert(data, dest_);
				if (ok) {
					successful_item_ += size;
					return;
				}
				std::this_thread::sleep_for(std::chrono::seconds(i + 1));
			}
			failed_item_ += size;
		}
		else {	
			auto ok = engine->insert(data, dest_);
			if (ok) {
				successful_item_ += size;
				return;
			}
			if (disk_cache_) {
				disk_cache_->produce(std::move(data));
				return;
			}
			failed_item_ += size;
		}
	}
};

}  // namespace fast
