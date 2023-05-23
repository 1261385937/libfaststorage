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
/**
 * @brief Parallel-insert is 1 thread by default. Set it to be greater than 1 if
 * you want parallel. Each parallel will has a connection.
 *
 * @tparam StorageEngine can be clickhouse, kafka etc.
 * @tparam StorageType is the type of inserted data.
 * @tparam Parallel is the insert-thread count.
 * different entry
 */
template <typename StorageEngine, typename StorageType, size_t Parallel = 1>
class faststorage {
public:
	using engine_type = typename StorageEngine::engine_type;
	using storage_pair = std::pair<std::string, StorageType>;
	using queue = moodycamel::ConcurrentQueue<storage_pair>;

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

	std::array<task_context, Parallel> task_group_;
	std::atomic<bool> run_ = true;
	std::atomic<uint16_t> cur_index_ = 0;

	// for disk cache
	std::unique_ptr<disk::disk_cache<storage_pair>> disk_cache_ = nullptr;
	std::unique_ptr<StorageEngine> engine_for_disk_;

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
	 * @brief Indicate reserved memeory size for more efficient. Should call
	 * after init_storage.
	 *
	 * @tparam ...Args
	 * @param ...args For clickhouse, the args is batch_commit, row and column
	 */
	template <typename... Args>
	void reserve_block(Args &&...args) {
		using ch_tag = sqlcpp::ch::inner::clickhouse_tag;
		if constexpr (std::is_same_v<engine_type, ch_tag>) {
			for (size_t i = 0; i < Parallel; ++i) {
				task_group_[i].engine->reserve_block(args...);
			}
			engine_for_disk_->reserve_block(std::forward<Args>(args)...);
		}
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
		disk_cache_ = std::make_unique<typename decltype(disk_cache_)::element_type>(std::move(path));
		disk_cache_->subscribe([this](auto&& data) { this->bulk_insert(data, engine_for_disk_); });
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
	 *
	 * @param dest may be url, table or db.table
	 */
	template <typename Datatype, typename String>
	void storage(Datatype&& data, String&& dest) {
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
				disk_cache_->produce(
					storage_pair{ std::forward<String>(dest), std::forward<Datatype>(data) });
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

		q.enqueue(storage_pair{ std::forward<String>(dest), std::forward<Datatype>(data) });
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
			std::vector<storage_pair> bulk;
			bulk.resize(count);
			auto size = q.try_dequeue_bulk(bulk.begin(), count);
			this->bulk_insert(bulk, engine);
			cached_count -= size;
		}

		// for last data
		if (cached_count > 0) {
			auto count = cached_count.load();
			std::vector<storage_pair> bulk;
			bulk.resize(count);
			q.try_dequeue_bulk(bulk.begin(), count);
			this->bulk_insert(bulk, engine);
		}
	}

	template <typename Data, typename E>
	void bulk_insert(Data&& data, E&& engine) {
		using single_queue = std::vector<StorageType>;
		std::unordered_map<std::string, single_queue> mqueue;
		for (auto& [dest, d] : data) {
			auto it = mqueue.find(dest);
			if (it != mqueue.end()) {
				it->second.emplace_back(std::move(d));
				continue;
			}
			single_queue queue;
			queue.reserve(data.size());
			queue.emplace_back(std::move(d));
			mqueue.emplace(std::move(dest), std::move(queue));
		}
		this->real_insert(std::move(mqueue), engine);
	}

	template <typename Q, typename E>
	void real_insert(Q&& mq, E&& engine) {
		for (auto& [dest, queue] : mq) {
			auto size = queue.size();
			auto ok = engine->insert(std::move(queue), dest);
			if (ok) {
				successful_item_ += size;
				printf("successful_item_:%llu\n", size);
			}
			else {
				failed_item_ += size;
				printf("failed_item_:%llu\n", failed_item_.load());
			}
		}
	};
};

}  // namespace fast
