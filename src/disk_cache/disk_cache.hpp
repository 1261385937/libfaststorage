#pragma once

#include <atomic>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "serialize.hpp"

namespace disk {

template<typename T>
concept sequence_container = requires (T t, std::size_t n) {
	{ t.begin() } ->std::convertible_to<decltype(t.begin())>;
	{ t.end() } ->std::convertible_to<decltype(t.end())>;
	{ t.resize(n) } ->std::same_as<void>;
};

template<typename T>
concept not_sequence_container = !sequence_container<T>;

template <typename T, typename U = void>
struct has_size : std::false_type {};
template <typename T>
struct has_size<T, std::enable_if_t<std::is_member_function_pointer_v<decltype(&T::size)>>>
	: std::true_type {};
template <typename T>
constexpr auto has_fun_size_v = has_size<T>::value;

constexpr size_t to_disk_count_threshold = 100000;
constexpr size_t to_disk_times_threshold = 300;

template <typename CacheType, typename Serialize = serialize::serializer>
class disk_cache {
public:
	using cache_type = std::deque<CacheType>;
	using sub_cb = std::function<void(cache_type&&)>;

private:
	Serialize serializer_;
	sub_cb cb_;
	std::string dir_;

	std::thread write_disk_thread_;
	std::atomic<bool> write_run_ = true;
	std::atomic<uint64_t> write_file_index_ = 0;

	std::thread read_disk_thread_;
	std::atomic<bool> read_run_ = true;
	uint64_t read_file_index_ = 0;

	std::mutex caches_mtx_;
	cache_type caches_;
	std::atomic<uint64_t> caches_count_ = 0;
	std::atomic<uint64_t> times_ = 0;
	inline static uint64_t serialize_count_ = 0;
	inline static uint64_t deserialize_count_ = 0;

public:
	disk_cache(std::string unique_dir) : dir_(std::move(unique_dir)) {
		std::set<uint64_t, std::greater<uint64_t>> file_index;
		std::filesystem::create_directories(dir_);  // no matter exist or not
		for (auto& p : std::filesystem::directory_iterator(dir_)) {
			if (!std::filesystem::is_regular_file(p)) {
				continue;
			}
			auto file_name = p.path().filename().string();
			auto number = file_name.substr(0, file_name.length() - 4);  // "22.dat"
			file_index.emplace(std::atoi(number.data()));
		}

		if (!file_index.empty()) {
			// has unsolved *.dat file, maybe restart cause
			auto it = file_index.begin();
			write_file_index_ = *it + 1;
			read_file_index_ = *file_index.rbegin();
		}

		setup_write_thread();
		setup_read_thread();
	}

	~disk_cache() {
		write_run_ = false;
		if (write_disk_thread_.joinable()) {
			write_disk_thread_.join();
		}
		read_run_ = false;
		if (read_disk_thread_.joinable()) {
			read_disk_thread_.join();
		}
	}

	template <typename T>
		requires not_sequence_container<T>
	void produce(T&& cache) {
		std::lock_guard lock(caches_mtx_);
		caches_.emplace_back(std::forward<T>(cache));
		caches_count_++;
	}

	template <typename T>
	void produce(T&& cache, size_t size) {
		std::lock_guard lock(caches_mtx_);
		caches_.emplace_back(std::forward<T>(cache));
		caches_count_ += size;
	}

	template<typename T> 
		requires sequence_container<T>
	void produce(T&& cache) {
		constexpr auto rvalue = std::is_rvalue_reference_v<decltype(cache)>;
		auto size = cache.size();

		std::lock_guard lock(caches_mtx_);
		if constexpr (rvalue) {
			caches_.insert(caches_.end(),
				std::make_move_iterator(cache.begin()), std::make_move_iterator(cache.end()));
		}
		else {
			caches_.insert(caches_.end(), cache.begin(), cache.end());
		}
		caches_count_ += size;
	}

	void subscribe(sub_cb cb) {
		cb_ = std::move(cb);
	}

private:
	void setup_write_thread() {
		write_disk_thread_ = std::thread([this]() {
			while (write_run_) {
				if (caches_count_ < to_disk_count_threshold) {
					// do not need cv notify, a little latency is good for
					// writing disk
					using namespace std::literals;
					std::this_thread::sleep_for(5ms);
					if (times_ < to_disk_times_threshold) {
						times_++;
						continue;
					}
				}
				times_ = 0;

				// need to write disk
				std::unique_lock lock(caches_mtx_);
				if (caches_.empty()) {
					// maybe has no data if catch to_disk_times_threshold
					continue;
				}
				auto caches = std::move(caches_);
				// printf("caches_count_: %llu\n", caches_count_.load());
				caches_count_ = 0;
				lock.unlock();

				serialize_count_ += caches.size();
				// printf("serialize count: %llu, this :%llu\n",
				// serialize_count_, caches.size());

				auto seria_obj = serializer_.serialize(caches);
				auto data = seria_obj.data();
				auto len = seria_obj.size();
				auto file_name = dir_ + "/" + std::to_string(write_file_index_.load()) + ".dat";
				auto file = fopen(file_name.c_str(), "wb");
				fwrite(data, len, 1, file);
				fclose(file);
				write_file_index_ += 1;
				// printf("write file: %s  %llu \n", file_name.data(), len);
			}
		});
	}

	void setup_read_thread() {
		read_disk_thread_ = std::thread([this]() {
			while (read_run_ || (write_file_index_ != read_file_index_)) {
				if (write_file_index_ == read_file_index_) {
					// has no cache file to read.
					using namespace std::literals;
					std::this_thread::sleep_for(1ms);
					continue;
				}

				// need to read disk
				auto file_name = dir_ + "/" + std::to_string(read_file_index_) + ".dat";
				auto buf_size = std::filesystem::file_size(file_name);
				std::string disk_buf;
				disk_buf.resize(buf_size);

				auto file = fopen(file_name.c_str(), "rb");
				fread(disk_buf.data(), buf_size, 1, file);
				fclose(file);
				// printf("remove file: %s %llu\n", file_name.data(), buf_size);
				std::filesystem::remove(file_name);
				// callback
				try {
					auto r =
						serializer_.template deserialize<cache_type>(disk_buf.data(), disk_buf.length());
					deserialize_count_ += r.size();
					cb_(std::move(r));
					// printf("deserialize count: %llu, this :%llu\n",
					// deserialize_count_, r.size());
				}
				catch (const std::exception& e) {
					printf("deserialize failed: %s\n", e.what());
				}
				read_file_index_ += 1;
			}
		});
	}
};

}  // namespace disk
