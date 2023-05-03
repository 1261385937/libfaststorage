#pragma once
#include <algorithm>
#include <atomic>
#include <set>
#include <shared_mutex>
#include <vector>

#include "ch_connection.hpp"

namespace sqlcpp::ch {

namespace cluster {
struct ch_replica {
	std::string ip;
	uint16_t port = 9000;
	std::string user = "";
	std::string passwd = "";
	// higher priority with smaller number
	uint32_t priority = 1;
	bool operator<(const ch_replica& c) const {
		return this->priority < c.priority;
	}
};
struct ch_shard {
	std::multiset<ch_replica> replicas;
	uint32_t weight = 1;
};
}  // namespace cluster

/**
 * @brief Each shard just has one connection which connect to the lowest
 * priority replica node.
 */
class ch_cluster_connection {
public:
	using engine_type = inner::clickhouse_tag;

	struct shard_conn {
		std::shared_ptr<ch_connection> conn;
		uint64_t inserted_count;
		cluster::ch_shard shard;
		int index = 0;  // for remove this shard
	};

private:
	std::atomic<uint64_t> cluster_version_{ 0 };
	std::vector<shard_conn> shard_conns_;
	std::shared_mutex shard_conns_mtx_;

	std::atomic<size_t> count_ = 200000;
	std::atomic<size_t> row_ = 5;
	std::atomic<size_t> column_ = 5;

public:
	/**
	 * @brief Make the cluster connections
	 * @param cluster_info The cluster infomation, shard and replica should be
	 * prepared by user(we can not deal with this)
	 */
	ch_cluster_connection(std::vector<cluster::ch_shard> cluster_info) {
		update_servers(std::move(cluster_info));
	}

	/**
	 * @brief For clickhouse cluster change, redo the cluster connection.
	 * @param cluster_info The new cluster info, shard and replica should be
	 * prepared by user(we can not deal with this)
	 */
	void update_servers(std::vector<cluster::ch_shard> cluster_info) {
		if (cluster_info.empty()) {
			throw std::logic_error("cluster_info can not be empty");
		}

		int index = 1;
		std::vector<shard_conn> temp;
		for (auto& shard : cluster_info) {
			if (shard.replicas.empty()) {
				throw std::logic_error("replicas can not be empty");
			}
			auto conn = make_shard_connection(shard);
			if (conn != nullptr) {
				shard_conn sc{};
				sc.shard = std::move(shard);
				sc.conn = std::move(conn);
				sc.index = index;
				temp.emplace_back(std::move(sc));
				index++;
			}
		}

		std::unique_lock lock(shard_conns_mtx_);
		shard_conns_ = std::move(temp);
		lock.unlock();
		cluster_version_++;
	}

	void reserve_block(size_t commit_count, size_t row, size_t column) {
		count_ = commit_count;
		row_ = row;
		column_ = column;
	}

	template <typename DataType>
	bool insert(DataType&& data, const std::string& db_table) {
		thread_local std::atomic<uint64_t> info_version = 0;
		thread_local decltype(shard_conns_) now_shard_conns_;
		if (info_version != cluster_version_) {  // cluster change
			std::shared_lock shared_lock(shard_conns_mtx_);
			now_shard_conns_ = shard_conns_;
			shared_lock.unlock();
			info_version = cluster_version_.load();
		}

		auto data_count = data.size();
		auto& sc = choose_shard_conn(now_shard_conns_);
		// count_, row_, column_ may be changed
		sc.conn->reserve_block(count_, row_, column_);
		if (sc.conn->insert(std::forward<DataType>(data), db_table)) {
			sc.inserted_count += data_count;
			// for test load_balance
			/* printf("%s, %llu\n", sc.shard.replicas.begin()->ip.c_str(),
					sc.inserted_count);*/
			return true;
		}

		// The replica node may be broken, so remove it.
		// If this shard has no replica node, then remove the shard.
		auto broken_replica_it = sc.shard.replicas.begin();
		sc.shard.replicas.erase(broken_replica_it);
		if (sc.shard.replicas.empty()) {
			auto it = std::find_if(now_shard_conns_.begin(), now_shard_conns_.end(),
							 [index = sc.index](const shard_conn& sc) { return index == sc.index; });
			now_shard_conns_.erase(it);
			return false;
		}
		// Get another connection
		auto conn = make_shard_connection(sc.shard);
		if (!conn) {
			return false;
		}
		sc.conn = std::move(conn);
		return sc.conn->insert(std::forward<DataType>(data), db_table);
	}

private:
	/**
	 * @brief Get a connection of minimum data replica node
	 * @param shard_conns All shard connections
	 * @return
	 */
	shard_conn& choose_shard_conn(std::vector<shard_conn>& shard_conns) {
		// Get a minimum data replica node, std::sort is not good here
		auto it = std::min_element(shard_conns.begin(), shard_conns.end(),
								   [](const shard_conn& sc1, const shard_conn& sc2) {
			auto density1 = sc1.inserted_count / sc1.shard.weight;
			auto density2 = sc2.inserted_count / sc2.shard.weight;
			return density1 < density2;
		});
		if (it == shard_conns.end()) {
			// all cluster node gone, almost impossble
			throw std::runtime_error("can not find cluster connection");
		}
		return *it;
	}

	/**
	 * @brief Do connection and remove broken replica node
	 * @param shard A ch shard info
	 * @return The health conn or nullptr if all replica node gone
	 */
	std::shared_ptr<ch_connection> make_shard_connection(cluster::ch_shard& shard) {
		while (!shard.replicas.empty()) {
			// lowest priority
			auto it = shard.replicas.begin();
			try {
				auto conn = std::make_shared<ch_connection>(
					(*it).ip, (*it).port, (*it).user, (*it).passwd);
				return conn;
			}
			catch (const std::exception& e) {
				printf("make ch connection failed: %s\n, remove the bad replica node(ip:%s, port:%d)",
					e.what(), (*it).ip.c_str(), (*it).port);
				shard.replicas.erase(it);
			}
		}
		return {};
	}
};

}  // namespace sqlcpp::ch