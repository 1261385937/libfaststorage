#pragma once
#include <algorithm>
#include <atomic>
#include <shared_mutex>
#include <vector>

#include "ch_cluster.hpp"
#include "ch_connection.hpp"

namespace sqlcpp::ch {

/**
 * @brief Each shard just has one connection which connect to the lowest
 * priority replica node.
 */
class ch_cluster_connection {
public:
    using engine_type = inner::clickhouse_tag;

    struct shard_conn {
        cluster::ch_shard shard; //original shard info

        int index = 0;  // for remove this shard
        std::shared_ptr<ch_connection> conn;
        uint64_t handled_count = 0;
    };

private:
    std::atomic<uint64_t> cluster_version_{ 0 };
    std::vector<shard_conn> shard_conns_;
    std::shared_mutex shard_conns_mtx_;

    std::atomic<size_t> count_ = 0;
    std::atomic<size_t> row_ = 0;
    std::atomic<size_t> column_ = 0;

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
        thread_local size_t insert_times = 0; //every

        if (info_version != cluster_version_) {  // cluster change
            std::shared_lock shared_lock(shard_conns_mtx_);
            now_shard_conns_ = shard_conns_;
            shared_lock.unlock();
            info_version = cluster_version_.load();
        }

        auto data_count = data.size();
        auto& sc = choose_shard_conn(now_shard_conns_);
        for (;;) {
            if (sc.conn->insert(std::forward<DataType>(data), db_table)) {
                sc.handled_count += data_count;
                insert_times++;
                return true;
            }

            // Try to get another replica connection with same shard
            auto conn = make_shard_connection(sc.shard);
            if (conn) {
                sc.conn = std::move(conn);
                continue;
            }
            if (sc.shard.replicas.empty()) {
                // If this shard has no replica node, remove the shard.  
                auto it = std::find_if(now_shard_conns_.begin(), now_shard_conns_.end(),
                    [index = sc.index](const shard_conn& sc) { return index == sc.index; });
                now_shard_conns_.erase(it);
            }
            
            // Get the next shard connection, if no shard found, it will throw an exception.
            sc = choose_shard_conn(now_shard_conns_);
        }
    }

private:
    /**
     * @brief Get a replica node connection by roundrobin.
     * @param shard_conns All shard connections
     * @return
     */
    shard_conn& choose_shard_conn(std::vector<shard_conn>& shard_conns) {
        if (shard_conns.empty()) { // All cluster node gone, almost impossble
            throw std::runtime_error("All clickhouse node gone, can not get any connection");
        }
        
        auto it = std::min_element(shard_conns.begin(), shard_conns.end(),
            [](const shard_conn& sc1, const shard_conn& sc2) {
            auto density1 = sc1.handled_count / sc1.shard.weight;
            auto density2 = sc2.handled_count / sc2.shard.weight;
            return density1 < density2;
        });
        return *it;
    }

    /**
     * @brief Do connection and remove broken replica node
     * if continuous failed times reach max_connect_failed
     * @param shard A ch shard info
     * @return The health conn or nullptr if all replica node gone
     */
    std::shared_ptr<ch_connection> make_shard_connection(cluster::ch_shard& shard) {
        for (auto it = shard.replicas.begin(); it != shard.replicas.end();) {
            try {
                auto conn = std::make_shared<ch_connection>(it->ip, it->port, it->user, it->passwd);
                conn->reserve_block(count_, row_, column_);
                it->continuous_connect_failed = 0;
                return conn;
            }
            catch (const std::exception& e) {
                it->continuous_connect_failed++;
                if (it->continuous_connect_failed >= it->max_connect_failed) {
                    // Continuous failed times reach max_connect_failed. Remove this replica.
                    it = shard.replicas.erase(it);
                }
                else {
                    ++it;
                }

                printf("make replica(ip:%s, port:%d) connection failed: %s\n",
                    it->ip.c_str(), it->port, e.what());
            }
        }
        return {};
    }
};

}  // namespace sqlcpp::ch