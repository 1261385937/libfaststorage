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
        float disk_usage = 0.0f;
    };

private:
    std::atomic<uint64_t> cluster_version_{ 0 };
    std::vector<shard_conn> shard_conns_;
    std::shared_mutex shard_conns_mtx_;

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
            if (conn == nullptr) {
                continue;
            }

            shard_conn sc{};
            sc.shard = std::move(shard);
            sc.conn = std::move(conn);
            sc.index = index;
            temp.emplace_back(std::move(sc));
            index++;
        }
        update_shard_disk_usage(temp);

        std::unique_lock lock(shard_conns_mtx_);
        shard_conns_ = std::move(temp);
        lock.unlock();
        cluster_version_++;
    }

    template <typename DataType>
    bool insert(DataType&& data, const std::string& db_table) {
        thread_local std::atomic<uint64_t> info_version = 0;
        thread_local decltype(shard_conns_) now_shard_conns_;
        thread_local size_t insert_times = 0; //Check shard disk usage per 10000 times

        if (info_version != cluster_version_) {  // cluster change
            std::shared_lock shared_lock(shard_conns_mtx_);
            now_shard_conns_ = shard_conns_;
            shared_lock.unlock();
            info_version = cluster_version_.load();
        }
        else if (insert_times % 10000 == 0) {
            update_shard_disk_usage(now_shard_conns_);
        }

        if (now_shard_conns_.empty()) {
            return false;
        }
        auto& sc = choose_shard_conn(now_shard_conns_);

        for (;;) {
            if (sc.conn->insert(std::forward<DataType>(data), db_table)) {
                sc.handled_count += data.size();
                insert_times++;
                return true;
            }

            // Try to get another replica connection with same shard
            auto conn = make_shard_connection(sc.shard);
            if (conn) {
                sc.conn = std::move(conn);
                continue;
            }

            // If this shard has no replica node, remove the shard.  
            if (sc.shard.replicas.empty()) { 
                auto it = std::find_if(now_shard_conns_.begin(), now_shard_conns_.end(),
                    [index = sc.index](const shard_conn& sc) { return index == sc.index; });
                now_shard_conns_.erase(it);
            }
            if (now_shard_conns_.empty()) {
                return false;
            }
            sc = choose_shard_conn(now_shard_conns_);
        }
    }

private:
    /**
     * @brief Get a shard connection by roundrobin. 
     * For one shard which has the lowest disk usage, it will be selected twice.
     * @param shard_conns All shard connections, can not be empty
     * @return
     */
    shard_conn& choose_shard_conn(std::vector<shard_conn>& shard_conns) {
        thread_local size_t pos = 0;
        if (pos >= shard_conns.size()) {
            //This is a compensation for one shard which has the least disk usage.
            pos = 0;
            return shard_conns[0];
        }

        auto& shard = shard_conns[pos];
        pos++;
        return shard;
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

    void update_shard_disk_usage(std::vector<shard_conn>& shard_conns) {
        constexpr char disk_sql[] 
            = "select free_space, total_space, keep_free_space from system.disks;";
        for (auto& shard : shard_conns) {
            try {
                uint64_t free_space = 0;
                uint64_t space = 0;
                shard.conn->get_raw_conn()->Select(disk_sql,
                    [&free_space, &space](const clickhouse::Block& block) {
                    for (size_t i = 0; i < block.GetRowCount(); ++i) {
                        free_space += (*block[0]->As<clickhouse::ColumnUInt64>())[i];
                        auto total_space = (*block[1]->As<clickhouse::ColumnUInt64>())[i];
                        auto keep_free_space = (*block[2]->As<clickhouse::ColumnUInt64>())[i];
                        space += (total_space - keep_free_space);
                    }
                });
                //Calculate entirety usage
                shard.disk_usage = (space - free_space) * 1.0f / space;
            }
            catch (const std::exception& e) {
                printf("check shard disk usage failed: %s\n", e.what());
            }
        }
        std::sort(shard_conns.begin(), shard_conns.end(),
            [](const shard_conn& shard1, const shard_conn& shard2) {
            return shard1.disk_usage < shard2.disk_usage;
        });
    }
};

}  // namespace sqlcpp::ch