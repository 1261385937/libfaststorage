#include <string>
#include <set>

namespace sqlcpp::ch {
namespace cluster {

struct ch_replica {
    std::string ip;
    uint16_t port = 9000;
    std::string user = "";
    std::string passwd = "";
    uint32_t priority = 1; // higher priority with smaller number
    int max_connect_failed = 60;

    // If continuous failed times reach max_connect_failed, 
    // the replica node may be broken, will be removed
    mutable int continuous_connect_failed = 0;

    bool operator<(const ch_replica& c) const {
        return this->priority < c.priority;
    }
};

struct ch_shard {
    std::multiset<ch_replica> replicas;
    //uint32_t weight = 1;
};

}
}