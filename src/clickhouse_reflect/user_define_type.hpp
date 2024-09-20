#pragma once
#include "meta_describe.hpp"

namespace sqlcpp::ch {

struct date_time {
	std::time_t time = 0;

	date_time() = default;
	date_time(std::time_t t) : time(t) {}

	operator std::time_t() {
		return time;
	}
	MSGPACK_DEFINE(time);
};

struct uuid {
	uint64_t high64 = 0;
	uint64_t low64 = 0;

	uuid() = default;
	uuid(uint64_t high, uint64_t low) : high64(high), low64(low) {}

	operator std::pair<uint64_t, uint64_t>() {
		return std::pair{ high64, low64 };
	}
	
	MSGPACK_DEFINE(high64, low64);
};

}  // namespace sqlcpp::ch
