#pragma once
#include "reflection/reflect_meta.hpp"
#define INNER_DEFINE_TYPE

namespace sqlcpp::ch {

struct date_time {
	std::time_t timestamp = 0;

	date_time() = default;
	date_time(std::time_t t) : timestamp(t) {}

	operator std::time_t() {
		return timestamp;
	}
};
REFLECT_NON_INTRUSIVE(date_time, timestamp);

struct uuid {
	uint64_t high64 = 0;
	uint64_t low64 = 0;

	uuid() = default;
	uuid(uint64_t high, uint64_t low) : high64(high), low64(low) {}

	operator std::pair<uint64_t, uint64_t>() {
		return std::pair{ high64, low64 };
	}
};
REFLECT_NON_INTRUSIVE(uuid, high64, low64);

}  // namespace sqlcpp::ch
