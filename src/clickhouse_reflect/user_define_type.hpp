#pragma once
#include "meta_describe.hpp"

namespace sqlcpp::ch {
struct date_time {
	uint32_t time_{};

	date_time() = default;
	date_time(uint32_t time) : time_(time) {}

	operator uint32_t() {
		return time_;
	}
	MSGPACK_DEFINE(time_);
};
}  // namespace sqlcpp::ch
