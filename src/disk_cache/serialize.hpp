#pragma once
#define MSGPACK_NO_BOOST
#include "msgpack.hpp"
#undef MSGPACK_NO_BOOST

namespace serialize {
class serializer {
public:
	template <typename T>
	inline auto serialize(T&& t) {
		msgpack::sbuffer sb(64 * 1024 * 1024);
		msgpack::pack(sb, std::forward<T>(t));
		return sb;
	}

	template <typename T>
	inline T deserialize(const char* buf, std::size_t len) {
		auto obj_handle = msgpack::unpack(buf, len);
		return obj_handle.get().as<T>();
	}
};
}  // namespace serialize
