#pragma once
#define MSGPACK_NO_BOOST
#include "msgpack.hpp"
#undef MSGPACK_NO_BOOST

#ifdef USER_DEFINE_TYPE
#include "reflection/reflection.hpp"

namespace msgpack {

template<typename ReflectType>
struct pack_template {
	template <typename Stream>
	msgpack::packer<Stream>& operator()(msgpack::packer<Stream>& o, const ReflectType& v) const {
		static_assert(reflection::is_non_intrusive_reflection_v<ReflectType>,
			"just support non-intrusive");
		using reflect_type = decltype(reflection_reflect_member(std::declval<ReflectType>()));
		constexpr auto address = reflect_type::elements_address();
		constexpr auto element_size = reflect_type::args_size_t::value;

		o.pack_array(element_size);
		for_each_tuple([&o, &v, &address](auto index) {
			o.pack(v.*std::get<index>(address));
		}, std::make_index_sequence<element_size>());
		return o;
	}
};

template<typename ReflectType>
struct convert_template {
	msgpack::object const& operator()(const msgpack::object& o, ReflectType& v) const {
		static_assert(reflection::is_non_intrusive_reflection_v<ReflectType>,
			"just support non-intrusive");
		using reflect_type = decltype(reflection_reflect_member(std::declval<ReflectType>()));
		constexpr auto address = reflect_type::elements_address();
		constexpr auto element_size = reflect_type::args_size_t::value;

		if (o.type != msgpack::type::ARRAY || o.via.array.size != element_size) {
			throw msgpack::type_error();
		}
		for_each_tuple([&o, &v, &address](auto index) {
			auto& elem = v.*std::get<index>(address);
			using elem_type = std::decay_t<decltype(elem)>;
			if (!o.via.array.ptr[index].is_nil()) {
				elem = o.via.array.ptr[index].template as<elem_type>();
			}
		}, std::make_index_sequence<element_size>());
		return o;
	}
};

}
#endif

namespace serialize {
class serializer {
private:
	msgpack::sbuffer sbuffer_;
	msgpack::packer<msgpack::sbuffer> packer_{ sbuffer_ };
	msgpack::zone z_;

public:
	template <typename T>
	inline auto serialize(T&& t) {
		sbuffer_.clear();
		packer_.pack(t);
		return std::string_view{ sbuffer_.data(), sbuffer_.size() };
	}

	template <typename T>
	inline T deserialize(const char* buf, std::size_t len) {
		return msgpack::unpack(z_, buf, len).as<T>();
	}
};
}  // namespace serialize
