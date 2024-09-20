#pragma once
#include <atomic>
#include <chrono>
#include <string>

#include "reflection/reflect_meta.hpp"
#ifdef _WIN32
#pragma warning(disable : 4996)
#endif
#include "ch_type_mapping.hpp"
#include "clickhouse/client.h"

namespace sqlcpp::ch {

namespace inner {
struct clickhouse_tag {};
}  // namespace inner

class ch_connection {
public:
	using engine_type = inner::clickhouse_tag;

private:
	std::unique_ptr<clickhouse::Client> client_;

public:
	ch_connection(const std::string& ip, uint16_t port, const std::string& user,
		const std::string& passwd)
		: client_(std::make_unique<clickhouse::Client>(
			clickhouse::ClientOptions()
			.SetHost(ip)
			.SetPort(port)
			.SetUser(user)
			.SetPassword(passwd)
			.SetPingBeforeQuery(true)
			.SetSendRetries(1)
			.SetRetryTimeout(std::chrono::seconds(3))
			.SetConnectionConnectTimeout(std::chrono::seconds(3))
			.SetConnectionRecvTimeout(std::chrono::seconds(10))
			.SetConnectionSendTimeout(std::chrono::seconds(10))))
	{}

	auto& get_raw_conn() {
		return client_;
	}

	template <typename DataType>
	bool insert(DataType&& data, const std::string& db_table) {
		static_assert(reflection::is_sequence_std_container_v<DataType>,
					  "clickhouse insert must be batch for high performance");

		using value_type = typename std::decay_t<DataType>::value_type;
		constexpr auto is_smartptr = reflection::is_std_smartptr_v<value_type>;
		clickhouse::Block block;

		if constexpr (is_smartptr) {
			using type = typename value_type::element_type;
			static_assert(reflection::is_has_reflect_type_v<type>, "not found reflect type");
			block = gen_ch_block<type>(std::forward<DataType>(data));
		}
		else {
			static_assert(reflection::is_has_reflect_type_v<value_type>, "not found reflect type");
			block = gen_ch_block<value_type>(std::forward<DataType>(data));
		}

		try {
			//clickhouse detect network with SetPingBeforeQuery(true), if bad will reconnect.
			client_->Insert(db_table, block);
			return true;
		}
		catch (const std::exception& e) {
			printf("insert failed:%s\n", e.what());
		}
		return false;
	}

private:
	template <bool Rvalue = true, typename Address, typename columnTup, typename D>
	void convert_to_column(Address&& address, columnTup&& tup, D&& d) {
		constexpr auto element_size = std::tuple_size_v<std::decay_t<decltype(address)>>;
		for_each_tuple([&address, &tup, &d](auto index) {
			auto& value = d.*std::get<index>(address);
			auto& column_ptr = std::get<index>(tup);

			using element_type = std::decay_t<decltype(value)>;
			constexpr auto layer = reflection::nested_sequence_layer_v<element_type>;
			if constexpr (layer == 0 || layer == 1 || layer == 2) {
				column_ptr->Append(value);
			}
			else {
				static_assert(reflection::always_false_v<element_type>, "sequence nest more than 2");
			}
		}, std::make_index_sequence<element_size>());
	}

	template <typename Type, typename DataType>
	auto gen_ch_block(DataType&& data) {
		constexpr auto names = Type::elements_name();
		constexpr auto address = Type::elements_address();
		constexpr auto element_size = Type::args_size_t::value;
		thread_local auto column_tup = std::apply([this](auto&&... args) {
			return std::make_tuple(inner::type_mapping < std::decay_t<decltype(Type{}.*args) >> {}.
				make_column(0, 0, 0)...);
		}, address);

		for_each_tuple([](auto index) {
			auto& column_ptr = std::get<index>(column_tup);
			column_ptr->Clear();
		}, std::make_index_sequence<element_size>());

		using value_type = typename std::decay_t<DataType>::value_type;
		constexpr auto is_smartptr = reflection::is_std_smartptr_v<value_type>;
		constexpr auto is_rvalue = std::is_rvalue_reference_v<decltype(data)>;
		for (auto& d : data) {
			if constexpr (is_smartptr) {
				convert_to_column<is_rvalue>(address, column_tup, *d);
			}
			else {
				convert_to_column<is_rvalue>(address, column_tup, d);
			}
		}

		clickhouse::Block block;
		for_each_tuple([&names, &block](auto index) {
			//auto& col = std::get<index>(column_tup);
			//if (col->Size()) {
				block.AppendColumn(std::string(std::get<index>(names)), std::get<index>(column_tup));
			//}
		}, std::make_index_sequence<element_size>());
		return block;
	}
};

}  // namespace sqlcpp::ch
