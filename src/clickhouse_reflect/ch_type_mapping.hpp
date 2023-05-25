#pragma once
#include <string>
#include "clickhouse/columns/array.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "reflection/reflect_meta.hpp"
#include "user_define_type.hpp"

namespace sqlcpp::ch::inner {

template <typename T, typename U = void>
struct type_mapping {
	using ch_type = void;
	using ch_ptr_type = void;
	auto make_column(size_t, size_t, size_t) {
		static_assert(reflection::always_false_v<T>, "ch type mapping failed");
	}
};

template <>
struct type_mapping<int8_t> {
	using ch_type = clickhouse::ColumnInt8;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnInt8>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnInt8>(count);
	}
};

template <>
struct type_mapping<uint8_t> {
	using ch_type = clickhouse::ColumnUInt8;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnUInt8>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnUInt8>(count);
	}
};

template <>
struct type_mapping<int16_t> {
	using ch_type = clickhouse::ColumnInt16;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnInt16>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnInt16>(count);
	}
};

template <>
struct type_mapping<uint16_t> {
	using ch_type = clickhouse::ColumnUInt16;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnUInt16>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnUInt16>(count);
	}
};

template <>
struct type_mapping<int32_t> {
	using ch_type = clickhouse::ColumnInt32;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnInt32>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnInt32>(count);
	}
};

template <>
struct type_mapping<uint32_t> {
	using ch_type = clickhouse::ColumnUInt32;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnUInt32>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnUInt32>(count);
	}
};

template <>
struct type_mapping<int64_t> {
	using ch_type = clickhouse::ColumnInt64;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnInt64>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnInt64>(count);
	}
};

template <>
struct type_mapping<uint64_t> {
	using ch_type = clickhouse::ColumnUInt64;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnUInt64>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnUInt64>(count);
	}
};

template <>
struct type_mapping<std::string> {
	using ch_type = clickhouse::ColumnString;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnString>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnString>(count);
	}
};

template <>
struct type_mapping<std::string_view> {
	using ch_type = clickhouse::ColumnString;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnString>;
	auto make_column(size_t count, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnString>(count);
	}
};

template <>
struct type_mapping<date_time> {
	using ch_type = clickhouse::ColumnDateTime;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnDateTime>;
	auto make_column(size_t, size_t, size_t) {
		return std::make_shared<clickhouse::ColumnDateTime>();
	}
};

template <typename T>
struct type_mapping<T, std::enable_if_t<reflection::nested_sequence_layer_v<T> == 1>> {
	using ch_type = clickhouse::ColumnArray;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnArray>;
	auto make_column(size_t count, size_t, size_t column) {
		using inner_mapping_type =
			typename type_mapping<reflection::nested_sequence_inner_t<T>>::ch_type;
		return std::make_shared<clickhouse::ColumnArrayT<inner_mapping_type>>((count * column));
	}
};

template <typename T>
struct type_mapping<T, std::enable_if_t<reflection::nested_sequence_layer_v<T> == 2>> {
	using ch_type = clickhouse::ColumnArray;
	using ch_ptr_type = std::shared_ptr<clickhouse::ColumnArray>;
	auto make_column(size_t count, size_t row, size_t column) {
		using inner_mapping_type =
			typename type_mapping<reflection::nested_sequence_inner_t<T>>::ch_type;
		return std::make_shared<
			clickhouse::ColumnArrayT<clickhouse::ColumnArrayT<inner_mapping_type>>>(
			(count * column * row));
	}
};

}  // namespace sqlcpp::ch::inner
