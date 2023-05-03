#pragma once
#include "disk_cache/serialize.hpp"
#include "reflection/reflect_meta.hpp"

#define META_DESCRIBE_INTRUSIVE(type, ...) \
    REFLECT_INTRUSIVE(type, __VA_ARGS__);  \
    MSGPACK_DEFINE(__VA_ARGS__);
