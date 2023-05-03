project(faststorage)

cmake_minimum_required(VERSION 3.8)

set(CMAKE_CXX_STANDARD 17)

#set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
#set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
 


if(NOT CMAKE_BUILD_TYPE)
    SET(CMAKE_BUILD_TYPE Release)
endif()

if (MSVC)
    add_compile_options(/W4)
else ()
    set(c_extra_wall   "-Wno-deprecated-declarations -Wempty-body -Wconversion -Wreturn-type -Wparentheses -Wno-format -Wuninitialized -Wunreachable-code -Wunused-function -Wunused-value -Wunused-variable -Wno-pointer-sign")
    set(cxx_extra_wall "-Wno-deprecated-declarations -Wempty-body -Wconversion -Wreturn-type -Wparentheses -Wno-format -Wuninitialized -Wunreachable-code -Wunused-function -Wunused-value -Wunused-variable")

    set(CMAKE_C_FLAGS_DEBUG  "${CMAKE_C_FLAGS_DEBUG} -fPIC -Wl,--as-needed -O0 -g -Wall -Wextra  -Werror ${c_extra_wall}")
    set(CMAKE_C_FLAGS_RELEASE  "${CMAKE_C_FLAGS_RELEASE} -fPIC -Wl,--as-needed -O3 -Wall -Wextra -Werror ${c_extra_wall}")

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fPIC -Wl,--as-needed -O0 -g -Wall -Wextra  -Werror ${cxx_extra_wall}")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fPIC -Wl,--as-needed -O3 -Wall -Wextra -Werror ${cxx_extra_wall}") 
endif ()


if (MSVC)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif ()
add_definitions(-DSPDLOG_ACTIVE_LEVEL=0)
add_definitions(-D_SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING)


include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/3party/gtest-1.11.0/include)
include_directories(${PROJECT_SOURCE_DIR}/3party/concurrentqueue-master)
include_directories(${PROJECT_SOURCE_DIR}/3party/json-3.11.2/include)
include_directories(${PROJECT_SOURCE_DIR}/3party/msgpack-4.1.3/include)

include_directories(${PROJECT_SOURCE_DIR}/3party/clickhouse-cpp-2.3.0)
include_directories(${PROJECT_SOURCE_DIR}/3party/clickhouse-cpp-2.3.0/contrib)

add_subdirectory(3party/clickhouse-cpp-2.3.0)
add_subdirectory(3party/gtest-1.11.0)
add_subdirectory(unit_test)
 