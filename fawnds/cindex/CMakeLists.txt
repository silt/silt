cmake_minimum_required(VERSION 2.6)

project(CINDEX)

# compiler options
#add_definitions(-std=c++0x)
add_definitions(-g -Wall -Wextra -Wconversion -Wsign-conversion)
add_definitions(-O3)

# preprocessor 
message(STATUS "NDEBUG (disable all additional checkings; no*, yes) = $ENV{NDEBUG}")
if("$ENV{NDEBUG}" STREQUAL "yes")
	add_definitions(-DNDEBUG)
endif()

# source files
set(SOURCES ${SOURCES} bit_access.cpp)
set(SOURCES ${SOURCES} key_array.cpp)

set(SOURCES ${SOURCES} bit_vector.cpp)

set(SOURCES ${SOURCES} expected_size.cpp)

set(SOURCES ${SOURCES} flat_absoff_bucketing.cpp)
set(SOURCES ${SOURCES} twolevel_absoff_bucketing.cpp)
set(SOURCES ${SOURCES} twolevel_reloff_bucketing.cpp)
set(SOURCES ${SOURCES} semi_direct_16_absoff_bucketing.cpp)
set(SOURCES ${SOURCES} semi_direct_16_reloff_bucketing.cpp)

set(SOURCES ${SOURCES} trie.cpp)
set(SOURCES ${SOURCES} bucketing_index.cpp)

set(SOURCES ${SOURCES} stopwatch.cpp)

# boost
find_package(Boost 1.34.1)
if (Boost_FOUND)
	include_directories(${Boost_INCLUDE_DIRS})
else()
	message(boost not found)
endif()

# main library
add_library(cindex ${SOURCES})
target_link_libraries(cindex rt gsl gslcblas)

# benchmark (SOSP)
add_executable(bench_SOSP bench_SOSP.cpp)
target_link_libraries(bench_SOSP cindex)

