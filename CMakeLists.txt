# Copyright 2017 The LEVELDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.1)
project(leveldb VERSION 0.1.0 LANGUAGES C CXX)

# This project can take advantage of C++11.
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED OFF)
# TODO(pwnall): See if setting this to ON gives us the *_unlocked functions.
set(CMAKE_CXX_EXTENSIONS OFF)

option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" ON)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" ON)
option(LEVELDB_INSTALL "Install LevelDB's header and library" ON)

include(TestBigEndian)
test_big_endian(LEVELDB_IS_BIG_ENDIAN)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckIncludeFileCXX)
check_include_file_cxx("atomic" LEVELDB_ATOMIC_PRESENT)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)

include(CheckSymbolExists)
check_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)

configure_file(
  "${PROJECT_SOURCE_DIR}/port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/include/port/port_config.h"
)

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "${PROJECT_SOURCE_DIR}"
)

# POSIX code is specified separately so we can leave it out in the future.
add_library(leveldb_port_posix OBJECT port/port_posix.cc
    ${PROJECT_BINARY_DIR}/include/port/port_config.h
    ${PROJECT_SOURCE_DIR}/port/atomic_pointer.h
    ${PROJECT_SOURCE_DIR}/port/port_posix.h
    ${PROJECT_SOURCE_DIR}/port/port.h
)

add_library(leveldb ""
  $<TARGET_OBJECTS:leveldb_port_posix>
  "db/builder.cc"
  "db/builder.h"
  "db/c.cc"
  "db/db_impl.cc"
  "db/db_impl.h"
  "db/db_iter.cc"
  "db/db_iter.h"
  "db/dbformat.cc"
  "db/dbformat.h"
  "db/dumpfile.cc"
  "db/filename.cc"
  "db/filename.h"
  "db/log_format.h"
  "db/log_reader.cc"
  "db/log_reader.h"
  "db/log_writer.cc"
  "db/log_writer.h"
  "db/memtable.cc"
  "db/memtable.h"
  "db/repair.cc"
  "db/skiplist.h"
  "db/snapshot.h"
  "db/table_cache.cc"
  "db/table_cache.h"
  "db/version_edit.cc"
  "db/version_edit.h"
  "db/version_set.cc"
  "db/version_set.h"
  "db/write_batch_internal.h"
  "db/write_batch.cc"
  "port/port.h"
  "port/thread_annotations.h"
  "table/block_builder.cc"
  "table/block_builder.h"
  "table/block.cc"
  "table/block.h"
  "table/filter_block.cc"
  "table/filter_block.h"
  "table/format.cc"
  "table/format.h"
  "table/iterator_wrapper.h"
  "table/iterator.cc"
  "table/merger.cc"
  "table/merger.h"
  "table/table_builder.cc"
  "table/table.cc"
  "table/two_level_iterator.cc"
  "table/two_level_iterator.h"
  "util/arena.cc"
  "util/arena.h"
  "util/bloom.cc"
  "util/cache.cc"
  "util/coding.cc"
  "util/coding.h"
  "util/comparator.cc"
  "util/crc32c.cc"
  "util/crc32c.h"
  "util/env_posix.cc"
  "util/env.cc"
  "util/filter_policy.cc"
  "util/hash.cc"
  "util/hash.h"
  "util/logging.cc"
  "util/logging.h"
  "util/mutexlock.h"
  "util/options.cc"
  "util/posix_logger.h"
  "util/random.h"
  "util/status.cc"
  "include/leveldb/c.h"
  "include/leveldb/cache.h"
  "include/leveldb/comparator.h"
  "include/leveldb/db.h"
  "include/leveldb/dumpfile.h"
  "include/leveldb/env.h"
  "include/leveldb/filter_policy.h"
  "include/leveldb/iterator.h"
  "include/leveldb/options.h"
  "include/leveldb/slice.h"
  "include/leveldb/status.h"
  "include/leveldb/table_builder.h"
  "include/leveldb/table.h"
  "include/leveldb/write_batch.h"
)
target_include_directories(leveldb
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
)
target_compile_definitions(leveldb
  PRIVATE
    LEVELDB_PLATFORM_POSIX
)

# TODO(pwnall): This is only needed for port_posix.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(leveldb Threads::Threads)
set(deps Threads)

if (HAVE_CRC32C)
  target_link_libraries(leveldb crc32c)
  list(APPEND deps crc32c)
endif (HAVE_CRC32C)
if (HAVE_SNAPPY)
  target_link_libraries(leveldb snappy)
  list(APPEND deps snappy)
endif (HAVE_SNAPPY)

add_library(leveldb_memenv
    "${PROJECT_SOURCE_DIR}/helpers/memenv/memenv.cc"
    "${PROJECT_SOURCE_DIR}/helpers/memenv/memenv.h"
)
target_compile_definitions(leveldb_memenv
  PRIVATE
    LEVELDB_PLATFORM_POSIX
)
target_link_libraries(leveldb_memenv leveldb)

add_executable(leveldbutil
  "${PROJECT_SOURCE_DIR}/db/leveldbutil.cc"
)
target_link_libraries(leveldbutil leveldb)

if(LEVELDB_BUILD_TESTS)
  enable_testing()

  set(test_sources
      db/autocompact_test.cc
      db/corruption_test.cc
      db/db_test.cc
      db/dbformat_test.cc
      db/fault_injection_test.cc
      db/filename_test.cc
      db/log_test.cc
      db/recovery_test.cc
      db/skiplist_test.cc
      db/version_edit_test.cc
      db/version_set_test.cc
      db/write_batch_test.cc
      helpers/memenv/memenv_test.cc
      issues/issue178_test.cc
      issues/issue200_test.cc
      table/filter_block_test.cc
      table/table_test.cc
      util/arena_test.cc
      util/bloom_test.cc
      util/cache_test.cc
      util/coding_test.cc
      util/crc32c_test.cc
      util/env_posix_test.cc
      util/env_test.cc
      util/hash_test.cc)

  foreach(test_source IN LISTS test_sources)
    get_filename_component(test_name "${test_source}" NAME_WE)
    add_executable(${test_name}
        ${test_source}
        "util/testharness.cc"
        "util/testutil.cc"
        "${PROJECT_BINARY_DIR}/include/port/port_config.h"
        "util/testharness.h"
        "util/testutil.h"
    )
    target_compile_definitions(${test_name}
      PRIVATE
        LEVELDB_PLATFORM_POSIX
    )
    target_link_libraries(${test_name} PRIVATE leveldb_memenv)
    add_test(NAME ${test_name} COMMAND ${test_name})
  endforeach()
endif(LEVELDB_BUILD_TESTS)

if(LEVELDB_BUILD_BENCHMARKS)
  set(benchmark_sources "db/db_bench.cc")
  set(benchmark_libs leveldb)

  check_library_exists(sqlite3 sqlite3_open "" HAVE_SQLITE3)
  if (HAVE_SQLITE3)
    list(APPEND benchmark_sources "doc/bench/db_bench_sqlite3.cc")
    list(APPEND benchmark_libs sqlite3)
  endif (HAVE_SQLITE3)

  # check_library_exists is insufficient here because the library names have
  # different manglings when compiled with clang or gcc, at least when installed
  # with Homebrew on Mac.
  set(OLD_CMAKE_REQURED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
  list(APPEND CMAKE_REQUIRED_LIBRARIES kyotocabinet)
  include(CheckCXXSourceCompiles)
  check_cxx_source_compiles("
#include <kcpolydb.h>

int main() {
  kyotocabinet::TreeDB* db = new kyotocabinet::TreeDB();
  delete db;
  return 0;
}
  "  HAVE_KYOTOCABINET)
  set(CMAKE_REQUIRED_LIBRARIES ${OLD_CMAKE_REQURED_LIBRARIES})
  if (HAVE_KYOTOCABINET)
    list(APPEND benchmark_sources "doc/bench/db_bench_tree_db.cc")
    list(APPEND benchmark_libs kyotocabinet)
  endif (HAVE_KYOTOCABINET)

  foreach(benchmark_source IN LISTS benchmark_sources)
    get_filename_component(benchmark_name ${benchmark_source} NAME_WE)
    add_executable(${benchmark_name}
        ${benchmark_source}
        "util/histogram.cc"
        "util/testutil.cc"
        "${PROJECT_BINARY_DIR}/include/port/port_config.h"
        "util/histogram.h"
        "util/testutil.h"
    )
    target_compile_definitions(${benchmark_name}
      PRIVATE
        LEVELDB_PLATFORM_POSIX
    )
    target_link_libraries(${benchmark_name} PRIVATE ${benchmark_libs})
  endforeach()
endif(LEVELDB_BUILD_BENCHMARKS)

if(LEVELDB_INSTALL)
  # Installation (https://github.com/forexample/package-example)

  # Layout. This works for all platforms:
  #   * <prefix>/lib/cmake/<PROJECT-NAME>
  #   * <prefix>/lib/
  #   * <prefix>/include/
  set(config_install_dir "lib/cmake/${PROJECT_NAME}")
  set(include_install_dir "include")

  set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")

  # Configuration
  set(version_config "${generated_dir}/${PROJECT_NAME}ConfigVersion.cmake")
  set(project_config "${generated_dir}/${PROJECT_NAME}Config.cmake")
  set(TARGETS_EXPORT_NAME "${PROJECT_NAME}Targets")
  set(namespace "${PROJECT_NAME}::")

  # Include module with fuction 'write_basic_package_version_file'
  include(CMakePackageConfigHelpers)

  # Configure '<PROJECT-NAME>ConfigVersion.cmake'
  # Use:
  #   * PROJECT_VERSION
  write_basic_package_version_file(
      "${version_config}" COMPATIBILITY SameMajorVersion
  )

  # Configure '<PROJECT-NAME>Config.cmake'
  # Use variables:
  #   * TARGETS_EXPORT_NAME
  #   * PROJECT_NAME
  configure_package_config_file(
      "cmake/Config.cmake.in"
      "${project_config}"
      INSTALL_DESTINATION "${config_install_dir}"
  )

  # Targets:
  #   * <prefix>/lib/libleveldb.a
  #   * header location after install: <prefix>/include/leveldb/db.h
  #   * headers can be included by C++ code `#include <leveldb/db.h>`
  install(
      TARGETS leveldb
      EXPORT "${TARGETS_EXPORT_NAME}"
      LIBRARY DESTINATION "lib"
      ARCHIVE DESTINATION "lib"
      RUNTIME DESTINATION "bin"
      INCLUDES DESTINATION "${include_install_dir}"
  )

  # Headers:
  #   * include/leveldb/db.h -> <prefix>/include/leveldb/db.h
  install(
      DIRECTORY "include"
      DESTINATION "${include_install_dir}"
      FILES_MATCHING PATTERN "*.h"
  )

  # Config
  #   * <prefix>/lib/cmake/Foo/leveldbConfig.cmake
  #   * <prefix>/lib/cmake/Foo/leveldbConfigVersion.cmake
  install(
      FILES "${project_config}" "${version_config}"
      DESTINATION "${config_install_dir}"
  )

  # Config
  #   * <prefix>/lib/cmake/leveldb/leveldbTargets.cmake
  install(
      EXPORT "${TARGETS_EXPORT_NAME}"
      NAMESPACE "${namespace}"
      DESTINATION "${config_install_dir}"
  )
endif(LEVELDB_INSTALL)
