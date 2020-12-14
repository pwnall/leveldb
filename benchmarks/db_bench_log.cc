// Copyright (c) 2019 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>

#include "benchmark/benchmark.h"
#include "gtest/gtest.h"
#include "db/version_set.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

namespace {

std::string MakeKey(unsigned int num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%016u", num);
  return std::string(buf);
}

void BM_LogAndApply(benchmark::State& state) {
  int num_base_files = state.range(0);

  std::string dbname = testing::TempDir() + "leveldb_test_benchmark";
  DestroyDB(dbname, Options());

  DB* db = nullptr;
  Options opts;
  opts.create_if_missing = true;
  Status s = DB::Open(opts, dbname, &db);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  delete db;
  db = nullptr;

  Env* env = Env::Default();

  port::Mutex mu;
  MutexLock l(&mu);

  InternalKeyComparator cmp(BytewiseComparator());
  Options options;
  VersionSet vset(dbname, &options, nullptr, &cmp);
  bool save_manifest;
  Status recover_status = vset.Recover(&save_manifest);
  if (!recover_status.ok()) {
    state.SkipWithError(recover_status.ToString().c_str());
    return;
  }
  VersionEdit vbase;
  uint64_t fnum = 1;
  for (int i = 0; i < num_base_files; i++) {
    InternalKey start(MakeKey(2 * fnum), 1, kTypeValue);
    InternalKey limit(MakeKey(2 * fnum + 1), 1, kTypeDeletion);
    vbase.AddFile(2, fnum++, 1 /* file size */, start, limit);
  }
  Status apply_status = vset.LogAndApply(&vbase, &mu);
  if (!apply_status.ok()) {
    state.SkipWithError(apply_status.ToString().c_str());
    return;
  }

  for (auto _ : state) {
    VersionEdit vedit;
    vedit.RemoveFile(2, fnum);
    InternalKey start(MakeKey(2 * fnum), 1, kTypeValue);
    InternalKey limit(MakeKey(2 * fnum + 1), 1, kTypeDeletion);
    vedit.AddFile(2, fnum++, 1 /* file size */, start, limit);
    vset.LogAndApply(&vedit, &mu);
  }

  state.counters["iter_time"] = benchmark::Counter(
      1,
      benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_LogAndApply)->RangeMultiplier(10)->Range(1, 100000)->UseRealTime()
                         ->Unit(benchmark::kMicrosecond);

}  // namespace

}  // namespace leveldb

BENCHMARK_MAIN();
