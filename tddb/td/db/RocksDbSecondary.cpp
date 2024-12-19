/*
    This file is part of TON Blockchain Library.
    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.
    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.
    Copyright 2017-2020 Telegram Systems LLP
*/
#include "td/db/RocksDbSecondary.h"

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/statistics.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/utilities/transaction.h"

namespace td {
namespace {
static Status from_rocksdb(rocksdb::Status status) {
  if (status.ok()) {
    return Status::OK();
  }
  return Status::Error(status.ToString());
}
static Slice from_rocksdb(rocksdb::Slice slice) {
  return Slice(slice.data(), slice.size());
}
static rocksdb::Slice to_rocksdb(Slice slice) {
  return rocksdb::Slice(slice.data(), slice.size());
}
}  // namespace

Status RocksDbSecondary::destroy(Slice path) {
  return from_rocksdb(rocksdb::DestroyDB(path.str(), {}));
}

RocksDbSecondary::RocksDbSecondary(RocksDbSecondary &&) = default;
RocksDbSecondary &RocksDbSecondary::operator=(RocksDbSecondary &&) = default;

RocksDbSecondary::~RocksDbSecondary() {
  if (!db_) {
    return;
  }
  end_snapshot().ensure();
}

RocksDbSecondary RocksDbSecondary::clone() const {
  return RocksDbSecondary{db_, options_};
}

Result<RocksDbSecondary> RocksDbSecondary::open(std::string path, RocksDbSecondaryOptions options) {
  rocksdb::DB *db;
  auto statistics = rocksdb::CreateDBStatistics();
  {
    rocksdb::Options db_options;

    static auto default_cache = rocksdb::NewLRUCache(1 << 30);
    if (options.block_cache == nullptr) {
      options.block_cache = default_cache;
    }

    rocksdb::BlockBasedTableOptions table_options;
    // commenting this line because in recent rocksdb version it leads to NotFound error (file not in archive slice)
    // table_options.block_cache = options.block_cache; 
    db_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    db_options.manual_wal_flush = true;
    db_options.create_if_missing = true;
    db_options.max_background_compactions = 4;
    db_options.max_background_flushes = 2;
    db_options.bytes_per_sync = 1 << 20;
    db_options.writable_file_max_buffer_size = 2 << 14;
    db_options.keep_log_file_num = 1;
    db_options.statistics = options.statistics;
    rocksdb::ColumnFamilyOptions cf_options(db_options);
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cf_options));
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    std::string secondary_path = path;
    std::replace(secondary_path.begin(), secondary_path.end(), TD_DIR_SLASH, '_');
    TRY_STATUS(from_rocksdb(
      rocksdb::DB::OpenAsSecondary(db_options, path, options.secondary_logs_path + TD_DIR_SLASH + secondary_path, column_families, &handles, &db)));
    CHECK(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  TRY_STATUS(from_rocksdb(db->TryCatchUpWithPrimary()));
  return RocksDbSecondary(std::shared_ptr<rocksdb::DB>(db), std::move(options));
}

Status RocksDbSecondary::try_catch_up_with_primary() {
  return from_rocksdb(db_->TryCatchUpWithPrimary());
}

std::unique_ptr<KeyValueReader> RocksDbSecondary::snapshot() {
  auto res = std::make_unique<RocksDbSecondary>(clone());
  res->begin_snapshot().ensure();
  return std::move(res);
}

std::string RocksDbSecondary::stats() const {
  std::string out;
  db_->GetProperty("rocksdb.stats", &out);
  //db_->GetProperty("rocksdb.cur-size-all-mem-tables", &out);
  return out;
}

Result<RocksDbSecondary::GetStatus> RocksDbSecondary::get(Slice key, std::string &value) {
  rocksdb::Status status;
  if (snapshot_) {
    rocksdb::ReadOptions options;
    options.snapshot = snapshot_.get();
    status = db_->Get(options, to_rocksdb(key), &value);
  } else {
    status = db_->Get({}, to_rocksdb(key), &value);
  }
  if (status.ok()) {
    return GetStatus::Ok;
  }
  if (status.code() == rocksdb::Status::kNotFound) {
    return GetStatus::NotFound;
  }
  return from_rocksdb(status);
}

Status RocksDbSecondary::set(Slice key, Slice value) {
  CHECK(false)
}

Status RocksDbSecondary::erase(Slice key) {
  CHECK(false)
}

Result<size_t> RocksDbSecondary::count(Slice prefix) {
  rocksdb::ReadOptions options;
  options.snapshot = snapshot_.get();
  std::unique_ptr<rocksdb::Iterator> iterator;
  
  iterator.reset(db_->NewIterator(options));

  size_t res = 0;
  for (iterator->Seek(to_rocksdb(prefix)); iterator->Valid(); iterator->Next()) {
    if (from_rocksdb(iterator->key()).truncate(prefix.size()) != prefix) {
      break;
    }
    res++;
  }
  if (!iterator->status().ok()) {
    return from_rocksdb(iterator->status());
  }
  return res;
}

Status RocksDbSecondary::begin_write_batch() {
  CHECK(false)
}

Status RocksDbSecondary::begin_transaction() {
  CHECK(false)
}

Status RocksDbSecondary::commit_write_batch() {
  CHECK(false)
}

Status RocksDbSecondary::commit_transaction() {
  CHECK(false)
}

Status RocksDbSecondary::abort_write_batch() {
  CHECK(false)
}

Status RocksDbSecondary::abort_transaction() {
  CHECK(false)
}

Status RocksDbSecondary::flush() {
  CHECK(false)
}

Status RocksDbSecondary::begin_snapshot() {
  snapshot_.reset(db_->GetSnapshot());
  return td::Status::OK();
}

Status RocksDbSecondary::end_snapshot() {
  if (snapshot_) {
    db_->ReleaseSnapshot(snapshot_.release());
  }
  return td::Status::OK();
}

RocksDbSecondary::RocksDbSecondary(std::shared_ptr<rocksdb::DB> db, RocksDbSecondaryOptions options)
    : db_(std::move(db)), options_(std::move(options)) {
}
}  // namespace td