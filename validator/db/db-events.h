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
#pragma once

#include "td/utils/Slice.h"
#include "td/utils/Status.h"
#include "td/utils/common.h"
#include "td/utils/port/config.h"

namespace ton {

namespace validator {

std::string get_db_events_fifo_path(td::Slice root_path);

class DbEventPublisher {
 public:
  DbEventPublisher() = default;
  explicit DbEventPublisher(std::string fifo_path);

  void set_fifo_path(std::string fifo_path);
  void publish(td::Slice data);

  const std::string &fifo_path() const {
    return fifo_path_;
  }

 private:
  std::string fifo_path_;
  bool fifo_ready_ = false;
  bool disabled_ = false;
  bool ready_error_logged_ = false;
  bool write_error_logged_ = false;
  bool no_reader_logged_ = false;
  bool temp_error_logged_ = false;
  bool unsupported_logged_ = false;

#if TD_PORT_POSIX
  enum class WriteStatus { Ok, NoReader, TemporaryError, FatalError };

  td::Status ensure_ready();
  WriteStatus write_once(td::Slice data);
#endif
};

}  // namespace validator

}  // namespace ton
