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

#include "validator/db/db-events.h"

#include "td/utils/logging.h"
#include "td/utils/ScopeGuard.h"
#include "td/utils/StringBuilder.h"

#if TD_PORT_POSIX
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace ton {

namespace validator {

namespace {
constexpr td::CSlice kDbEventsFifoName = "db-events.fifo";
}

DbEventPublisher::DbEventPublisher(std::string fifo_path) : fifo_path_(std::move(fifo_path)) {
}

void DbEventPublisher::set_fifo_path(std::string fifo_path) {
  fifo_path_ = std::move(fifo_path);
  fifo_ready_ = false;
  disabled_ = false;
  ready_error_logged_ = false;
  write_error_logged_ = false;
  no_reader_logged_ = false;
  temp_error_logged_ = false;
}

std::string get_db_events_fifo_path(td::Slice root_path) {
  std::string result = root_path.str();
  if (!result.empty() && result.back() != TD_DIR_SLASH) {
    result += TD_DIR_SLASH;
  }
  result.append(kDbEventsFifoName.begin(), kDbEventsFifoName.end());
  return result;
}

void DbEventPublisher::publish(td::Slice data) {
  if (fifo_path_.empty()) {
    return;
  }
#if !TD_PORT_POSIX
  if (!unsupported_logged_) {
    LOG(WARNING) << "DB events FIFO is not supported on this platform";
    unsupported_logged_ = true;
  }
  return;
#else
  if (disabled_) {
    return;
  }
  auto status = ensure_ready();
  if (status.is_error()) {
    if (!ready_error_logged_) {
      LOG(ERROR) << "Failed to prepare DB events FIFO '" << fifo_path_ << "': " << status;
      ready_error_logged_ = true;
    }
    disabled_ = true;
    return;
  }
  switch (write_once(data)) {
    case WriteStatus::Ok:
      no_reader_logged_ = false;
      temp_error_logged_ = false;
      break;
    case WriteStatus::NoReader:
      if (!no_reader_logged_) {
        LOG(INFO) << "DB events FIFO '" << fifo_path_ << "' has no reader. Dropping event";
        no_reader_logged_ = true;
      }
      break;
    case WriteStatus::TemporaryError:
      if (!temp_error_logged_) {
        LOG(WARNING) << "DB events FIFO '" << fifo_path_ << "' is temporarily unavailable. Dropping event";
        temp_error_logged_ = true;
      }
      break;
    case WriteStatus::FatalError:
      if (!write_error_logged_) {
        LOG(ERROR) << "Failed to publish DB event to '" << fifo_path_ << "', disabling events";
        write_error_logged_ = true;
      }
      disabled_ = true;
      break;
  }
#endif
}

#if TD_PORT_POSIX
td::Status DbEventPublisher::ensure_ready() {
  if (fifo_ready_) {
    return td::Status::OK();
  }
  struct stat st;
  if (lstat(fifo_path_.c_str(), &st) == 0) {
    if (!S_ISFIFO(st.st_mode)) {
      return td::Status::Error(PSLICE() << "path '" << fifo_path_ << "' exists and is not a FIFO");
    }
  } else {
    if (errno != ENOENT) {
      return td::Status::PosixError(errno, PSLICE() << "stat failed for '" << fifo_path_ << "'");
    }
    if (mkfifo(fifo_path_.c_str(), 0660) != 0 && errno != EEXIST) {
      return td::Status::PosixError(errno, PSLICE() << "mkfifo failed for '" << fifo_path_ << "'");
    }
  }
  fifo_ready_ = true;
  return td::Status::OK();
}

DbEventPublisher::WriteStatus DbEventPublisher::write_once(td::Slice data) {
  td::int32 flags = O_WRONLY | O_NONBLOCK | O_CLOEXEC;
  int fd = open(fifo_path_.c_str(), flags);
  if (fd < 0) {
    if (errno == ENXIO || errno == EPIPE) {
      return WriteStatus::NoReader;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      return WriteStatus::TemporaryError;
    }
    return WriteStatus::FatalError;
  }
  auto guard = td::ScopeExit() + [&] {
    while (close(fd) != 0) {
      if (errno != EINTR) {
        break;
      }
    }
  };
  while (!data.empty()) {
    auto written = write(fd, data.data(), data.size());
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return WriteStatus::TemporaryError;
      }
      if (errno == EPIPE) {
        return WriteStatus::NoReader;
      }
      return WriteStatus::FatalError;
    }
    data.remove_prefix(static_cast<size_t>(written));
  }
  return WriteStatus::Ok;
}
#endif

}  // namespace validator

}  // namespace ton
