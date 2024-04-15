#pragma once

#include <errno.h>

#include <cstring>
#include <string_view>

namespace sc::util {

inline auto ErrnoString() -> std::string_view {
  return std::string_view(strerror(errno));
}

}  // namespace sc::util