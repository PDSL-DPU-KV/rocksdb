#include <cereal/types/string.hpp>
#include <string>

struct compaction_args {
  std::string input;
  std::string db_name;
  uint64_t job_id;
  template <typename A>
  void serialize(A &ar) {
    ar(input, db_name, job_id);
  }
};