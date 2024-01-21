#pragma once

#include <mercury_proc.h>

#include <cereal/cereal.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/tuple.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cstring>
#include <string>

using namespace std::string_literals;

class MercuryCore;

class ProcOutputArchive final
    : public cereal::OutputArchive<ProcOutputArchive,
                                   cereal::AllowEmptyClassElision> {
 public:
  ProcOutputArchive(hg_proc_t p)
      : cereal::OutputArchive<ProcOutputArchive,
                              cereal::AllowEmptyClassElision>(this),
        proc(p) {}

  ~ProcOutputArchive() final = default;

  void write(const void *data, size_t size) {
    hg_return_t ret = hg_proc_memcpy(proc, const_cast<void *>(data), size);
    if (ret != HG_SUCCESS) {
      throw std::runtime_error(
          "Error during serialization, hg_proc_memcpy returned"s +
          std::to_string(ret));
    }
  }

 private:
  hg_proc_t proc;
};

class ProcInputArchive final
    : public cereal::InputArchive<ProcInputArchive,
                                  cereal::AllowEmptyClassElision> {
 public:
  ProcInputArchive(hg_proc_t p)
      : cereal::InputArchive<ProcInputArchive, cereal::AllowEmptyClassElision>(
            this),
        proc(p) {}

  ~ProcInputArchive() final = default;

  void read(void *data, std::size_t size) {
    hg_return_t ret = hg_proc_memcpy(proc, static_cast<void *>(data), size);
    if (ret != HG_SUCCESS) {
      throw std::runtime_error(
          "Error during serialization, hg_proc_memcpy returned"s +
          std::to_string(ret));
    }
  }

 private:
  hg_proc_t proc;
};

template <class T>
inline typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_SAVE_FUNCTION_NAME(ProcOutputArchive &ar, T const &t) {
  ar.write(std::addressof(t), sizeof(t));
}

template <class T>
inline typename std::enable_if<std::is_arithmetic<T>::value, void>::type
CEREAL_LOAD_FUNCTION_NAME(ProcInputArchive &ar, T &t) {
  ar.read(std::addressof(t), sizeof(t));
}

template <class T>
inline void CEREAL_SERIALIZE_FUNCTION_NAME(ProcOutputArchive &ar,
                                           cereal::NameValuePair<T> &t) {
  ar(t.value);
}

template <class T>
inline void CEREAL_SERIALIZE_FUNCTION_NAME(ProcInputArchive &ar,
                                           cereal::NameValuePair<T> &t) {
  ar(t.value);
}

template <class T>
inline void CEREAL_SERIALIZE_FUNCTION_NAME(ProcOutputArchive &ar,
                                           cereal::SizeTag<T> &t) {
  ar(t.size);
}

template <class T>
inline void CEREAL_SERIALIZE_FUNCTION_NAME(ProcInputArchive &ar,
                                           cereal::SizeTag<T> &t) {
  ar(t.size);
}

template <class T>
inline void CEREAL_SAVE_FUNCTION_NAME(ProcOutputArchive &ar,
                                      cereal::BinaryData<T> const &bd) {
  ar.write(bd.data, static_cast<std::size_t>(bd.size));
}

template <class T>
inline void CEREAL_LOAD_FUNCTION_NAME(ProcInputArchive &ar,
                                      cereal::BinaryData<T> &bd) {
  ar.read(bd.data, static_cast<std::size_t>(bd.size));
}

// register archives for polymorphic support
CEREAL_REGISTER_ARCHIVE(ProcOutputArchive)
CEREAL_REGISTER_ARCHIVE(ProcInputArchive)

// tie input and output archives together
CEREAL_SETUP_ARCHIVE_TRAITS(ProcInputArchive, ProcOutputArchive)

// general serialization of cereal for mercury
// switch-cases are benefit for you to add some debug info here

template <typename T>
auto proc_object_encode(hg_proc_t proc, T &data) -> hg_return_t {
  switch (hg_proc_get_op(proc)) {
    case HG_ENCODE: {
      ProcOutputArchive ar(proc);
      ar << data;  // add tag or other info
    } break;
    case HG_DECODE:
      return HG_INVALID_ARG;
    case HG_FREE:
    default:
      break;
  }
  return HG_SUCCESS;
}

template <typename T>
auto proc_object_decode(hg_proc_t proc, T &data) -> hg_return_t {
  switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      return HG_INVALID_ARG;
    case HG_DECODE: {
      ProcInputArchive ar(proc);
      ar >> data;  // decode tag or other info
    } break;
    case HG_FREE: {
    }
    default:
      break;
  }
  return HG_SUCCESS;
}