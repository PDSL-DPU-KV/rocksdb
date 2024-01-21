#pragma once

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_core_types.h>
#include <mercury_proc.h>
#include <mercury_proc_bulk.h>
#include <mercury_types.h>

#include <atomic>
#include <cassert>
#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>

#include "marker.hh"
#include "na_types.h"
#include "serialization.hh"
#include "sync.hh"

using MetaProc = std::function<hg_return_t(hg_proc_t)>;
static auto trigger_meta_proc(hg_proc_t proc, void *data) -> hg_return_t {
  return (*reinterpret_cast<MetaProc *>(data))(proc);
}
auto generic_rpc(hg_handle_t handle) -> hg_return_t;

// MercuryCore holds the core class: hg_class_t and hg_context_t, shared between
// all others classes
class MercuryCore : Noncopyable, Nonmovable {
  friend class MercuryEngine;
  friend class MercuryEndpoint;
  friend class Callable;

 public:
  ~MercuryCore() {
    if (hg_ctx != nullptr) {
      HG_Context_destroy(hg_ctx);
    }
    if (hg_class != nullptr) {
      HG_Finalize(hg_class);
    }
  }

 private:
  MercuryCore(const char *local, bool is_server) {
    hg_init_info info = HG_INIT_INFO_INITIALIZER;
    info.na_init_info.thread_mode = NA_THREAD_MODE_SINGLE;
    info.na_init_info.progress_mode = NA_NO_BLOCK;
    info.request_post_init = 1024;
    info.request_post_incr = 1024;
    hg_class = HG_Init(local, is_server);
    hg_ctx = HG_Context_create(hg_class);
    if (hg_class == nullptr or hg_ctx == nullptr) {
      throw std::runtime_error("fail to construct");
    }
  }

 private:
  hg_class_t *hg_class;
  hg_context_t *hg_ctx;
};

enum class PayloadType {
  Request,
  Response,
};
// Payload is devided into two kinds, request and response.
// Payload use general serializer to call the cereal achiever. see
// serialization.hh Just use as member funtion to parse.
template <PayloadType type>
class Payload {
  friend class Handle;
  friend class Callable;

  using Fn = hg_return_t (*)(hg_handle_t, void *);
  constexpr const static Fn unpack =
      (type == PayloadType::Request) ? HG_Get_input : HG_Get_output;
  constexpr const static Fn free =
      (type == PayloadType::Request) ? HG_Free_input : HG_Free_output;

 public:
  template <typename T>
  [[nodiscard]] auto as() const -> T {
    std::tuple<std::decay_t<T>> t;
    MetaProc decoder = [&t](hg_proc_t proc) {
      return proc_object_decode(proc, t);
    };
    auto ret = unpack(handle, &decoder);
    assert(ret == HG_SUCCESS);
    ret = free(handle, &decoder);
    assert(ret == HG_SUCCESS);
    return std::get<0>(std::move(t));
  }

  template <typename T1, typename T2, typename... Tn>
  [[nodiscard]] auto as() const {
    std::tuple<typename std::decay<T1>::type, typename std::decay<T2>::type,
               typename std::decay<Tn>::type...>
        t;
    MetaProc decoder = [&t](hg_proc_t proc) {
      return proc_object_decode(proc, t);
    };
    auto ret = unpack(handle, &decoder);
    assert(ret == HG_SUCCESS);
    ret = free(handle, &decoder);
    assert(ret == HG_SUCCESS);
    return t;
  }

 protected:
  Payload(hg_handle_t h) : handle(h) {}

 protected:
  hg_handle_t handle;
};
using RequestPayload = Payload<PayloadType::Request>;
using ResponsePayload = Payload<PayloadType::Response>;

class FutureResponsePayload : public BlockWait, public ResponsePayload {
 public:
  FutureResponsePayload(hg_handle_t h) : ResponsePayload(h) {}
  ~FutureResponsePayload() = default;
};

// Handle is used by the server to repond the caller
class Handle {
  friend auto generic_rpc(hg_handle_t handle) -> hg_return_t;

 public:
  [[nodiscard]] auto get_payload() const -> RequestPayload {
    return RequestPayload{handle};
  }

  template <typename... T>
  auto respond(const T &...args) const -> void {
    // currently we do not care the result of the response
    detached_respond(args...);
  }

  template <typename... T>
  auto detached_respond(const T &...args) const -> void {
    auto t = std::make_tuple(std::cref(args)...);
    MetaProc encoder = [&t](hg_proc_t proc) -> hg_return_t {
      return proc_object_encode(proc, t);
    };
    auto ret = HG_Respond(
        handle,
        [](const hg_cb_info *info) {
          assert(info->ret == HG_SUCCESS);
          return info->ret;
        },
        nullptr, &encoder);
    assert(ret == HG_SUCCESS);
  }

 private:
  Handle(hg_handle_t h) : handle(h) {}

 private:
  hg_handle_t handle;
};

// Endpoint is used by the client to call a certain server.
class MercuryEndpoint {
  friend class MercuryEngine;
  friend class Callable;

 public:
  MercuryEndpoint() {}
  ~MercuryEndpoint() {
    if (addr != HG_ADDR_NULL) {
      HG_Addr_free(core.lock()->hg_class, addr);
    }
  }

 private:
  MercuryEndpoint(std::weak_ptr<MercuryCore> core, hg_addr_t addr)
      : core(std::move(core)), addr(addr) {}

 private:
  std::weak_ptr<MercuryCore> core;
  hg_addr_t addr;
};

class Callable : public Noncopyable {
  friend class RemoteProcedure;

 public:
  // movable
  Callable(Callable &&other) noexcept
      : core(std::move(other.core)),
        handle(std::exchange(other.handle, HG_HANDLE_NULL)) {}
  auto operator=(Callable &&other) noexcept -> Callable & {
    core = std::move(other.core);
    handle = std::exchange(other.handle, HG_HANDLE_NULL);
    return *this;
  }

 public:
  ~Callable() { HG_Destroy(handle); }

  template <typename... T>
  [[nodiscard]] auto forward(const T &...args) const
      -> FutureResponsePayload * {
    auto t = std::make_tuple(std::cref(args)...);
    MetaProc encoder = [&t](hg_proc_t proc) -> hg_return_t {
      return proc_object_encode(proc, t);
    };
    auto p = new FutureResponsePayload(handle);
    auto ret = HG_Forward(
        handle,
        [](const hg_cb_info *info) {
          assert(info->ret == HG_SUCCESS);
          ((FutureResponsePayload *)(info->arg))->notify();
          return info->ret;
        },
        p, &encoder);
    assert(ret == HG_SUCCESS);
    return p;
  }

  template <typename... T>
  auto detached_forward(const T &...args) const -> void {
    auto t = std::make_tuple(std::cref(args)...);
    MetaProc encoder = [&t](hg_proc_t proc) -> hg_return_t {
      return proc_object_encode(proc, t);
    };
    auto ret = HG_Forward(handle, nullptr, nullptr, &encoder);
    assert(ret == HG_SUCCESS);
  }

 private:
  Callable(std::weak_ptr<MercuryCore> core, hg_id_t id,
           const MercuryEndpoint &ep)
      : core(std::move(core)), handle(HG_HANDLE_NULL) {
    if (auto ret = HG_Create(this->core.lock()->hg_ctx, ep.addr, id, &handle);
        ret != HG_SUCCESS) {
      throw std::runtime_error("fail to issue an rpc");
    }
  }

 private:
  std::weak_ptr<MercuryCore> core;
  hg_handle_t handle;
};

// Remote Procedure is used in both client and server.
// Notice: server need to provide the handler at definition.
class RemoteProcedure {
  friend class MercuryEngine;

 public:
  [[nodiscard]] auto on(const MercuryEndpoint &ep) const -> Callable {
    return Callable{core, id, ep};
  }

 private:
  RemoteProcedure(std::weak_ptr<MercuryCore> core, hg_id_t id)
      : core(std::move(core)), id(id) {}

 private:
  std::weak_ptr<MercuryCore> core;
  hg_id_t id;
};

// MercuryEngine is the entry point of this wrapper.
// Function progress is a block funtion to trigger all callbacks and incoming
// requests Pay attention to the mutex in the handler, they may cause deadlock
// somehow.
class MercuryEngine : public Noncopyable, public Nonmovable {
  friend auto generic_rpc(hg_handle_t handle) -> hg_return_t;

 public:
  MercuryEngine(const char *local, bool is_server)
      : core(new MercuryCore(local, is_server)) {}
  ~MercuryEngine() { stop(); }
  auto stop() -> void { finish.store(true); }

 public:
  //* used in server side
  auto define(const char *name, std::function<void(const Handle &)> &func) const
      -> RemoteProcedure {
    return define(name, std::function<void(const Handle &)>(func));
  }
  auto define(const char *name,
              std::function<void(const Handle &)> &&func) const
      -> RemoteProcedure {
    auto id = HG_Register_name(core->hg_class, name, trigger_meta_proc,
                               trigger_meta_proc, generic_rpc);
    auto rpc_data = new RpcData;
    rpc_data->core = core;
    rpc_data->func = [func = std::move(func)](const Handle &r) -> hg_return_t {
      func(r);
      return HG_SUCCESS;
    };
    auto ret = HG_Register_data(core->hg_class, id, rpc_data,
                                [](void *data) { delete (RpcData *)(data); });
    assert(ret == HG_SUCCESS);
    return RemoteProcedure{core, id};
  }
  //* used in client side
  auto define(const char *name) const -> RemoteProcedure {
    hg_id_t id = 0;
    hg_bool_t flag = HG_TRUE;
    HG_Registered_name(core->hg_class, name, &id, &flag);
    if (flag == HG_FALSE) {
      id = HG_Register_name(core->hg_class, name, trigger_meta_proc,
                            trigger_meta_proc, nullptr);
    }
    return RemoteProcedure{core, id};
  }

  auto lookup(const char *address) const -> MercuryEndpoint {
    hg_addr_t addr = HG_ADDR_NULL;
    auto ret = HG_Addr_lookup2(core->hg_class, address, &addr);
    assert(ret == HG_SUCCESS and addr != HG_ADDR_NULL);
    return MercuryEndpoint{core, addr};
  }

  auto progress() const -> void {
    printf("mercury endpoint starts serving\n");
    hg_return_t rc = HG_SUCCESS;
    do {
      do {
        auto actual = 0u;
        rc = HG_Trigger(core->hg_ctx, 0, 256, &actual);
        if (actual > 0) {
          continue;
        } else if (actual == 0) {
          break;
        }
      } while (rc == HG_SUCCESS or rc == HG_TIMEOUT);

      rc = HG_Progress(core->hg_ctx, 1000);

      if (finish.load(std::memory_order_relaxed)) {
        printf("mercury endpoint stops serving\n");
        return;
      }
    } while (rc == HG_SUCCESS or rc == HG_TIMEOUT);
    if (rc != HG_SUCCESS) {
      printf("mercury endpoint exits with return code %d\n", rc);
    }
  }

 private:
  using Rpc = std::function<void(const Handle &)>;
  struct RpcData {
    std::weak_ptr<MercuryCore> core;
    Rpc func;
  };

 private:
  std::atomic_bool finish{false};
  std::shared_ptr<MercuryCore> core;
};

// generic rpc wrapper
inline auto generic_rpc(hg_handle_t handle) -> hg_return_t {
  auto info = HG_Get_info(handle);
  auto data = static_cast<MercuryEngine::RpcData *>(
      HG_Registered_data(info->hg_class, info->id));
  assert(data->core.lock());  // just check
  auto &rpc = data->func;
  rpc(Handle(handle));
  HG_Destroy(handle);
  printf("async stop an rpc handler\n");
  return HG_SUCCESS;
}