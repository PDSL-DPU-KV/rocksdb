#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cstddef>
#include <string>

#include "rsc/utilities/errno.hh"
#include "util/spdlogger.h"

namespace sc::rdma {

class EventChannel {
 public:
  EventChannel() {
    ec_ = rdma_create_event_channel();
    if (ec_ == nullptr) {
      std::runtime_error("fail to create event channel");
    }
  }

  ~EventChannel() {
    if (ec_ != nullptr) {
      rdma_destroy_event_channel(ec_);
    }
  }

 public:
  // NOTICE: the caller shall ack the event
  auto WaitFor(rdma_cm_event_type expected) -> rdma_cm_event* {
    rdma_cm_event* e = nullptr;
    if (auto rc = rdma_get_cm_event(ec_, &e); rc != 0) {
      ERROR("fail to get {} because \"{}\"",
            std::string_view(rdma_event_str(expected)), util::ErrnoString());
      return nullptr;
    }
    if (e->status != 0) {
      ERROR("get a bad event {}, status: {}, expect: {}",
            rdma_event_str(e->event), e->status,
            std::string_view(rdma_event_str(expected)));
    } else if (e->event != expected) {
      ERROR("expect: {}, get: {}", std::string_view(rdma_event_str(expected)),
            std::string_view(rdma_event_str(e->event)));
    } else {
      return e;
    }
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the unexpected event");
    }
    return nullptr;
  }

  auto Underlying() -> rdma_event_channel* { return ec_; }

 private:
  rdma_event_channel* ec_;
};

class ConnHandle {
 public:
  ConnHandle(bool is_passive) : is_passive_(is_passive) {
    memset(&for_client_, 0, sizeof(rdma_conn_param));
    memset(&for_server_, 0, sizeof(rdma_conn_param));
    memset(&dev_attr_ex_, 0, sizeof(ibv_device_attr_ex));
  }
  ~ConnHandle() {
    if (for_server_.private_data != nullptr) {
      delete[](char*) for_server_.private_data;
    }
    if (for_client_.private_data != nullptr) {
      delete[](char*) for_client_.private_data;
    }
    if (id_ != nullptr) {
      if (auto rc = rdma_destroy_id(id_); rc != 0) {
        ERROR("fail to destroy cm id because \"{}\"", util::ErrnoString());
      }
    }
    if (addr_ != nullptr) {
      rdma_freeaddrinfo(addr_);
    }
  }

  auto CreateId(const char* ipv4_host, const char* port) -> bool {
    rdma_addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = RAI_NUMERICHOST | RAI_FAMILY;
    hints.ai_flags |= (is_passive_ ? RAI_PASSIVE : 0);
    hints.ai_family = AF_INET;
    if (auto rc = rdma_getaddrinfo(ipv4_host, port, &hints, &addr_); rc != 0) {
      ERROR("fail to get address infomation of {}:{} because \"{}\"", ipv4_host,
            port, util::ErrnoString());
      return false;
    }

    INFO("get addrinfo {}", addr_ != nullptr);

    if (auto rc = rdma_create_id(ec_.Underlying(), &id_, nullptr, RDMA_PS_TCP);
        rc != 0) {
      ERROR("fail to create cm_id because \"{}\"", util::ErrnoString());
      return false;
    }
    INFO("RDMA connection id for the server is created {}", id_ != nullptr);
    return true;
  }

  auto SetDataToSend(void* data, uint8_t length) -> void {
    auto data_dup = new char[length];
    memcpy(data_dup, data, length);
    auto& param = is_passive_ ? for_client_ : for_server_;
    param.private_data = data_dup;
    param.private_data_len = length;
  }

  constexpr auto GetReceivedData() -> std::pair<const void*, uint8_t> {
    return {(is_passive_ ? for_server_.private_data : for_client_.private_data),
            (is_passive_ ? for_server_.private_data_len
                         : for_client_.private_data_len)};
  }

  auto IsConnected() -> bool { return connected_; }

 protected:
  auto PrepareParams() -> void {
    ibv_query_device_ex_input query{};
    auto& param = is_passive_ ? for_client_ : for_server_;
    if (id_->verbs)
      if (auto rc = ibv_query_device_ex(id_->verbs, &query, &dev_attr_ex_);
          rc != 0) {
        // WARN("fail to query extended attributes of device because of \"{}\"",
        // util::ErrnoString()); WARN("set all parameters with possible
        // minimum");
        param.responder_resources = 1;
        param.initiator_depth = 1;
        param.rnr_retry_count = 7;
        return;
      }
    param.initiator_depth = dev_attr_ex_.orig_attr.max_qp_init_rd_atom;
    param.responder_resources = dev_attr_ex_.orig_attr.max_qp_rd_atom;
    param.rnr_retry_count = 7;
  }

  auto RetrieveParams(rdma_cm_event* e) -> void {
    auto data_dup = new char[e->param.conn.private_data_len];
    memcpy(data_dup, e->param.conn.private_data,
           e->param.conn.private_data_len);
    auto& param = is_passive_ ? for_server_ : for_client_;
    memcpy(&param, &e->param.conn, sizeof(rdma_conn_param));
    param.private_data = data_dup;
    param.private_data_len = e->param.conn.private_data_len;
  }

 protected:
  bool is_passive_;
  bool connected_{false};
  EventChannel ec_;
  rdma_addrinfo* addr_{nullptr};
  rdma_cm_id* id_{nullptr};
  rdma_conn_param for_client_;
  rdma_conn_param for_server_;
  ibv_device_attr_ex dev_attr_ex_;

  // the handler will be called after
  // 1. client-side has succeded to resolve the address and route.
  // 2. server-side has received the connection request from client.
  using handler_t = std::function<bool(ConnHandle*, rdma_cm_id*)>;
};

class SimpleConnector : public ConnHandle {
 public:
  constexpr static uint32_t default_timeout = 10;

 public:
  SimpleConnector() : ConnHandle(false) {}
  ~SimpleConnector() {}

 public:
  auto ConnectAndEstablish(const char* ipv4_host, const char* port,
                           const handler_t& ctx_builder) -> bool {
    if (not CreateId(ipv4_host, port)) {
      return false;
    }
    rdma_cm_event* e = nullptr;
    //* resolve address
    if (auto rc = rdma_resolve_addr(id_, nullptr, addr_->ai_dst_addr,
                                    default_timeout);
        rc != 0) {
      ERROR("fail to resolve address because \"{}\"", util::ErrnoString());
      return false;
    }
    if (e = ec_.WaitFor(RDMA_CM_EVENT_ADDR_RESOLVED); e == nullptr) {
      // present error inside
      return false;
    }
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the address resolve event because \"{}\"",
            util::ErrnoString());
      return false;
    }
    //* prepare data
    PrepareParams();
    //* resolve route
    if (auto rc = rdma_resolve_route(id_, default_timeout); rc != 0) {
      ERROR("fail to resolve route because \"{}\"", util::ErrnoString());
      return false;
    }
    if (e = ec_.WaitFor(RDMA_CM_EVENT_ROUTE_RESOLVED); e == nullptr) {
      // present error inside
      return false;
    }
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the route resolve event because \"{}\"",
            util::ErrnoString());
      return false;
    }
    //* create context
    if (not ctx_builder(this, id_)) {
      ERROR("fail to build context");
      return false;
    }
    //* initiate connection
    if (auto rc = rdma_connect(id_, &for_server_); rc != 0) {
      ERROR("fail to connect with {}:{} because \"{}\"",
            inet_ntoa(((sockaddr_in*)addr_->ai_dst_addr)->sin_addr),
            ((sockaddr_in*)addr_->ai_dst_addr)->sin_port, util::ErrnoString());
      return false;
    }
    //* establish and exchange private data
    if (e = ec_.WaitFor(RDMA_CM_EVENT_ESTABLISHED); e == nullptr) {
      // present error inside
      return false;
    }
    //* retrieve params
    RetrieveParams(e);
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the address resolve event because \"{}\"",
            util::ErrnoString());
      return false;
    }

    return true;
  }

  auto Disconnect() -> void {
    //* initiate disconnection
    rdma_cm_event* e = nullptr;
    if (auto rc = rdma_disconnect(id_); rc != 0) {
      ERROR("fail to disconnect with {}:{} because \"{}\"",
            inet_ntoa(((sockaddr_in*)addr_->ai_dst_addr)->sin_addr),
            ((sockaddr_in*)addr_->ai_dst_addr)->sin_port, util::ErrnoString());
      return;
    }
    if (e = ec_.WaitFor(RDMA_CM_EVENT_DISCONNECTED); e == nullptr) {
      // present error inside
      return;
    }
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the disconnection event because \"{}\"",
            util::ErrnoString());
      return;
    }
  }
};

class SimpleAcceptor : public ConnHandle {
 public:
  SimpleAcceptor() : ConnHandle(true) {}
  ~SimpleAcceptor() {}

 public:
  auto ListenAndAccept(const char* ipv4_host, const char* port,
                       const handler_t& ctx_builder) -> bool {
    if (not CreateId(ipv4_host, port)) {
      return false;
    }
    INFO("CreateID OK");
    rdma_cm_event* e = nullptr;
    //* bind address
    if (auto rc = rdma_bind_addr(id_, addr_->ai_src_addr); rc != 0) {
      ERROR("fail to bind address because \"{}\"", util::ErrnoString());
      return false;
    }
    INFO("server bind addr success");
    //* prepare data
    PrepareParams();
    INFO("prepare success");
    //* listen for connection
    if (auto rc = rdma_listen(id_, 8); rc != 0) {
      ERROR("fail to listen on {}:{} because \"{}\"",
            inet_ntoa(((sockaddr_in*)addr_->ai_src_addr)->sin_addr),
            ((sockaddr_in*)addr_->ai_src_addr)->sin_port, util::ErrnoString());
      return false;
    }
    INFO("listen success");
    if (e = ec_.WaitFor(RDMA_CM_EVENT_CONNECT_REQUEST); e == nullptr) {
      // present error inside
      return false;
    }
    //* create context
    if (not ctx_builder(this, e->id)) {
      ERROR("fail to build context, now reject the connection");
      //* reject
      if (auto rc = rdma_reject(e->id, nullptr, 0); rc != 0) {
        ERROR("fail to reject connection requrest because \"{}\"",
              util::ErrnoString());
        // this may lead to client wait forever
      }
      if (auto rc = rdma_ack_cm_event(e); rc != 0) {
        ERROR("fail to ack the connection event because \"{}\"",
              util::ErrnoString());
      }
      return false;
    }
    //* accept
    if (auto rc = rdma_accept(e->id, &for_client_); rc != 0) {
      ERROR("fail to accept connection requrest because \"{}\"",
            util::ErrnoString());
      return false;
    }
    //* retrieve params
    RetrieveParams(e);
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the connection event because \"{}\"",
            util::ErrnoString());
      return false;
    }
    //* establish and exchange private data
    if (e = ec_.WaitFor(RDMA_CM_EVENT_ESTABLISHED); e == nullptr) {
      // present error inside
      return false;
    }
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the establish event because \"{}\"",
            util::ErrnoString());
      return false;
    }
    return true;
  }

  auto WaitForDisconnection() -> void {
    //* wait for disconnnection
    rdma_cm_event* e = nullptr;
    if (e = ec_.WaitFor(RDMA_CM_EVENT_DISCONNECTED); e == nullptr) {
      // present error inside
      return;
    }
    if (auto rc = rdma_ack_cm_event(e); rc != 0) {
      ERROR("fail to ack the disconnection event because \"{}\"",
            util::ErrnoString());
      return;
    }
  }
};

}  // namespace sc::rdma