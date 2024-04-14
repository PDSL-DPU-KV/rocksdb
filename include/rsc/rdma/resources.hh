#pragma once

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <optional>

#include "rsc/utilities/errno.hh"
#include "util/spdlogger.h"

namespace sc::rdma {

class RemoteMR {
 public:
  RemoteMR(void* addr, uint32_t length, uint32_t rkey)
      : addr_(addr), length_(length), rkey_(rkey) {}

 public:
  auto Address() -> uint64_t { return (uint64_t)addr_; }
  auto Length() -> uint32_t { return length_; }
  auto RKey() -> uint32_t { return rkey_; }

 private:
  void* addr_;
  uint32_t length_;
  uint32_t rkey_;
};

class LocalMR {
  friend class PD;

 public:
  ~LocalMR() {
    if (mr_ == nullptr) {
      return;
    }
    if (auto rc = ibv_dereg_mr(mr_); rc != 0) {
      ERROR("fail to deregiser memory region with (addr: {}, length: {}) because \"{}\"", mr_->addr,
            mr_->length, util::ErrnoString());
    }
  }

  // movable
  LocalMR(LocalMR&& other) : mr_(std::exchange(other.mr_, nullptr)){};
  auto operator=(LocalMR&& other) -> LocalMR& {
    mr_ = std::exchange(other.mr_, nullptr);
    return *this;
  };

  auto ToRemoteMR() -> RemoteMR { return RemoteMR(mr_->addr, mr_->length, mr_->rkey); }

 private:  //! WARN: can only be created by PD
  LocalMR(ibv_mr* mr = nullptr) : mr_(mr) {}

  // nocopyable
  LocalMR(const LocalMR&) = delete;
  auto operator=(const LocalMR&) -> LocalMR& = delete;

 public:
  auto Address() const -> uint64_t { return (uint64_t)mr_->addr; }
  auto Length() const -> uint32_t { return mr_->length; }
  auto RKey() const -> uint32_t { return mr_->rkey; }
  auto LKey() const -> uint32_t { return mr_->lkey; }

 private:
  ibv_mr* mr_;
};

class PD {
  friend class QP;

 public:
  PD(ibv_context* ctx) {
    if (pd_ = ibv_alloc_pd(ctx); pd_ == nullptr) {
      ERROR("fail to allocate pd because \"{}\"", util::ErrnoString());
      throw std::runtime_error("fail to allocate pd");
    }
  }
  ~PD() {
    if (pd_ == nullptr) {
      return;
    }
    if (auto rc = ibv_dealloc_pd(pd_); rc != 0) {
      ERROR("fail to deallocate pd because \"{}\"", util::ErrnoString());
    }
  }
  // movable
  PD(PD&& other) : pd_(std::exchange(other.pd_, nullptr)) {}
  auto operator=(PD&& other) -> PD& {
    pd_ = std::exchange(other.pd_, nullptr);
    return *this;
  }

 private:
  // nocopyable
  PD(const PD&) = delete;
  auto operator=(const PD&) -> PD& = delete;

 public:
  auto RegisterMemory(void* buffer, uint32_t length, int access) -> LocalMR* {
    TRACE("register buffer address: {}, length: {}", buffer, length);
    auto mr = ibv_reg_mr(pd_, buffer, length, access);

    if (mr == nullptr) {
      ERROR("fail to register memory region because \"{}\"", util::ErrnoString());
      return nullptr;
    }
    return new LocalMR(mr);
  }

 private:
  ibv_pd* pd_;
};

class CQ {
  friend class QP;

 public:
  CQ(ibv_context* ctx, uint32_t n_cqe) {
    if (cq_ = ibv_create_cq(ctx, n_cqe, this, nullptr, 0); cq_ == nullptr) {
      ERROR("fail to create cq because \"{}\"", util::ErrnoString());
      throw std::runtime_error("fail to create cq");
    }
  }
  ~CQ() {
    if (cq_ == nullptr) {
      return;
    }
    if (auto rc = ibv_destroy_cq(cq_); rc != 0) {
      ERROR("fail to destroy cq because \"{}\"", util::ErrnoString());
    }
  }

  CQ(CQ&& other) : cq_(std::exchange(other.cq_, nullptr)) {}
  auto operator=(CQ&& other) -> CQ& {
    cq_ = std::exchange(other.cq_, nullptr);
    return *this;
  }

 private:
  // nocopyable
  CQ(const CQ&) = delete;
  auto operator=(const CQ&) -> CQ& = delete;

 public:
  auto Poll() -> std::optional<ibv_wc> {
    ibv_wc wc;
    auto rc = ibv_poll_cq(cq_, 1, &wc);
    if (rc < 0) {
      ERROR("fail to poll cq because \"{}\"", util::ErrnoString());
    }
    return ((rc == 1) ? std::nullopt : std::optional<ibv_wc>{wc});
  }

 private:
  ibv_cq* cq_;
};

class QP {
 public:
  QP(rdma_cm_id* id, PD* pd, CQ* rcq, CQ* scq, /*reserved for srq*/ const ibv_qp_cap& cap,
     ibv_qp_type type, bool sig_all) {
    auto attr = ibv_qp_init_attr{
        .qp_context = id,
        .send_cq = scq->cq_,
        .recv_cq = rcq->cq_,
        .srq = nullptr,
        .cap = cap,
        .qp_type = type,
        .sq_sig_all = sig_all,
    };
    if (auto rc = rdma_create_qp(id, pd->pd_, &attr); rc != 0) {
      ERROR("fail to create qp because \"{}\"", util::ErrnoString());
      throw std::runtime_error("fail to create qp");
    }
    qp_ = id->qp;
  }
  ~QP() {
    if (qp_ == nullptr) {
      return;
    }
    if (auto rc = ibv_destroy_qp(qp_); rc != 0) {
      ERROR("fail to destroy qp because \"{}\"", util::ErrnoString());
    }
  }

  // movable
  QP(QP&& other) : qp_(std::exchange(other.qp_, nullptr)) {}
  auto operator=(QP&& other) -> QP& {
    qp_ = std::exchange(other.qp_, nullptr);
    return *this;
  }

 public:
  auto PostSend(ibv_send_wr* wrs) -> ibv_send_wr* {
    ibv_send_wr* bad_wr = nullptr;
    if (auto rc = ibv_post_send(qp_, wrs, &bad_wr); rc != 0) {
      ERROR("fail to post wrs because \"{}\"", util::ErrnoString());
    }
    return bad_wr;
  }
  auto PostRecv(ibv_recv_wr* wrs) -> ibv_recv_wr* {
    ibv_recv_wr* bad_wr = nullptr;
    // ibv_post_srq_recv
    if (auto rc = ibv_post_recv(qp_, wrs, &bad_wr); rc != 0) {
      ERROR("fail to post wrs because \"{}\"", util::ErrnoString());
      return nullptr;
    }
    return bad_wr;
  }

 private:
  // nocopyable
  QP(const QP&) = delete;
  auto operator=(const QP&) -> QP& = delete;

 private:
  ibv_qp* qp_;
};

struct ConnCtxInitAttr {
  uint32_t max_cqe;
  uint32_t max_send_wr;
  uint32_t max_recv_wr;
  uint32_t max_send_sge;
  uint32_t max_recv_sge;
  uint32_t max_inline_data;
  ibv_qp_type qp_type;
  bool sig_all;
  bool use_same_cq;
  bool use_srq;
};

class ConnContext {
 public:
  ~ConnContext() {
    if (qp_ != nullptr) {
      delete qp_;
    }
    if (not(scq_ == rcq_) and rcq_ != nullptr) {
      delete rcq_;
    }
    if (scq_ != nullptr) {
      delete scq_;
    }
    if (pd_ != nullptr) {
      delete pd_;
    }
  }

  ConnContext(ConnContext&& other) {
    pd_ = std::exchange(other.pd_, nullptr);
    scq_ = std::exchange(other.scq_, nullptr);
    rcq_ = std::exchange(other.rcq_, nullptr);
    qp_ = std::exchange(other.qp_, nullptr);
  }
  auto operator=(ConnContext&& other) -> ConnContext& {
    pd_ = std::exchange(other.pd_, nullptr);
    scq_ = std::exchange(other.scq_, nullptr);
    rcq_ = std::exchange(other.rcq_, nullptr);
    qp_ = std::exchange(other.qp_, nullptr);
    return *this;
  }

 public:
  //! This is a builder for rdmacm, quite simple.
  //! The caller shall take the responsibility to release the context.
  static auto Builder(rdma_cm_id* id, const ConnCtxInitAttr& attr) -> ConnContext* {
    try {
      auto ctx = new ConnContext(id, attr);
      return ctx;
    } catch (const std::runtime_error& e) {
      ERROR("fail to build connection context because \"{}\"", e.what());
    }
    return nullptr;
  }

 public:
  auto GetPD() -> PD* { return pd_; }
  auto GetSCQ() -> CQ* { return scq_; }
  auto GetRCQ() -> CQ* { return rcq_; }
  auto GetQP() -> QP* { return qp_; }

  auto IsSharedCQ() -> bool { return scq_ == rcq_; }
  auto GetCQ() -> CQ* {
    assert(IsSharedCQ());
    return scq_;
  }

 private:
  ConnContext(rdma_cm_id* id, const ConnCtxInitAttr& attr) {
    pd_ = new PD(id->verbs);
    if (attr.use_same_cq) {
      scq_ = rcq_ = new CQ(id->verbs, attr.max_cqe);
    } else {
      scq_ = new CQ(id->verbs, attr.max_cqe);
      rcq_ = new CQ(id->verbs, attr.max_cqe);
    }
    qp_ = new QP(id, pd_, scq_, rcq_,
                 ibv_qp_cap{
                     .max_send_wr = attr.max_send_wr,
                     .max_recv_wr = attr.max_recv_wr,
                     .max_send_sge = attr.max_send_sge,
                     .max_recv_sge = attr.max_recv_sge,
                     .max_inline_data = attr.max_inline_data,
                 },
                 attr.qp_type, attr.sig_all);
  }

  // nocopyable
  ConnContext(const ConnContext&) = delete;
  auto operator=(const ConnContext&) -> ConnContext& = delete;

 private:
  PD* pd_;
  CQ* scq_;
  CQ* rcq_;
  // SRQ
  QP* qp_;
};

}  // namespace sc::rdma