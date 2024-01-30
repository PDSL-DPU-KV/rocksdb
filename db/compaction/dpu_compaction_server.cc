#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <strings.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

#include "dpu_compaction_common.h"
#include "env/nas_env.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/table.h"
#include "util/thread_pool.h"

class CompactionServer {
 public:
  CompactionServer(const char *ip, int port, int thread_pool_size,
                   const char *fs_addr)
      : pool(thread_pool_size) {
    serv_addr.sin_family = AF_INET;      // AF_INET address
    serv_addr.sin_port = htons(port);    // setup port(host -> networks)
    inet_aton(ip, &serv_addr.sin_addr);  // ip address
    if (!strcmp(fs_addr, "none")) {
      env = std::shared_ptr<rocksdb::Env>(rocksdb::Env::Default());
    } else {
      rpc_engine = new RPCEngine(std::string(fs_addr));
      env = NewCompositeEnv(rocksdb::NewRemoteFileSystem(rpc_engine));
    }
  };
  ~CompactionServer() {
    rdma_destroy_id(cm_server_id);
    rdma_destroy_event_channel(cm_event_channel);
  };

  void Listen() {
    int ret;
    cm_event_channel = rdma_create_event_channel();
    if (!cm_event_channel) {
      printf("fail to create event channel!\n");
      exit(-1);
    }
    TEST_NZ(rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(cm_server_id, (struct sockaddr *)&serv_addr));
    // non-blocking, a new connection will trigger a cm event.
    TEST_NZ(rdma_listen(cm_server_id, 128));
  }

  void ReceiveParam(struct rdma_cm_id *id, std::string *db_name, size_t *job_id,
                    std::string *input) {
    struct ibv_wc wc;
    struct rdma_connection *conn = (struct rdma_connection *)id->context;
    // wait for post send, already pass buffer to the client, blocking call
    if (process_work_completion_events(id->recv_cq_channel, &wc, 1) != 1) {
      printf("Failed to receive param from client!\n");
      return;
    }
    // now compaction params are in the local buffer of the connection
    char *ptr = conn->recv_buf;
    size_t name_size = *(size_t *)ptr;
    ptr += sizeof(size_t);
    *db_name = std::string(ptr, name_size);
    ptr += db_name->size();
    *job_id = *(size_t *)ptr;
    ptr += sizeof(size_t);
    size_t input_size = *(size_t *)ptr;
    ptr += sizeof(size_t);
    *input = std::string(ptr, input_size);
    size_t total_size = 3 * sizeof(size_t) + db_name->size() + input->size();
    printf("total size: %ld, db_name: %s, job_id: %ld, input_size: %ld\n",
           total_size, db_name->c_str(), *job_id, input->size());
  }

  void SendResult(struct rdma_cm_id *id, std::string &output) {
    struct ibv_wc wc;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    struct rdma_connection *conn = (struct rdma_connection *)id->context;
    conn->send_buf = (char *)calloc(1, MAX_BUF_SIZE);
    char *ptr = conn->send_buf;
    *(size_t *)ptr = output.size();
    if (output.size() + sizeof(size_t) > MAX_BUF_SIZE) {
      printf("output size bigger than MAX_BUF_SIZE!, %ld\n",
             output.size() + sizeof(size_t));
    }
    ptr += sizeof(size_t);
    memcpy(ptr, output.c_str(), output.size());
    TEST_Z(conn->send_mr =
               ibv_reg_mr(conn->id->pd, conn->send_buf, MAX_BUF_SIZE,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_WRITE));

    sge.addr = (uintptr_t)conn->send_mr->addr;
    sge.length = conn->send_mr->length;
    sge.lkey = conn->send_mr->lkey;
    memset(&wr, 0, sizeof(wr));
    // wr.wr_id = (uintptr_t)ctx;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = conn->remote_buffer_attr->address;
    wr.wr.rdma.rkey = conn->remote_buffer_attr->stag.remote_stag;
    TEST_NZ(ibv_post_send(conn->id->qp, &wr, &bad_wr));
    // post send
    if (process_work_completion_events(id->send_cq_channel, &wc, 1) != 1) {
      printf("We failed to get 1 work completions (RDMA_SEND to client)\n");
    }
  }

  void Job(struct rdma_cm_id *id) {
    std::string input, db_name, output;
    size_t job_id;
    ReceiveParam(id, &db_name, &job_id, &input);
    // open and compact
    rocksdb::OpenAndCompactOptions options;
    rocksdb::CompactionServiceOptionsOverride options_override;
    rocksdb::BlockBasedTableOptions block_based_table_options;
    options_override.table_factory = std::shared_ptr<rocksdb::TableFactory>(
        NewBlockBasedTableFactory(block_based_table_options));
    options_override.env = env.get();
    rocksdb::Status s = rocksdb::DB::OpenAndCompact(
        options, db_name, db_name + "/" + std::to_string(job_id), input,
        &output, options_override);
    if (!s.ok()) {
      printf("code: %d, message: %s\n", s.code(), s.getState());
      output = "failed";
    }
    SendResult(id, output);
  }

  void Progress() {
    struct rdma_cm_event *cm_event = nullptr;
    struct rdma_cm_id *cm_client_id = nullptr;
    while (true) {
      // blocking call
      rdma_get_cm_event(cm_event_channel, &cm_event);
      if (cm_event->status != 0) {
        printf("CM event has non zero status: %d\n", cm_event->status);
        rdma_ack_cm_event(cm_event);
      }
      printf("A new %s type event is received!\n",
             rdma_event_str(cm_event->event));
      switch (cm_event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
          // allocate memory sources for client
          OnConnectionRequest(cm_event->id, cm_event->param.conn.private_data);
          rdma_ack_cm_event(cm_event);
          break;
        case RDMA_CM_EVENT_ESTABLISHED:
          // enqueue a compaction job
          cm_client_id = cm_event->id;
          rdma_ack_cm_event(cm_event);
          OnEstablish(cm_client_id);
          break;
        case RDMA_CM_EVENT_DISCONNECTED:
          cm_client_id = cm_event->id;
          rdma_ack_cm_event(cm_event);
          OnDisconnect(cm_client_id);
          break;
        default:
          break;
      }
    }
  }

 private:
  void OnDisconnect(struct rdma_cm_id *client_id) {
    struct rdma_connection *conn = (struct rdma_connection *)client_id->context;
    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);
    free(conn->send_buf);
    free(conn->recv_buf);
    free(conn->recv_buf_attr);
    free(conn->remote_buffer_attr);
    free(conn);
  }

  void OnEstablish(struct rdma_cm_id *client_id) {
    pool.enqueue(&CompactionServer::Job, this, client_id);
  }

  void OnConnectionRequest(struct rdma_cm_id *client_id, const void *param) {
    struct rdma_connection *conn =
        (struct rdma_connection *)malloc(sizeof(struct rdma_connection));
    conn->id = client_id;
    client_id->context = conn;
    conn->remote_buffer_attr =
        (struct rdma_buffer_attr *)malloc(sizeof(struct rdma_buffer_attr));
    conn->remote_buffer_attr->address =
        ((struct rdma_buffer_attr *)param)->address;
    conn->remote_buffer_attr->length =
        ((struct rdma_buffer_attr *)param)->length;
    conn->remote_buffer_attr->stag.remote_stag =
        ((struct rdma_buffer_attr *)param)->stag.local_stag;
    show_buffer_attr(conn->remote_buffer_attr, false);
    // create resources for the client
    CreateConnection(conn);
    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.responder_resources = 16;
    cm_params.initiator_depth = 16;
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;  // '7' indicates retry infinitely
    cm_params.private_data = (void *)conn->recv_buf_attr;
    cm_params.private_data_len = sizeof(struct rdma_buffer_attr);
    TEST_NZ(rdma_accept(conn->id, &cm_params));
  }

  void CreateConnection(struct rdma_connection *conn) {
    // create pd, cq, qp for the client
    TEST_Z(conn->id->pd = ibv_alloc_pd(conn->id->verbs));
    TEST_Z(conn->id->recv_cq_channel =
               ibv_create_comp_channel(conn->id->verbs));
    conn->id->send_cq_channel = conn->id->recv_cq_channel;
    TEST_Z(conn->id->recv_cq =
               ibv_create_cq(conn->id->verbs, 3 * MAX_Q_NUM, NULL,
                             conn->id->recv_cq_channel, 0));
    conn->id->send_cq = conn->id->recv_cq;
    TEST_NZ(ibv_req_notify_cq(conn->id->recv_cq, 0));
    struct ibv_qp_init_attr attr;
    bzero(&attr, sizeof(struct ibv_qp_init_attr));
    attr.cap.max_recv_sge = 1;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_wr = MAX_Q_NUM;
    attr.cap.max_send_wr = MAX_Q_NUM;
    attr.qp_type = IBV_QPT_RC;
    attr.recv_cq = conn->id->recv_cq;
    attr.send_cq = conn->id->send_cq;
    TEST_NZ(rdma_create_qp(conn->id, conn->id->pd, &attr));
    // register buffer for receive compaction param
    conn->recv_buf = (char *)malloc(MAX_BUF_SIZE);
    TEST_Z(conn->recv_mr =
               ibv_reg_mr(conn->id->pd, conn->recv_buf, MAX_BUF_SIZE,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_WRITE));
    conn->recv_buf_attr =
        (struct rdma_buffer_attr *)malloc(sizeof(rdma_buffer_attr));
    conn->recv_buf_attr->address = (uint64_t)conn->recv_mr->addr;
    conn->recv_buf_attr->length = (uint32_t)conn->recv_mr->length;
    conn->recv_buf_attr->stag.local_stag = (uint32_t)conn->recv_mr->lkey;
    show_buffer_attr(conn->recv_buf_attr, true);
    // pre-post the receive buffer
    struct ibv_sge recv_sge;
    recv_sge.addr = (uint64_t)conn->recv_mr->addr;
    recv_sge.length = (uint32_t)conn->recv_mr->length;
    recv_sge.lkey = (uint32_t)conn->recv_mr->lkey;
    struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
    bzero(&recv_wr, sizeof(recv_wr));
    recv_wr.num_sge = 1;
    recv_wr.sg_list = &recv_sge;
    TEST_NZ(ibv_post_recv(conn->id->qp, &recv_wr, &bad_recv_wr));
  }

 private:
  struct sockaddr_in serv_addr;
  struct rdma_event_channel *cm_event_channel{nullptr};
  struct rdma_cm_id *cm_server_id{nullptr};
  ThreadPool pool;
  RPCEngine *rpc_engine;
  std::shared_ptr<ROCKSDB_NAMESPACE::Env> env;
  std::atomic_int compaction_num{0};
};

int main(int argc, char *argv[]) {
  if (argc < 4) {
    printf("Usage: ./server <ip> <port> <fs_addr>\n");
    exit(0);
  }
  CompactionServer server(argv[1], strtol(argv[2], NULL, 10), 16, argv[3]);
  server.Listen();
  server.Progress();
}
