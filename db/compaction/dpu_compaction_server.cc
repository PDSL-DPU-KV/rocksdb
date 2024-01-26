#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/sctp.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "env/nas_env.h"
#include "rocksdb/db.h"
#include "table/block_based/block_based_table_factory.h"
#include "util/thread_pool.h"

#define MAX_BUF_SIZE (8 * 1024)

class CompactionServer {
 public:
  CompactionServer(const char *ip, int port, int thread_pool_size,
                   const char *fs_addr)
      : ip_(ip), port_(port), pool(thread_pool_size) {
    rpc_engine = new RPCEngine(std::string(fs_addr));
    env = NewCompositeEnv(rocksdb::NewRemoteFileSystem(rpc_engine));
  };
  ~CompactionServer() { delete rpc_engine; };

  void Listen() {
    sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);  // setup socket
    if (sockfd == -1) {
      printf("Create server socket Failed!\n");
      exit(1);
    }
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;       // AF_INET address
    serv_addr.sin_port = htons(port_);    // setup port(host -> networks)
    inet_aton(ip_, &serv_addr.sin_addr);  // ip address
    if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(struct sockaddr)) ==
        -1) {
      printf("bind error!\n");
      exit(1);
    }
    struct sctp_initmsg initmsg;
    memset(&initmsg, 0, sizeof(initmsg));
    initmsg.sinit_num_ostreams = 5;
    initmsg.sinit_max_instreams = 5;
    initmsg.sinit_max_attempts = 4;
    setsockopt(sockfd, IPPROTO_SCTP, SCTP_INITMSG, &initmsg, sizeof(initmsg));
    if (listen(sockfd, 128) == -1) {
      printf("listen error!\n");
      exit(1);
    }
    printf("server listening...\n");
  }

  void ReceiveParam(int client_fd, std::string *db_name, int *job_id,
                    std::string *input) {
    // receive param
    void *recv_buf = calloc(1, MAX_BUF_SIZE);
    ssize_t recv_size = 0;
    while (recv_size < MAX_BUF_SIZE) {
      recv_size =
          sctp_recvmsg(client_fd, recv_buf, MAX_BUF_SIZE, NULL, 0, 0, 0);
      if (recv_size == -1) {
        printf("recv error: %s\n", strerror(errno));
      }
    }
#ifdef COMP_DEBUG
    printf("received size: %ld\n", recv_size);
#endif
    // simple serialization
    char *ptr = (char *)recv_buf;
    ptr = (char *)recv_buf;
    size_t total_size = *(size_t *)ptr;
    ptr += sizeof(size_t);
    int name_size = *(size_t *)ptr;
    ptr += sizeof(size_t);
    *db_name = std::string(ptr, name_size);
    ptr += db_name->size();
    *job_id = *(size_t *)ptr;
    ptr += sizeof(size_t);
    int input_size = *(size_t *)ptr;
    ptr += sizeof(size_t);
#ifdef COMP_DEBUG
    printf("total size: %ld, db_name: %s, job_id: %d, input_size: %d\n",
           total_size, db_name->c_str(), *job_id, input_size);
#endif
    *input = std::string(ptr, input_size);
    free(recv_buf);
  }

  void SendResult(int client_fd, std::string &output) {
    int total_size = output.size() + sizeof(size_t);
    if (total_size > MAX_BUF_SIZE) {
      printf("result big than MAX BUF SIZE!, output size: %d\n", total_size);
    }
    void *send_buf = calloc(1, MAX_BUF_SIZE);
    *(size_t *)send_buf = output.size();
    memcpy((char *)send_buf + sizeof(size_t), output.c_str(), output.size());
    size_t send_size =
        sctp_sendmsg(client_fd, send_buf, MAX_BUF_SIZE, NULL, 0, 0, 0, 0, 0, 0);
#ifdef COMP_DEBUG
    printf("send %ld bytes to client.\n", send_size);
#endif
    free(send_buf);
  }

  void Job(int client_fd) {
    std::string db_name, input;
    int job_id;
    ReceiveParam(client_fd, &db_name, &job_id, &input);
    // do compact
    rocksdb::OpenAndCompactOptions options;
    rocksdb::CompactionServiceOptionsOverride options_override;
    options_override.table_factory = std::shared_ptr<rocksdb::TableFactory>(
        new rocksdb::BlockBasedTableFactory());
    options_override.env = env.get();
    std::string output;
    rocksdb::Status s = rocksdb::DB::OpenAndCompact(
        options, db_name, db_name + "/" + std::to_string(job_id), input,
        &output, options_override);
    if (!s.ok()) {
      printf("code: %d, message: %s\n", s.code(), s.getState());
      output = "failed";
    }
#ifdef COMP_DEBUG
    printf("compaction finished, output size: %ld\n", output.size());
#endif
    SendResult(client_fd, output);
    compaction_num.fetch_sub(1);
    close(client_fd);
  }

  void Progress() {
    while (true) {
      struct sockaddr_in remote_addr;
      socklen_t addr_size = sizeof(remote_addr);
      int client_fd;
      // accept() block until getting a request
      if ((client_fd = accept(sockfd, (struct sockaddr *)&remote_addr,
                              &addr_size)) == -1) {
        printf("fail to accept!\n");
        break;
      }
      compaction_num.fetch_add(1);
#ifdef COMP_DEBUG
      printf("accept connnection! %d, current compaction num: %d\n", client_fd,
             compaction_num.load());
#endif
      pool.enqueue(&CompactionServer::Job, this, client_fd);
    }
  }

 private:
  const char *ip_;
  int port_;
  int sockfd;
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
  CompactionServer server(argv[1], strtol(argv[2], NULL, 10), 20, argv[3]);
  server.Listen();
  server.Progress();
}
