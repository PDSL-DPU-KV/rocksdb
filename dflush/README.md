#### 不打开 HDFS 选项
##### 编译步骤
1. 安装 grpc 并修改 `CMakeLists.txt` 的 grpc 路径，修改本地目录下的 `.bashrc` 文件在文件末尾添加 `exportLD_LIBRARY_PATH=/home/zqy2023/lib/grpc/lib:$LD_LIBRARY_PATH` 后执行 `source .bashrc`；
2. 执行 `plugin/nas` 的 `set_up.sh`；
3. 修改 CMakeLists 中的 `/home/zqy2023` 路径，并且在 Arm 端需要额外把 X86 路径修改为 `aarch64-linux-gnu`；
4. 修改 `cmake_configure.sh` 将 `WITH_HDFS` 参数置为 0，计算端将 `WITH_DFLUSH` 置为 1；
5. Arm 端额外进入 `dflush` 路径执行一次 `make`；
6. 执行 `./cmake_configure.sh` 和 `./cmake_build.sh`，其中 Arm 端需要额外执行 `./cmake_build dpu_server`；
7. 计算端修改 `test_sh/test.sh` 中的 db 和 wal 路径。
##### 执行步骤
1. 先在 Arm 端执行 `./dpu_server`，再在计算端执行 `./test.sh` 即可。

#### 打开 HDFS 选项
##### 编译步骤
1. 前两步与不打开 HDFS 选项一致；
2. 安装 `hadoop` ，修改 ` plugin/hdfs/setup. sh ` 中的 `HADOOP_HOME` 路径到安装的 `hadoop目录`，如果为 Arm 端，则需要修改 `JAVA_HOME` 为 `/usr/lib/jvm/java-11-openjdk-arm64/` ，执行命令 `source ./plugin/hdfs/setup.sh`（需要每一次重启都执行一次 source）；
3. 修改 `cmake_configure.sh` 中的 `WITH_HDFS` 为 1；
4. 修改 `include/rocksdb/options.h` 中的 `csa_addresss` 和 `pro_cp_addresss` 路径为执行远端 compaction 的机器，S21 为 `192.168.200.21:port`，DPU21 为 `192.168.202.21:port`；
7. 计算端修改 `test_sh/test.sh` 中的 `db` 和 `wal_dir` 路径，并将 `allow_remote_compaction` 置为 `true`；
8. 存储端修改 `db/compaction/remote_compaction/utils.h` 的文件打开路径为自己的路径，并在 `test_sh` 路径下执行 `ln -s /proc/meminfo meminfo`；
9. 执行 `cmake_configure.sh` 后进入 `build` 编译 `make csa_server procp_server`，如果是计算端那么只需要编译 `db_bench`。

##### 执行步骤
1. 压缩端（S21），依次执行 `./procp_server ./csa_server`，再启动计算端（S20）`./test.sh`。

