# DFlush: DPU-offloaded Flush for Disaggregated LSM-based Key-Value Stores

## Brief Introduction 

DFlush is a novel solution that uses DPUs to offload background flush operations to reduce its CPU cost. DPUs are an appealing choice for this goal due to their cost-effectiveness, ease of programming, and widespread deployment. To fully harness the DPU’s capabilities, DFlush decomposes a flush job into fine-grained steps, mapped them to DPU hardware units, and accelerates them through pipeline, data, and channel parallelism, ensuring data-plane efficiency. DFlush also introduces an adaptive control plane that dynamically schedules flush jobs from different LSM-KVS instances based on their priority, reducing write stall and tail latency.

DFlush is implemented based on RocksDB v8.3.0 and NVIDIA BlueField-3 DPU.

## Dependencies

- Hardware
  - At least one compute machine with NVIDIA BlueField-3 DPU
  - At least one storage machine employed HDFS with SSD
- Software
  - Operating System: Ubuntu 22.04 LTS
  - Programming Language: C++ 17
  - Compiler: gcc 13.1.0
  - Libraries: ibverbs doca-common doca-dpa doca-dma 
  - Other: install hadoop and grpc in all machines
  
## Build

- Configuring `CMakeLists.txt`'s grpc path and update `.bashrc`
- Modify `plugin/hdfs/setup.h`'s `HADOOP_HOME`
- Modify `include/rocksdb/options.h`'s `csa_address` and `pro_cp_address`
- Modify `test_sh/test_single.sh`'s db path and wal_dir path
- Modify file open's path in `db/compaction/remote_compaction/utils.h`'
- Compile
```
mkdir build
./cmake_configure.sh
./cmake_build.sh db_bench csa_server procp_server dpu_server
```
- Run in storage machine
```
./procp_server
./csa_server
```
- Run in DPU
```
./dpu_server
```
- Run in compute machine
```
./test_single.sh
```

## WorkLoads and comparison objects

### WorkLoads

Modify `test_single.sh` to replace different WorkLoads.

- value_array: different value size
- rw_array: different read/write ratio
- t_array: different dowrite threads
- f_array: different flush threads
- benchmarks: different write mode(include ycsb/fillrandom/fillseq/mixgraph)

### Comparison Objects

- Pure Rocksdb with HDFS (no CaaS and no DFlush)

Modify `allow_remote_compaction` to false in `test_single.sh`
Modify `-DWITH_DFLUSH` to false in `cmake_configure.sh` and recompile

- Rocksdb with CaaS

Modify `allow_remote_compaction` to true in `test_single.sh`

- Rocksdb with DFlush

Modify `-DWITH_DFLUSH` to true in `cmake_configure.sh` and recompile

