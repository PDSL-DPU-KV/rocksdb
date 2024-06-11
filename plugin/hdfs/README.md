This directory contains the hdfs extensions needed to make rocksdb store
files in HDFS.

# HDFS Cluster Topology

s11: DataNode, NameNode

s12: DataNode, SecondaryNameNode

s13: DataNode

Run `start-dfs.sh` on s11 to start HDFS cluster.
Run `tunnel.sh` on localhost to export website.

When finish test, run `stop-dfs.sh` on s11 to stop HDFS cluster

# How to use HDFS in RocksDB

You need to setup enviorment variable for HDFS. See `setup.sh` and change the path to your own.

Modify `cmake_configure.sh` to set `-DWITH_HDFS=1`.

# How to run CaaS-LSM

Modify `include/rocksdb/options.h` to set `hdfs_address`,`csa_address`,`pro_cp_address`. And recompile rocksdb.

First, start `csa_server` and `procp_server` on dpu10 (BF2) or dpu21(BF3) or s21 (CPU). Notice that if you want to run on other servers, you need install hadoop, openjdk-11-jdk and grpc on them.

Then, modify `test.sh` to set parameter `allow_remote_compaction=true` and run it.

# RocksDB dependencies

cd `./plugin/nas/` and run `./setup.sh` to install mercury and libfabrics. (If don't want to use nas env, use commit `8147863`)

Then, you need to install grpc in your local path. 

Modify `CMakeLists.txt` to point to the above libraries.

You may also need to install compression libraries.


