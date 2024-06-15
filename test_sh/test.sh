#! /bin/bash

### Test parameters
key_array=(16)
value_array=(1024)
block_array=(4096)
op_array=("snappy")
f_array=("8")
wn_array=("8")
t_array=("16")
db_array=("1")

### Benchmark parameters
db="/home/lsc/hadoop/zqy2023/rocksdb_test"
num_multi_db="1"
wal_dir="/home/lsc/hadoop/zqy2023/rocksdb_test"
use_existing_db="true"
threads="32"
benchmarks="fillrandom,stats,wait,stats"
num="100000000"
#reads="10000000"
key_size="16"
value_size="100"
batch_size="4"

### MemTable parameters
memtablerep="skip_list"
max_write_buffer_number="2"
write_buffer_size="134217728"

### Compression parameters
block_size="4096"
compression_type="none" #"none,zlib,lz4,dpu"
#min_level_to_compress="2"
#compression_per_level="none,none,lz4,lz4,zstd"
#checksum_type="1"

### Compaction parameters
allow_remote_compaction="false"
#max_background_jobs="2"
max_background_flushes="8"
max_background_compactions="16"
subcompactions="64"
#level_compaction_dynamic_level_bytes="true"
disable_auto_compactions="false"
use_direct_io_for_flush_and_compaction="true"
use_direct_reads="true"
level0_slowdown_writes_trigger="20"
level0_stop_writes_trigger="36"

### Write optimization parameters
#disable_wal="true"
unordered_write="false"
enable_pipelined_write="true"
allow_concurrent_memtable_write="true"

### Read optimization parameters
bloom_bits=10
#cache_size=1073741824

### BlobDB parameters
# enable_blob_files="true"
# blob_compression_type="snappy"

### Env paramemters
use_nas="false"
fs_svr_addr="ofi+verbs://192.168.2.21:12345"

### Statistics parameters
#target_io="nvme3c3n1"
#target_net="enp134s0f1np1"
report_bg_io_stats="true"
report_interval_seconds="1"
report_csv="true"
statistics="true"
#stats_level="3"
#stats_dump_period_sec="10"
#stats_interval="1"
#stats_interval_seconds="10"
#histogram="true"
#show_table_properties="true"
perf_level="3"

### Tuner parameters
TEA_enable="false"
FEA_enable="false"

tdate=$(date "+%Y_%m_%d_%H_%M_%S")

bench_file_path="$(dirname $PWD )/build/db_bench"

bench_file_dir="$(dirname $PWD )/test_sh"

if [ ! -f "${bench_file_path}" ];then
bench_file_path="$PWD/db_bench"
bench_file_dir="$PWD"
fi

if [ ! -f "${bench_file_path}" ];then
echo "Error:${bench_file_path} or $(dirname $PWD )/build/db_bench not find!"
exit 1
fi

const_params=""

function FILL_PARAMS() {

    if [ -n "$db" ];then
        const_params=$const_params"--db=$db "
    fi

    if [ -n "$num_multi_db" ];then
        const_params=$const_params"--num_multi_db=$num_multi_db "
    fi

    if [ -n "$memtablerep" ];then
        const_params=$const_params"--memtablerep=$memtablerep "
    fi

    if [ -n "$allow_concurrent_memtable_write" ];then
        const_params=$const_params"--allow_concurrent_memtable_write=$allow_concurrent_memtable_write "
    fi

    if [ -n "$wal_dir" ];then
        const_params=$const_params"--wal_dir=$wal_dir "
    fi

    if [ -n "$use_existing_db" ];then
        const_params=$const_params"--use_existing_db=$use_existing_db "
    fi

    if [ -n "$benchmarks" ];then
        const_params=$const_params"--benchmarks=$benchmarks "
    fi

    if [ -n "$num" ];then
        const_params=$const_params"--num=$num "
    fi
    
    if [ -n "$reads" ];then
        const_params=$const_params"--reads=$reads "
    fi

    if [ -n "$key_size" ];then
        const_params=$const_params"--key_size=$key_size "
    fi

    if [ -n "$value_size" ];then
        const_params=$const_params"--value_size=$value_size "
    fi

    if [ -n "$batch_size" ];then
        const_params=$const_params"--batch_size=$batch_size "
    fi

    if [ -n "$block_size" ];then
        const_params=$const_params"--block_size=$block_size "
    fi

    if [ -n "$checksum_type" ];then
        const_params=$const_params"--checksum_type=$checksum_type "
    fi

    if [ -n "$compression_type" ];then
        const_params=$const_params"--compression_type=$compression_type "
    fi

    if [ -n "$min_level_to_compress" ];then
        const_params=$const_params"--min_level_to_compress=$min_level_to_compress "
    fi

    if [ -n "$compression_per_level" ];then
        const_params=$const_params"--compression_per_level=$compression_per_level "
    fi

    if [ -n "$level_compaction_dynamic_level_bytes" ];then
        const_params=$const_params"--level_compaction_dynamic_level_bytes=$level_compaction_dynamic_level_bytes "
    fi

    if [ -n "$threads" ];then
        const_params=$const_params"--threads=$threads "
    fi

    if [ -n "$max_write_buffer_number" ];then
        const_params=$const_params"--max_write_buffer_number=$max_write_buffer_number "
    fi

    if [ -n "$write_buffer_size" ];then
        const_params=$const_params"--write_buffer_size=$write_buffer_size "
    fi

    if [ -n "$allow_remote_compaction" ];then
        const_params=$const_params"--allow_remote_compaction=$allow_remote_compaction "
    fi

    if [ -n "$max_background_jobs" ];then
        const_params=$const_params"--max_background_jobs=$max_background_jobs "
    fi

    if [ -n "$max_background_flushes" ];then
        const_params=$const_params"--max_background_flushes=$max_background_flushes "
    fi

    if [ -n "$max_background_compactions" ];then
        const_params=$const_params"--max_background_compactions=$max_background_compactions "
    fi

    if [ -n "$subcompactions" ];then
        const_params=$const_params"--subcompactions=$subcompactions "
    fi
    
    if [ -n "$level0_slowdown_writes_trigger" ];then
        const_params=$const_params"--level0_slowdown_writes_trigger=$level0_slowdown_writes_trigger "
    fi

    if [ -n "$level0_stop_writes_trigger" ];then
        const_params=$const_params"--level0_stop_writes_trigger=$level0_stop_writes_trigger "
    fi

    if [ -n "$unordered_write" ];then
        const_params=$const_params"--unordered_write=$unordered_write "
    fi

    if [ -n "$enable_pipelined_write" ];then
        const_params=$const_params"--enable_pipelined_write=$enable_pipelined_write "
    fi

    if [ -n "$disable_wal" ];then
        const_params=$const_params"--disable_wal=$disable_wal "
    fi

    if [ -n "$disable_auto_compactions" ];then
        const_params=$const_params"--disable_auto_compactions=$disable_auto_compactions "
    fi

    if [ -n "$use_direct_io_for_flush_and_compaction" ];then
	const_params=$const_params"--use_direct_io_for_flush_and_compaction=$use_direct_io_for_flush_and_compaction "
    fi

    if [ -n "$use_direct_reads" ];then
        const_params=$const_params"--use_direct_reads=$use_direct_reads "
    fi

    if [ -n "$bloom_bits" ];then
        const_params=$const_params"--bloom_bits=$bloom_bits "
    fi

    if [ -n "$cache_size" ];then
            const_params=$const_params"--cache_size=$cache_size "
    fi

    if [ -n "$enable_blob_files" ];then
        const_params=$const_params"--enable_blob_files=$enable_blob_files "
    fi
    
    if [ -n "$blob_compression_type" ];then
        const_params=$const_params"--blob_compression_type=$blob_compression_type "
    fi

    if [ -n "$use_nas" ];then
        const_params=$const_params"--use_nas=$use_nas "
    fi
    
    if [ -n "$fs_svr_addr" ];then
        const_params=$const_params"--fs_svr_addr=$fs_svr_addr "
    fi

    if [ -n "$report_bg_io_stats" ];then
        const_params=$const_params"--report_bg_io_stats=$report_bg_io_stats "
    fi

    if [ -n "$report_interval_seconds" ];then
        const_params=$const_params"--report_interval_seconds=$report_interval_seconds "
    fi

    if [ -n "$report_csv" ];then
        const_params=$const_params"--report_csv=$report_csv "
    fi

    if [ -n "$statistics" ];then
        const_params=$const_params"--statistics=$statistics "
    fi

    if [ -n "$stats_level" ];then
        const_params=$const_params"--stats_level=$stats_level "
    fi

    if [ -n "$perf_level" ];then
        const_params=$const_params"--perf_level=$perf_level "
    fi

    if [ -n "$stats_dump_period_sec" ];then
        const_params=$const_params"--stats_dump_period_sec=$stats_dump_period_sec "
    fi

    if [ -n "$stats_interval" ];then
        const_params=$const_params"--stats_interval=$stats_interval "
    fi

    if [ -n "$stats_per_interval" ];then
        const_params=$const_params"--stats_per_interval=$stats_per_interval "
    fi

    if [ -n "$stats_interval_seconds" ];then
        const_params=$const_params"--stats_interval_seconds=$stats_interval_seconds "
    fi

    if [ -n "$histogram" ];then
        const_params=$const_params"--histogram=$histogram "
    fi

    if [ -n "$TEA_enable" ];then
        const_params=$const_params"--TEA_enable=$TEA_enable "
    fi

    if [ -n "$FEA_enable" ];then
        const_params=$const_params"--FEA_enable=$FEA_enable "
    fi
}

function MONITOR_CPU() {
    sleep 1
    while [ -z "$(pidof $1)" ]
    do
        sleep 0.5
    done
    awk_str="\$1==\"top\",\$1==\"MiB\"&&\$2==\"Swap:\";\$1==\"PID\"||\$1~/^[0-9]*$/{print \$9,\$10,\$11,\$12}"
    top -Hp "$(pidof $1)" -b -d 1 -o -COMMAND | awk "$awk_str" > $2 &
    echo $!
}

function MONITOR_IO() {
    awk_str="\$1==\"Device\"||\$1==\"$target_io\"{print \$1,\$2,\$3,\$8,\$9,\$21 | \"tee $1\"}"
    iostat -dxm 1 | awk "$awk_str" > /dev/null &
    echo $!
}

function MONITOR_NET() {
    mlnx_perf -i $target_net | tee $1 > /dev/null &
    echo $!
}

RUN_ONE_TEST() {
    const_params=""
    FILL_PARAMS
    #cmd="sudo $bench_file_path $const_params"
    cmd="$bench_file_path $const_params | tee -a out.out"
    #cmd="sudo perf record -F 99 -g --call-graph dwarf -- $bench_file_path $const_params | tee -a out.out"
    if [ "$1" == "numa" ];then
        #cmd="sudo numactl -N 1 $bench_file_path $const_params"
        cmd=" $bench_file_path $const_params | tee -a out.out"
        #cmd="sudo perf record -F 99 -g --call-graph dwarf -- $bench_file_path $const_params"
    fi
    echo $cmd >>out.out
    echo $cmd
    eval $cmd
}

CLEAN_CACHE() {
    if [ -n "$db" ];then
       rm -f $db/*
    fi
    sleep 2
    sync
    # echo 3 | tee -a /proc/sys/vm/drop_caches > /dev/null
    sleep 2
}

REMOUNT_SSD() {
    umount /mnt/ssd
    mkfs.ext4 /dev/nvme3n1
    mount /dev/nvme3n1 /mnt/ssd
}

COPY_OUT_FILE() {
    mkdir $bench_file_dir/result_overall_$tdate > /dev/null 2>&1
    res_dir=$bench_file_dir/result_overall_$tdate/$1_$2
    mkdir $res_dir > /dev/null 2>&1
    \cp -f $bench_file_dir/out.out $res_dir/
    \cp -f $bench_file_dir/*.csv $res_dir/
    \rm -f $bench_file_dir/out.out
    \rm -f $bench_file_dir/*.csv
    # \cp -f $bench_file_dir/cpu.log $res_dir/
    # \cp -f $bench_file_dir/net.log $res_dir/
    # \cp -f $bench_file_dir/io.log $res_dir/
    #\cp -f $db/OPTIONS-* $res_dir/
    #\cp -f $db/LOG $res_dir/
    # \rm -f $bench_file_dir/*.log
    # \rm -f $bench_file_dir/*.csv
    # \rm -f $bench_file_dir/lsm_stats
}

LOAD() {
    benchmarks="fillrandom,stats,wait,stats"
    num="$1"
    threads="$2"

    RUN_ONE_TEST
    if [ $? -ne 0 ];then
        exit 1
    fi
    sleep 5
}

FILLRANDOM() {
    benchmarks="fillrandom,stats"
    num="$1"
    use_existing_db="$2"

    RUN_ONE_TEST numa "0-32"
    #RUN_ONE_TEST
    if [ $? -ne 0 ];then
        exit 1
    fi
    sleep 5
    COPY_OUT_FILE threads $3
}

READRANDOM() {
    benchmarks="readrandom,stats"
    reads="$1"
    use_existing_db="$2"

    RUN_ONE_TEST numa "0-32"
    if [ $? -ne 0 ];then
        exit 1
    fi
    sleep 5
    COPY_OUT_FILE readrandom $3
}

RUN_ALL_TEST() {
    for op in ${op_array[@]}; do
        for t in ${t_array[@]}; do
            for wn in ${wn_array[@]}; do
                for fth in ${f_array[@]}; do
                    for ndb in ${db_array[@]}; do
                        #REMOUNT_SSD
                        CLEAN_CACHE
                        # set parameters
                        compression_type="$op"
                        blob_compression_type="$op"
                        max_background_flushes="$fth"
                        max_write_buffer_number="$wn"
                        num_multi_db="$ndb"
                        # # load data
                        # LOAD 200000000 1
                        # run benchmark
                        threads="$t"
                        ops_per_thread=$(((100000000/$threads)/$num_multi_db))
                        FILLRANDOM $ops_per_thread false $t
                        # READRANDOM 2000000 true $op
                        sleep 5
                    done
                done
            done
        done
    done
}

RUN_ALL_TEST $1

