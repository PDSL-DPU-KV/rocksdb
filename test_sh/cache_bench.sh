#! /bin/bash

### Test parameters
key_array=(16)
value_array=(1024)
block_array=(4096)
op_array=("none" "lz4" "zlib")

### Benchmark parameters
db="/tmp/rocksdbtest_0/db_bench"
# wal_dir="/mnt/ssd/test"
use_existing_db="true"
threads="1"
benchmarks="fillrandom,stats,waitforcompaction,readrandom,stats"
num="10000000"
#reads="10000000"
key_size="16"
value_size="100"

### Compression parameters
block_size="4096"
compression_type="none" #"none,zlib,lz4,dpu"
#min_level_to_compress="2"
#compression_per_level="none,none,lz4,lz4,zstd"

### Compaction parameters
max_background_flushes="1"
max_background_compactions="16"
subcompactions="4"
#level_compaction_dynamic_level_bytes="true"
disable_auto_compactions="false"
use_direct_io_for_flush_and_compaction="true"
use_direct_reads="false"
disable_wal="true"

### Read parameters
bloom_bits=10
cache_size=8242880

### Statistics parameters
# target_io="nvme3c3n1"
# target_net="enp134s0f1np1"
report_bg_io_stats="true"
#report_fillrandom_latency="true"
statistics="true"
#stats_level="3"
#stats_dump_period_sec="10"
#stats_interval="1"
#stats_interval_seconds="10"
#histogram="true"
#show_table_properties="true"
perf_level="3"

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

    if [ -n "$block_size" ];then
        const_params=$const_params"--block_size=$block_size "
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

    if [ -n "$max_background_flushes" ];then
        const_params=$const_params"--max_background_flushes=$max_background_flushes "
    fi

    if [ -n "$max_background_compactions" ];then
        const_params=$const_params"--max_background_compactions=$max_background_compactions "
    fi

    if [ -n "$subcompactions" ];then
        const_params=$const_params"--subcompactions=$subcompactions "
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

    if [ -n "$report_bg_io_stats" ];then
        const_params=$const_params"--report_bg_io_stats=$report_bg_io_stats "
    fi

    if [ -n "$report_fillrandom_latency" ];then
        const_params=$const_params"--report_fillrandom_latency=$report_fillrandom_latency "
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

    if [ -n "$show_table_properties" ];then
        const_params=$const_params"--show_table_properties=$show_table_properties "
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
    cmd="sudo $bench_file_path $const_params | tee -a out.out"
    if [ "$1" == "numa" ];then
        #cmd="sudo numactl -N 1 $bench_file_path $const_params"
        cmd="sudo numactl -N 0 $bench_file_path $const_params | tee -a out.out"
    fi
    echo $cmd >out.out
    echo $cmd
    eval $cmd
}

CLEAN_CACHE() {
    #if [ -n "$db" ];then
    #    sudo rm -f $db/*
    #fi
    #sleep 2
    sync
    echo 3 | sudo tee -a /proc/sys/vm/drop_caches > /dev/null
    sleep 2
}

REMOUNT_SSD() {
    sudo umount /mnt/ssd
    sudo mkfs.ext4 /dev/nvme3n1
    sudo mount /dev/nvme3n1 /mnt/ssd
}

COPY_OUT_FILE() {
    mkdir $bench_file_dir/result_overall_$tdate > /dev/null 2>&1
    res_dir=$bench_file_dir/result_overall_$tdate/$1_$2
    mkdir $res_dir > /dev/null 2>&1
    \cp -f $bench_file_dir/out.out $res_dir/
    # \cp -f $bench_file_dir/cpu.log $res_dir/
    # \cp -f $bench_file_dir/net.log $res_dir/
    # \cp -f $bench_file_dir/io.log $res_dir/
    #\cp -f $db/OPTIONS-* $res_dir/
    #\cp -f $db/LOG $res_dir/
    # \rm -f $bench_file_dir/*.log
    # \rm -f $bench_file_dir/*.csv
    # \rm -f $bench_file_dir/lsm_stats
}

FILLRANDOM() {
    benchmarks="fillrandom,stats,wait,stats"
    num="$1"
    use_existing_db="$2"

    RUN_ONE_TEST numa "0-32"
    if [ $? -ne 0 ];then
        exit 1
    fi
    sleep 5
    COPY_OUT_FILE fillrandom $3
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
	#REMOUNT_SSD
	CLEAN_CACHE
	# set parameters
	compression_type="$op"
	# run benchmark
	FILLRANDOM 20000000 false $op
        READRANDOM 2000000 true $op
	sleep 5
    done
}

RUN_ONE_TEST