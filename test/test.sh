# test compression

db="/mnt/ssd/test"
use_existing_db=false

bench_array=("fillrandom,stats")

benchmarks="fillrandom,stats,wait"

threads="1"
max_background_flushes="1"
max_background_compactions="16"
subcompactions="1"

key_size="16"
value_size="1000"
num="100000000"
#reads="1000000"

#compression_type="none"
#compression_ratio="0.5"
#min_level_to_compress="3"

disable_wal=true
disable_auto_compactions=false

use_direct_reads=true
use_direct_io_for_flush_and_compaction=true

report_bg_io_stats=true
#block_cache_trace_file="./block_cache_trace"

bloom_bits=10
#cache_size=0

enable_blob_files=true
enable_blob_garbage_collection=true
#blob_compression_type="zlib"

statistics=true
perf_level=3
stats_interval_seconds=10

tdate=$(date "+%Y_%m_%d_%H_%M_%S")

bench_file_path="$(dirname $PWD )/build/db_bench"
bench_file_dir="$(dirname $PWD )/test"

if [ ! -f "${bench_file_path}" ];then
bench_file_path="$PWD/db_bench"
bench_file_dir="$PWD"
fi

if [ ! -f "${bench_file_path}" ];then
echo "Error:${bench_file_path} or $(dirname $PWD )/build/db_bench not find!"
exit 1
fi

const_params=""

FILL_PARAMS() {

	if [ -n "$db" ];then
		const_params=$const_params"--db=$db "
	fi

	if [ -n "$use_existing_db" ];then
		const_params=$const_params"--use_existing_db=$use_existing_db "
	fi

	if [ -n "$benchmarks" ];then
		const_params=$const_params"--benchmarks=$benchmarks "
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

	if [ -n "$key_size" ];then
		const_params=$const_params"--key_size=$key_size "
	fi

	if [ -n "$value_size" ];then
		const_params=$const_params"--value_size=$value_size "
	fi

	if [ -n "$num" ];then
		const_params=$const_params"--num=$num "
	fi

	if [ -n "$reads" ];then
		const_params=$const_params"--reads=$reads "
	fi

	if [ -n "$compression_type" ];then
		const_params=$const_params"--compression_type=$compression_type "
	fi

	if [ -n "$compression_ratio" ];then
		const_params=$const_params"--compression_ratio=$compression_ratio "
	fi

	if [ -n "$min_level_to_compress" ];then
		const_params=$const_params"--min_level_to_compress=$min_level_to_compress "
	fi

	if [ -n "$disable_wal" ];then
		const_params=$const_params"--disable_wal=$disable_wal "
	fi

	if [ -n "$disable_auto_compactions" ];then
		const_params=$const_params"--disable_auto_compactions=$disable_auto_compactions "
	fi

	if [ -n "$use_direct_reads" ];then
		const_params=$const_params"--use_direct_reads=$use_direct_reads "
	fi

	if [ -n "$use_direct_io_for_flush_and_compaction" ];then
		const_params=$const_params"--use_direct_io_for_flush_and_compaction=$use_direct_io_for_flush_and_compaction "
	fi

	if [ -n "$report_bg_io_stats" ];then
		const_params=$const_params"--report_bg_io_stats=$report_bg_io_stats "
	fi

	if [ -n "$block_cache_trace_file" ];then
		const_params=$const_params"--block_cache_trace_file=$block_cache_trace_file "
	fi

	if [ -n "$statistics" ];then
		const_params=$const_params"--statistics=$statistics "
	fi

	if [ -n "$stats_interval_seconds" ];then
		const_params=$const_params"--stats_interval_seconds=$stats_interval_seconds "
	fi
	
	if [ -n "$perf_level" ];then
		const_params=$const_params"--perf_level=$perf_level "
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

	if [ -n "$enable_blob_garbage_collection" ];then
		const_params=$const_params"--enable_blob_garbage_collection=$enable_blob_garbage_collection "
	fi

	if [ -n "$blob_compression_type" ];then
		const_params=$const_params"--blob_compression_type=$blob_compression_type "
	fi
}

RUN_ONE_TEST() {
    const_params=""
    FILL_PARAMS
    cmd="sudo $bench_file_path $const_params | tee -a out.out"
    if [ "$1" == "numa" ];then
        #cmd="sudo numactl -N 1 $bench_file_path $const_params"
        cmd="sudo numactl -N 1 $bench_file_path $const_params | tee -a out.out"
    fi
    echo $cmd >out.out
    echo $cmd
    eval $cmd
}

CLEAN_CACHE() {
#    if [ -n "$db" ];then
#        sudo rm -f $db/*
#    fi
#    sleep 2
    sync
    echo 3 | sudo tee -a /proc/sys/vm/drop_caches > /dev/null
    sleep 2
}

COPY_OUT_FILE() {
    mkdir $bench_file_dir/result_$tdate > /dev/null 2>&1
    res_dir=$bench_file_dir/result_$tdate/$benchmarks\_$num\_$value_size\_$compression_type\_$min_level_to_compress
    mkdir $res_dir > /dev/null 2>&1
    \cp -f $bench_file_dir/out.out $res_dir/
    \cp -f $bench_file_dir/compaction.csv $res_dir/
    \cp -f $bench_file_dir/flush.csv $res_dir/
    \cp -f $db/OPTIONS-* $res_dir/
    \cp -f $db/LOG $res_dir/
}


RUN_ALL_TEST() {
    for bench in ${bench_array[@]}; do
        CLEAN_CACHE

	benchmarks="$bench"

        RUN_ONE_TEST $1
        if [ $? -ne 0 ];then
            exit 1
        fi

        COPY_OUT_FILE
        sleep 5
    done
}

RUN_ALL_TEST $1

