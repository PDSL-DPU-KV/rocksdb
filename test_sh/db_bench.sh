bench_file_path="$(dirname $PWD)/build_debug/db_bench"
 
$bench_file_path --benchmarks="fillrandom,waitforcompaction,readrandom" --num=100000 \
--cache_index_and_filter_blocks --cache_size=102400 \
--threads=16 --compression_type=none \
--key_size=20 --value_size=4096 --max_value_size=8192 \
--use_remote_secondary_cache=1