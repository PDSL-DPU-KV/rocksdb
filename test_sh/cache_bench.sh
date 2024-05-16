bench_file_path="$(dirname $PWD)/build/cache_bench"
cache_size=33554432
ops_per_thread=10000000
threads=1
num_shard_bits=0
insert_percent=20
lookup_insert_percent=30
lookup_percent=40
erase_percent=10

$bench_file_path \
--secondary_cache_uri="remote_secondary_cache://\
capacity=1000000; \
max_value_size=10000; \
threads=1; \
addr=192.168.200.11; \
port=10086" \
--threads=$threads \
--ops_per_thread=$ops_per_thread \
--cache_size=$cache_size \
--num_shard_bits=$num_shard_bits \
--insert_percent=$insert_percent \
--lookup_insert_percent=$lookup_insert_percent \
--lookup_percent=$lookup_percent \
--erase_percent=$erase_percent \
&& \
$bench_file_path \
--threads=$threads \
--ops_per_thread=$ops_per_thread \
--cache_size=$cache_size \
--num_shard_bits=$num_shard_bits \
--insert_percent=$insert_percent \
--lookup_insert_percent=$lookup_insert_percent \
--lookup_percent=$lookup_percent \
--erase_percent=$erase_percent
