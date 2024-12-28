#for i in 0 1 2 3 4 5 6 7; do
# for i in 0; do
# echo $i
# ./test_single.sh $i > test$i.out &
# done 

cd ..
rm -rf build
./cmake_configure.sh
./cmake_build.sh
cd test_sh
./test_single.sh 0 false 16 72 95 4 
./test_single.sh 0 true  16 72 95 4 
./test_single.sh 0 false 16 88 95 4 
./test_single.sh 0 true  16 88 95 4 
./test_single.sh 0 false 8 88 95 1 
./test_single.sh 0 true  8 88 95 1 

./test_ycsb.sh 0 false 16 72 95 4 
./test_ycsb.sh 0 true  16 72 95 4 
./test_ycsb.sh 0 false 16 88 95 4 
./test_ycsb.sh 0 true  16 88 95 4 
./test_ycsb.sh 0 false 8 88 95 1 
./test_ycsb.sh 0 true  8 88 95 1 


cd ..
rm -rf build
./cmake_configure_dflush.sh
./cmake_build.sh
cd test_sh
./test_single.sh 0 false 16 72 95 4 
./test_single.sh 0 true  16 72 95 4 
./test_single.sh 0 false 16 88 95 4 
./test_single.sh 0 true  16 88 95 4 
./test_single.sh 0 false 8 88 95 1 
./test_single.sh 0 true  8 88 95 1 

./test_ycsb.sh 0 false 16 72 95 4 
./test_ycsb.sh 0 true  16 72 95 4 
./test_ycsb.sh 0 false 16 88 95 4 
./test_ycsb.sh 0 true  16 88 95 4 
./test_ycsb.sh 0 false 8 88 95 1 
./test_ycsb.sh 0 true  8 88 95 1 

# mkdir $bench_file_dir/$tdate
# mv result_overall* $bench_file_dir/$tdate
