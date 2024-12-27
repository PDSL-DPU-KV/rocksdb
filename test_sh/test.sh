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
./test_single.sh 0 false
./test_single.sh 0 true

cd ..
rm -rf build
./cmake_configure_dflush.sh
./cmake_build.sh
cd test_sh
./test_single.sh 0 false
./test_single.sh 0 true

# mkdir $bench_file_dir/$tdate
# mv result_overall* $bench_file_dir/$tdate
