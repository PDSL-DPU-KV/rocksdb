export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/dc/grpc/build/lib

if [ -n "$1" ]
then
/usr/bin/cmake --build ./build/ --target $1 -- -j 32
else
/usr/bin/cmake --build ./build/ --target rocksdb rocksdb-shared db_bench -- -j 32
fi
