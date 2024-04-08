if [ -n "$1" ]
then
/snap/bin/cmake --build ./build/ --target $1 -- -j 32
else
/snap/bin/cmake --build ./build/ --target rocksdb rocksdb-shared db_bench -- -j 32
fi
