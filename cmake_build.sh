if [ -n "$1" ]
then
/usr/bin/cmake --build ./build/ --target $1 -- -j 8
else
/usr/bin/cmake --build ./build/ --target rocksdb rocksdb-shared db_bench -- -j 8
fi
