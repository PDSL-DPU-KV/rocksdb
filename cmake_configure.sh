TYPE="Release"
#FLAGS="-DWITH_GFLAGS=1 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZLIB=1 -DWITH_ZSTD=1 -DUSE_RTTI=0 -DCMAKE_PREFIX_PATH=/home/dc/grpc/build -DFAIL_ON_WARNINGS=0"
FLAGS="-DWITH_GFLAGS=1 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZLIB=1 -DWITH_ZSTD=1 -DUSE_RTTI=1 -DWITH_HDFS=0 -DFAIL_ON_WARNINGS=0"
FLAGS_OPTIONAL="--no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE"
COMPILER_C="/usr/bin/gcc-9"
COMPILER_CPP="/usr/bin/g++-9"


MSG_SUCCESS="[SUCCESS]"
MSG_FAILED="[FAILED]"

{
    # cmake configure
    echo -n -e 'Step [1/1]: cmake configure\t...\t'
    {   # try
        /usr/bin/cmake -DCMAKE_BUILD_TYPE=$TYPE \
        -G "Unix Makefiles" \
        -S "./" \
        -B "./build" \
        $FLAGS \
        $FLAGS_OPTIONAL \
        -DCMAKE_C_COMPILER:FILEPATH=$COMPILER_C \
        -DCMAKE_CXX_COMPILER:FILEPATH=$COMPILER_CPP \
        > /dev/null
    } || {
        echo -e $MSG_FAILED
        exit -1
    }
    echo -e $MSG_SUCCESS
}
