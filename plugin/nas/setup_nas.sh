#!/bin/bash

if [ ! -d "../../third-party" ]; then
    mkdir ../../third-party
fi

cd ../../third-party

printf "\ndownload and build libfabric v1.20.0 ...\n\n"
# libfabric-v1.20.0
if [ ! -d "./libfabric-1.20.0" ]; then
    wget https://github.com/ofiwg/libfabric/releases/download/v1.20.0/libfabric-1.20.0.tar.bz2 && tar xf libfabric-1.20.0.tar.bz2 || exit
    install_prefix="$(pwd)/deps"
    echo $install_prefix
    cd ./libfabric-1.20.0 || exit
    ./configure --prefix=$install_prefix --enable-verbs=yes --enable-tcp=yes --enable-udp=yes &&
        make -j8 && make install || exit
    cd .. || exit
    rm libfabric-1.20.0.tar.bz2 || exit
fi

printf "\ndownload and build mercury v2.3.1 ...\n\n"
# mercury-v2.3.1
if [ ! -d "./mercury-2.3.1" ]; then
    wget https://github.com/mercury-hpc/mercury/releases/download/v2.3.1/mercury-2.3.1.tar.bz2 && tar xf mercury-2.3.1.tar.bz2 || exit
    install_prefix="$(pwd)/deps"
    cd ./mercury-2.3.1 || exit
    mkdir build && cd build &&
        PKG_CONFIG_PATH=$install_prefix/lib/pkgconfig CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=debug -DCMAKE_INSTALL_PREFIX="$install_prefix" -DBUILD_SHARED_LIBS=on -DNA_USE_OFI=on .. &&
        make -j8 && make install || exit
    cd ../.. || exit
    rm mercury-2.3.1.tar.bz2 || exit
fi

printf "\ndownload and build cereal v1.3.2 ...\n\n"
# cereal-v1.3.2
if [ ! -d "./cereal-1.3.2" ]; then
    wget -O cereal-v1.3.2.tar.gz https://github.com/USCiLab/cereal/archive/refs/tags/v1.3.2.tar.gz && tar xf cereal-v1.3.2.tar.gz || exit
    install_prefix="$(pwd)/deps"
    mv ./cereal-1.3.2/include/cereal "$install_prefix/include" || exit
    rm cereal-v1.3.2.tar.gz || exit
fi

cd ..

deps_path=$(pwd)/third-party/deps

printf "all dependencies are installed in %s\n\n" "$deps_path"

