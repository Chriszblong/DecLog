#!/bin/bash
set -e

FB_VERSION="2016.11.07.00"
ZSTD_VERSION="1.1.1"

# proxygen does not tag their build
PROXYGEN_COMMIT_HASH="8e76dac9c30ca82aebd56b8d7c61b6dbdd7e1316"

echo "This script configures ubuntu with everything needed to run beringei."
echo "It requires that you run it as root. sudo works great for that."

apt update

apt install --yes \
    autoconf \
    autoconf-archive \
    automake \
    binutils-dev \
    bison \
    clang-format-3.9 \
    cmake \
    flex \
    git \
    gperf \
    libcap-dev \
    libdouble-conversion-dev \
    libevent-dev \
    libgflags-dev \
    libgoogle-glog-dev \
    libjemalloc-dev \
    libkrb5-dev \
    liblz4-dev \
    liblzma-dev \
    libnuma-dev \
    libsasl2-dev \
    libsnappy-dev \
    libtool \
    make \
    pkg-config \
    scons \
    wget \
    zip \
    zlib1g-dev

ready_destdir() {
        if [[ -e ${2} ]]; then
                echo "Moving aside existing $1 directory.."
                mv -v "$2" "$2.bak.$(date +%Y-%m-%d)"
        fi
}

mkdir -pv /usr/local/facebook-${FB_VERSION}
ln -sf /usr/local/facebook-${FB_VERSION} /usr/local/facebook

export LDFLAGS="-L/usr/local/facebook/lib -Wl,-rpath=/usr/local/facebook/lib"
export CPPFLAGS="-I/usr/local/facebook/include"

cd /tmp


tar xzvf folly-${FB_VERSION}.tar.gz
tar xzvf wangle-${FB_VERSION}.tar.gz
tar xzvf fbthrift-${FB_VERSION}.tar.gz
tar xzvf proxygen-${FB_VERSION}.tar.gz
tar xzvf mstch-master.tar.gz
tar xzvf zstd-${ZSTD_VERSION}.tar.gz

sudo chmod 777 /tmp/proxygen-${FB_VERSION}/proxygen/lib/http/gen_HTTPCommonHeaders.cpp.sh
sudo chmod 777 /tmp/proxygen-${FB_VERSION}/proxygen/lib/http/gen_HTTPCommonHeaders.h.sh

pushd mstch-master
cmake -DCMAKE_INSTALL_PREFIX:PATH=/usr/local/facebook-${FB_VERSION} .
make install
popd

pushd zstd-${ZSTD_VERSION}
make install PREFIX=/usr/local/facebook-${FB_VERSION}
popd


pushd folly-${FB_VERSION}/folly
autoreconf -ivf
./configure --prefix=/usr/local/facebook-${FB_VERSION}
make install
popd

pushd wangle-${FB_VERSION}/wangle
cmake -DCMAKE_INSTALL_PREFIX:PATH=/usr/local/facebook-${FB_VERSION} -DBUILD_SHARED_LIBS:BOOL=ON .
make
# Wangle tests are broken. Disabling ctest.
# ctest
make install
popd

pushd fbthrift-${FB_VERSION}/thrift
autoreconf -ivf
./configure --prefix=/usr/local/facebook-${FB_VERSION}
make install
popd

pushd proxygen-${FB_VERSION}/proxygen
autoreconf -ivf
./configure --prefix=/usr/local/facebook-${FB_VERSION}
make install
popd
