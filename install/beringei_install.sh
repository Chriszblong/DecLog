#!/bin/bash

WORK_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")"  && pwd)"
VERSION=$(lsb_release -d)
set -e

echo "This script configures ubuntu18.04 or newer versions with everything needed to install Beringei dependencies. "
echo "It requires that you run it as root. Sudo works great for that."

sudo cp /etc/apt/sources.list /etc/apt/sources.list.bk
if [[ ${VERSION:20:2} == "22" ]]; then
	sudo cp $WORK_PATH/sources_22.04.list /etc/apt/sources.list
fi
if [[ ${VERSION:20:2} == "20" ]]; then
	sudo cp $WORK_PATH/sources_20.04.list /etc/apt/sources.list
fi

if [[ ${VERSION:20:2} != "16" ]]; then
gpg --keyserver keyserver.ubuntu.com --recv-keys 437D05B5
gpg --export --armor 437D05B5 | sudo apt-key add -

apt update

# The libboost version is 1.65 on ubuntu 18.04.

sudo apt-get autoremove \
    libboost*-dev \
    openssl \
    libicu-dev

apt install --yes \
    gcc \
    g++ \
    gcc-5 \
    g++-5 \
    zip \
    unzip \
    libpython-stdlib \
    python-minimal \
    python-dev \
    python2.7 \
    zlib1g-dev \
    libssl1.0.0 \
    autoconf \
    autoconf-archive \
    automake \
    make \
    cmake

gcc_version=`gcc -dumpversion`
echo "Gcc Version $gcc_version"
gpp_version=`g++ -dumpversion`
echo "Gcc Version $gpp_version"

if [[ ${gcc_version:0:1} == "1" ]]; then
	sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${gcc_version:0:2} 50
	sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-${gpp_version:0:2} 50
else
	sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${gcc_version:0:1} 50
	sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-${gpp_version:0:1} 50
fi
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-5 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-5 100

    if [ ! -d "$WORK_PATH/packages_beringei" ];then
        sudo unzip ./packages_beringei.zip -d $WORK_PATH
    fi

    cd $WORK_PATH/packages_beringei

    sudo dpkg -i ./libicu55_55.1-7_amd64.deb
    sudo dpkg -i ./icu-devtools_55.1-7_amd64.deb
    sudo dpkg -i ./libicu-dev_55.1-7_amd64.deb
    sudo dpkg -i ./libboost1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-atomic1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-atomic1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-system1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-system1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-chrono1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-chrono1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-serialization1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-serialization1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-date-time1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-date-time1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-thread1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-thread1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-context1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-context1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-filesystem1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-filesystem1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-program-options1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-program-options1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-python1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-python1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-regex1.58.0_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libboost-regex1.58-dev_1.58.0+dfsg-5ubuntu3_amd64.deb
    sudo dpkg -i ./libmpfr4_3.1.4-1_amd64.deb
    sudo dpkg -i ./openssl_1.0.2g-1ubuntu4_amd64.deb
    sudo dpkg -i ./libssl1.0.0_1.0.2g-1ubuntu4_amd64.deb
    sudo dpkg -i ./libssl-dev_1.0.2g-1ubuntu4_amd64.deb

    if [[ ${VERSION:20:2} == "20" ]] || [[ ${VERSION:20:2} == "22" ]]; then
        tar -zxvf jemalloc-4.4.0.tar.gz
        cd jemalloc-4.4.0/
        ./autogen.sh
        sudo make install
        sudo ldconfig
    fi

    if [[ ${VERSION:20:2} == "22" ]]; then
        tar -zxvf $WORK_PATH/packages_beringei/libdwarf-20201201.tar.gz -C $WORK_PATH/
        cd $WORK_PATH/libdwarf-20201201/
        ./configure --enable-shared --disable-static --disable-libelf --enable-wall
        sudo make install
        sudo ldconfig
    fi

fi

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
    binutils-dev \
    bison \
    clang-format-3.9 \
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
    pkg-config \
    scons \
    wget \
    zip \
    zlib1g-dev

if [[ ${VERSION:20:2} == "16" ]]; then
    apt install --yes \
    autoconf \
    autoconf-archive \
    automake \
    binutils-dev \
    bison \
    clang-format-3.9 \
    clang-format \
    cmake \
    flex \
    g++ \
    git \
    gperf \
    libboost-all-dev \
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
    libssl-dev \
    libtool \
    make \
    pkg-config \
    scons \
    wget \
    zip \
    zlib1g-dev
fi

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

if [[ ${VERSION:20:2} == "20" ]] || [[ ${VERSION:20:2} == "22" ]]; then
    export LD_PRELOAD=/lib/x86_64-linux-gnu/libjemalloc.so.2
fi

tar xzvf $WORK_PATH/folly-${FB_VERSION}.tar.gz -C /tmp
tar xzvf $WORK_PATH/wangle-${FB_VERSION}.tar.gz -C /tmp
tar xzvf $WORK_PATH/fbthrift-${FB_VERSION}.tar.gz -C /tmp
tar xzvf $WORK_PATH/proxygen-${FB_VERSION}.tar.gz -C /tmp
tar xzvf $WORK_PATH/mstch-master.tar.gz -C /tmp
tar xzvf $WORK_PATH/zstd-${ZSTD_VERSION}.tar.gz -C /tmp

cd /tmp

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


echo $(dirname $0)

cd $WORK_PATH/beringei

if [ ! -d "./build" ];then
    mkdir build
fi

cd build
cmake ..
sudo make

sudo ln -s $WORK_PATH/beringei/build/beringei/tools/beringei_get /usr/bin/beringei_get
sudo ln -s $WORK_PATH/beringei/build/beringei/tools/beringei_put /usr/bin/beringei_put
sudo ln -s $WORK_PATH/beringei/build/beringei/tools/beringei_configuration_generator /usr/bin/beringei_configuration_generator
sudo ln -s $WORK_PATH/beringei/build/beringei/service/beringei_main /usr/bin/beringei_main
