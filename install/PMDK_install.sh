#!/bin/bash
set -e

echo "This script configures ubuntu16.04 with everything needed to install PMDK. "
echo "It requires that you run it as root. Sudo works great for that."

apt update

apt install --yes \
    autoconf \
    automake \
    libglib2.0-dev \
    librdmacm1 \
    libpsm-infinipath1 \
    libncurses5-dev \
    pkg-config \
    pandoc \
    wget \
    zip \
    gcc \
    g++ \
    git \
    make \
    cmake \
    man-db

cd ~

wget -O ~/libfabric1_1.4.0-1_amd64.deb http://mirrors.ustc.edu.cn/debian/pool/main/libf/libfabric/libfabric1_1.4.0-1_amd64.deb
sudo dpkg -i libfabric1_1.4.0-1_amd64.deb

wget -O ~/libfabric-dev_1.4.0-1_amd64.deb http://mirrors.ustc.edu.cn/debian/pool/main/libf/libfabric/libfabric-dev_1.4.0-1_amd64.deb
sudo dpkg -i libfabric-dev_1.4.0-1_amd64.deb

wget -O ~/libdaxctl1_67-1_amd64.deb http://mirrors.aliyun.com/ubuntu/pool/main/n/ndctl/libdaxctl1_67-1_amd64.deb?spm=a2c6h.25603864.0.0.79ac34c31RNXhq
sudo dpkg -i libdaxctl1_67-1_amd64.deb

wget -O ~/libdaxctl-dev_67-1_amd64.deb http://mirrors.aliyun.com/ubuntu/pool/main/n/ndctl/libdaxctl-dev_67-1_amd64.deb?spm=a2c6h.25603864.0.0.79ac34c31RNXhq
sudo dpkg -i libdaxctl-dev_67-1_amd64.deb

wget -O ~/libndctl6_67-1_amd64.deb http://mirrors.aliyun.com/ubuntu/pool/main/n/ndctl/libndctl6_67-1_amd64.deb?spm=a2c6h.25603864.0.0.79ac34c31RNXhq
sudo dpkg -i libndctl6_67-1_amd64.deb

wget -O ~/libndctl-dev_67-1_amd64.deb http://mirrors.aliyun.com/ubuntu/pool/main/n/ndctl/libndctl-dev_67-1_amd64.deb?spm=a2c6h.25603864.0.0.79ac34c31RNXhq
sudo dpkg -i libndctl-dev_67-1_amd64.deb

git clone https://github.com/pmem/pmdk.git
cd pmdk
cp src/test/testconfig.sh.example src/test/testconfig.sh
sudo make
sudo make install
sudo make check
echo  /usr/local/lib/  >> /etc/ld.so.conf
ldconfig
