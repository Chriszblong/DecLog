# DecLog
This is the source code of DecLog.

Our paper,"DecLog: Decentralized Logging in Non-Volatile Memory for Time Series Database Systems" is submitted to VLDB2024.


## Build DecLog
```
$ unzip ./install/ALL.zip
$ sudo ./install/setup_ubuntu.sh
```
## Build YCSB-TS
YCSB-TS is a maven project and is compiled with JDK1.7.
To build it, run maven command `mvn clean package`.
Then uncompress the release package 
```
$ mv ./YCSB-TS/distribution/target/ycsb-0.4.0.tar.gz ./ycsb-0.4.0.tar.gz
$ tar -zxvf ./ycsb-0.4.0.tar.gz
```
## Before Run
[You should mount your PMEM* to a NVM-aware filesystem with DAX.](https://docs.pmem.io/persistent-memory/getting-started-guide/creating-development-environments/linux-environments/linux-memmap)
```
$ sudo mkfs.xfs /dev/pmem0
$ sudo mkdir /mnt/pmem0
$ sudo mount -o dax /dev/pmem0 /mnt/pmem0
$ sudo modprobe msr
```
Make directorys for key list file and log file in SSD
```
$ mkdir /tmp/key
$ mkdir /tmp/log
```


## Run DecLog

```
$ beringei/beringei/build/beringei/service/beringei_main \
        -beringei_configuration_path ./beringei/beringei.json \
        -create_directories \
        -bucket_size 600 \
        -buckets 120 \
        -logtostderr \
        -v=2 \
        -port 9997 \
        -shards 4 \
        -data_directory /tmp/gorilla_data \
        -key_directory /tmp/key/ \
        -log_directory /mnt/pmem \
        -log_writer_queue_size 300 \
        -log_writer_threads 4
```
## Run DecLog-SSD
```
$ beringei/beringei/build/beringei/service/beringei_main \
        -beringei_configuration_path ./beringei/beringei.json \
        -create_directories \
        -bucket_size 600 \
        -buckets 120 \
        -logtostderr \
        -v=2 \
        -port 9997 \
        -shards 4 \
        -data_directory /tmp/gorilla_data \
        -key_directory /tmp/key/ \
        -log_directory /tmp/log/ \
        -log_writer_queue_size 300 \
        -log_writer_threads 4
        -NVM_address_aligned 0 \
        -number_of_logging_queues 1 \
        -data_log_buffer_size 65536
```
## Run Beringei-NVM
```
$ beringei/beringei/build/beringei/service/beringei_main \
        -beringei_configuration_path ./beringei/beringei.json \
        -create_directories \
        -bucket_size 600 \
        -buckets 120 \
        -logtostderr \
        -v=2 \
        -port 9997 \
        -shards 4 \
        -data_directory /tmp/gorilla_data \
        -key_directory /tmp/key/ \
        -log_directory /mnt/pmem \
        -log_writer_queue_size 300 \
        -log_writer_threads 1
        -Beringei_NVM 1 \
        -NVM_address_aligned 0 \
        -number_of_logging_queues 1
```
## Run Beringei-SSD
```
$ beringei/beringei/build/beringei/service/beringei_main \
        -beringei_configuration_path ./beringei/beringei.json \
        -create_directories \
        -bucket_size 600 \
        -buckets 120 \
        -logtostderr \
        -v=2 \
        -port 9997 \
        -shards 4 \
        -data_directory /tmp/gorilla_data \
        -key_directory /tmp/key/ \
        -log_directory /tmp/log/ \
        -log_writer_queue_size 300 \
        -log_writer_threads 1
        -Beringei_NVM 1 \
        -NVM_address_aligned 0 \
        -number_of_logging_queues 1 \
        -data_log_buffer_size 65536
```


## Run the benchmark, YCSB-TS


```
$ ${jdk_dir}/bin/java \
        -classpath ./ycsb-0.4.0/lib/core-0.4.0.jar:./ycsb-0.4.0/beringei/lib/beringei-binding-0.4.0.jar:./ycsb-0.4.0/beringei/lib/slf4j-api-2.0.5.jar:./ycsb-0.4.0/beringei/lib/slf4j-simple-2.0.5.jar:./ycsb-0.4.0/beringei/lib/thriftjava-1.0-SNAPSHOT.jar com.yahoo.ycsb.Client \
        -db com.yahoo.ycsb.db.BeringeiDBClient -load -P ./ycsb-0.4.0/workloads/workloada -p host=127.0.0.1 -p port=9997 -p shardCount=4 -s -threads 64 &> /dev/null
$ ${jdk_dir}/bin/java \
        -classpath ./ycsb-0.4.0/lib/core-0.4.0.jar:./ycsb-0.4.0/beringei/lib/beringei-binding-0.4.0.jar:./ycsb-0.4.0/beringei/lib/slf4j-api-2.0.5.jar:./ycsb-0.4.0/beringei/lib/slf4j-simple-2.0.5.jar:./ycsb-0.4.0/beringei/lib/thriftjava-1.0-SNAPSHOT.jar com.yahoo.ycsb.Client \
        -db com.yahoo.ycsb.db.BeringeiDBClient -P ./ycsb-0.4.0/workloads/workloada -p host=127.0.0.1 -p port=9997 -p shardCount=4 -s -threads 64 > workloada_output.txt
```

