# run ycsb_ts on windows
java -classpath lib\core-0.4.0.jar;lib\beringei-binding-0.4.0.jar;lib\slf4j-api-2.0.5.jar;lib\slf4j-simple-2.0.5.jar;lib\thriftjava-1.0-SNAPSHOT.jar com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.BeringeiDBClient -P workloads\myworkload -p host=192.168.164.43 -p port=9997 -p shardCount=1 -s -threads 64 > load_nolog_64.dat

# run ycsb_ts on linux
sudo /usr/local/bin/jdk1.8.0_361/bin/java -classpath /usr/local/ycsb/lib/core-0.4.0.jar:/usr/local/ycsb/lib/beringei-binding-0.4.0.jar:/usr/local/ycsb/lib/slf4j-api-2.0.5.jar:/usr/local/ycsb/lib/slf4j-simple-2.0.5.jar:/usr/local/ycsb/lib/thriftjava-1.0-SNAPSHOT.jar com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.BeringeiDBClient -P /usr/local/ycsb/.github/timeSeries/beringei/workloads/myworkload -p host=127.0.0.1 -p port=9997 -p shardCount=4 -s -threads 64 > load_nolog_64.dat

# run ycsb_ts on linux for update worklods
# YCSB-TS should run with '-load' at first, then without '-load'.
sudo /usr/local/jdk1.8.0_361/bin/java -classpath /usr/local/ycsb/lib/core-0.4.0.jar:/usr/local/ycsb/lib/beringei-binding-0.4.0.jar:/usr/local/ycsb/lib/slf4j-api-2.0.5.jar:/usr/local/ycsb/lib/slf4j-simple-2.0.5.jar:/usr/local/ycsb/lib/thriftjava-1.0-SNAPSHOT.jar com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.BeringeiDBClient -P /mnt/data1/.github/timeSeries/beringei/workloads/myworkload -p host=127.0.0.1 -p port=9997 -p shardCount=4 -s -threads 64 > load_log_64.dat

# update
sudo /usr/local/jdk1.8.0_361/bin/java -classpath /usr/local/ycsb/lib/core-0.4.0.jar:/usr/local/ycsb/lib/beringei-binding-0.4.0.jar:/usr/local/ycsb/lib/slf4j-api-2.0.5.jar:/usr/local/ycsb/lib/slf4j-simple-2.0.5.jar:/usr/local/ycsb/lib/thriftjava-1.0-SNAPSHOT.jar com.yahoo.ycsb.Client -db com.yahoo.ycsb.db.BeringeiDBClient -P /mnt/data1/.github/timeSeries/beringei/workloads/myworkload -p host=127.0.0.1 -p port=9997 -p shardCount=4 -s -threads 64 > update_log_64.dat

# perf monitor
sudo perf top -g


#perf record

sudo perf record -p 48214 -a -g -F 100 -- sleep 20


#perf report
perf report


# rm tmp files
sudo rm -rf /mnt/pmem0/*
sudo rm -rf /mnt/pmem1/*
sudo rm -rf /mnt/pmem2/*
sudo rm -rf /mnt/pmem3/*
sudo rm -rf /mnt/pmem4/*
sudo rm -rf /mnt/pmem5/*
sudo rm -rf /mnt/pmem6/*
sudo rm -rf /mnt/pmem7/*

sudo rm -rf /mnt/data/tmp/tmp0/*
sudo rm -rf /mnt/data/tmp/tmp1/*
sudo rm -rf /mnt/data/tmp/tmp2/*
sudo rm -rf /mnt/data/tmp/tmp3/*
sudo rm -rf /mnt/data/tmp/tmp4/*
sudo rm -rf /mnt/data/tmp/tmp5/*
sudo rm -rf /mnt/data/tmp/tmp6/*
sudo rm -rf /mnt/data/tmp/tmp7/*


sudo rm -rf /home/gyy/tmp/tmp0/*
sudo rm -rf /home/gyy/tmp/tmp1/*
sudo rm -rf /home/gyy/tmp/tmp2/*
sudo rm -rf /home/gyy/tmp/tmp3/*
sudo rm -rf /home/gyy/tmp/tmp4/*
sudo rm -rf /home/gyy/tmp/tmp5/*
sudo rm -rf /home/gyy/tmp/tmp6/*
sudo rm -rf /home/gyy/tmp/tmp7/*

sudo rm -rf /tmp/0/logData/*
sudo rm -rf /tmp/1/logData/*
sudo rm -rf /tmp/2/logData/*
sudo rm -rf /tmp/3/logData/*
sudo rm -rf /tmp/4/logData/*
sudo rm -rf /tmp/5/logData/*
sudo rm -rf /tmp/6/logData/*
sudo rm -rf /tmp/7/logData/*

sudo rm -rf /tmp/0/shard-0-key_list*
sudo rm -rf /tmp/1/shard-0-key_list*
sudo rm -rf /tmp/2/shard-0-key_list*
sudo rm -rf /tmp/3/shard-0-key_list*
sudo rm -rf /tmp/4/shard-0-key_list*
sudo rm -rf /tmp/5/shard-0-key_list*
sudo rm -rf /tmp/6/shard-0-key_list*
sudo rm -rf /tmp/7/shard-0-key_list*