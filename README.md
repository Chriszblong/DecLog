# DecLog
This is the source code of DecLog.

Growing demands for the efficient processing of extreme-scale time series workloads calls for more capable time series database management systems (TSDBMS). Specifically, to maintain consistency and durability of transaction processing, systems employ write-ahead logging (WAL) that ensures that transactions are committed only after log entries are flushed to disk. However, when faced with massive I/O, this becomes a throughput bottleneck. Recent advances in byte-addressable Non-Volatile Memory (NVM) provide opportunities to improve logging performance by persisting logs to NVM instead. Existing studies typically track complex transaction dependencies and use barrier instructions of NVM to ensure log ordering, and few studies consider the heavy-tailed characteristics of time series workloads, where most transactions are independent of each other. 

We propose DecLog, a decentralized NVM-based logging system to enable concurrent logging of TSDBMS transactions. Specifically, we propose data-driven log sequence numbering and relaxed ordering strategies to track transaction dependencies and resolve serialization issues. We also propose a parallel logging method to persist logs to NVM after being compressed and aligned. An experimental study on the YCSB-TS benchmark offers insight into the performance properties of DecLog, showing that it improves throughput by 4.6x at most with less recovery time when compared with the open source TSDBMS Beringei.


