## v1.3 [unreleased]

#### Features

- Compatible with InfluxDB Version up to 0.8
- API: add a InfluxDB#createDatabase(DatabaseConfiguration) to be able to create a new Database with ShardSpaces defined.
- API: introduction of InfluxDB#createShardSpare, InfluxDB#getShardSpace and InfluxDB#dropShardSpace
- API: deprecated InfluxDB#createShard, InfluxDB#getShards and InfluxDB#dropShard, this is replaced with shardSpaces in InfluxDB >= 0.8.0
- API: renamed InfluxDB#deletePoints to InfluxDB#deleteSeries because this is what it actually does.
- [Issue #14] update docker-java for tests to 0.10.0 
- Update retrofit from 1.6.0 -> 1.6.1
- Use ms instead of m for millisecond timeprecision.

## v1.2 [2014-06-28]

#### Features

- [Issue #2] (https://github.com/influxdb/influxdb-java/issues/2) Implement the last missing api calls ( interfaces, sync, forceCompaction, servers, shards)
- use (http://square.github.io/okhttp/, okhttp) instead of java builtin httpconnection to get failover for the http endpoint.

#### Tasks

- [Issue #8] (https://github.com/influxdb/influxdb-java/issues/8) Use com.github.docker-java which replaces com.kpelykh for Integration tests.
- [Issue #6] (https://github.com/influxdb/influxdb-java/issues/6) Update Retrofit to 1.6.0 
- [Issue #7] (https://github.com/influxdb/influxdb-java/issues/7) Update Guava to 17.0 
- fix dependency to guava.

## v1.1 [2014-05-31]

#### Features

- Add InfluxDB#version() to get the InfluxDB Server version information.
- Changed InfluxDB#createDatabase() to match (https://github.com/influxdb/influxdb/issues/489) without replicationFactor.
- Updated Retrofit from 1.5.0 -> 1.5.1

## v1.0 [2014-05-6]

  * Initial Release
