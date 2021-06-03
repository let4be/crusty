#!/bin/bash
DB="crusty"
envsubst < init.sql | clickhouse client --host localhost --multiline --multiquery <<-EOSQL
	DROP DATABASE IF EXISTS $DB;
	CREATE DATABASE $DB;
	CREATE TABLE $DB.domain_discovery (
	  shard UInt16,
	  domain String,
	  domain_tail String,
	  updated_at SimpleAggregateFunction(max, DateTime),
	  created_at SimpleAggregateFunction(min, DateTime),
	  INDEX updated_at_index updated_at TYPE
	  set(100) GRANULARITY 1
	) ENGINE = AggregatingMergeTree() PARTITION BY shard PRIMARY KEY (shard, domain, domain_tail)
	ORDER BY
	  (shard, domain, domain_tail) SETTINGS index_granularity = 8192;
	CREATE TABLE $DB.metrics_db (
	  created_date Date DEFAULT now(),
	  host String,
	  app_id String,
	  created_at DateTime,
	  table_name String,
	  label String,
	  took_ms UInt32,
	  since_last_ms UInt32,
	  items UInt32
	) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(created_date)
	ORDER BY
	  (created_at, host, app_id) SETTINGS index_granularity = 8192;
	CREATE TABLE $DB.metrics_queue (
	  updated_at DateTime,
	  host String,
	  app_id String,
	  name String,
	  len UInt32
	) ENGINE = AggregatingMergeTree() PARTITION BY name
	ORDER BY
	  (updated_at, host, app_id) SETTINGS index_granularity = 8192;
	CREATE TABLE $DB.metrics_task (
	  created_date Date DEFAULT now(),
	  created_at DateTime,
	  host String,
	  app_id String,
	  url String,
	  error UInt8,
	  status_code UInt16,
	  wait_time_ms UInt32,
	  status_time_ms UInt32,
	  load_time_ms UInt32,
	  write_size_b UInt32,
	  read_size_b UInt32,
	  parse_time_ms UInt32
	) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(created_date) PRIMARY KEY (created_at, host, app_id)
	ORDER BY
	  (created_at, host, app_id) SETTINGS index_granularity = 8192;
EOSQL
