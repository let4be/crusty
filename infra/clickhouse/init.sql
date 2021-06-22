DROP DATABASE IF EXISTS crusty;

CREATE DATABASE crusty;

USE crusty;

CREATE TABLE metrics_db (
    created_date Date DEFAULT now(),
    host String,
    app_id String,
    created_at DateTime,
    table_name String,
    label String,
    took_ms UInt32,
    since_last_ms UInt32,
    items UInt32
) ENGINE = Memory;

CREATE TABLE metrics_queue (
    updated_at DateTime,
    host String,
    app_id String,
    name String,
    name_index UInt32,
    len UInt32
) ENGINE = Memory;

CREATE TABLE metrics_task (
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
) ENGINE = Memory;
