#[allow(unused_imports)]
use crate::prelude::*;
use crate::{
    job_reader,
    types::*
};

use crusty_core::config as rc;

use std::{fs, env};

use serde::{Deserialize};
use once_cell::sync::Lazy;

pub static CONFIG: Lazy<Mutex<CrustyConfig>> = Lazy::new(|| Mutex::new(CrustyConfig::default() ));

#[derive(Clone, Debug, Deserialize)]
pub struct ClickhouseWriterConfig {
    pub table_name: String,
    pub label: String,
    pub buffer_capacity: usize,
    pub check_for_force_write_duration: rc::CDuration,
    pub force_write_duration: rc::CDuration,
}


#[derive(Clone, Debug, Deserialize)]
pub struct ClickhouseConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub database: String,

    pub metrics_queue: ClickhouseWriterConfig,
    pub metrics_db: ClickhouseWriterConfig,
    pub metrics_task: ClickhouseWriterConfig,
    pub domain_discovery_insert: ClickhouseWriterConfig,
    pub domain_discovery_update: ClickhouseWriterConfig,
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            url: String::from("http://localhost:8123"),
            username: String::from("default"),
            password: String::from(""),
            database: String::from("default"),

            metrics_queue: ClickhouseWriterConfig{
                table_name: String::from("metrics_queue"),
                label: String::from(""),
                buffer_capacity: 1000,
                check_for_force_write_duration: rc::CDuration::from_millis(100),
                force_write_duration: rc::CDuration::from_millis(500),
            },
            metrics_db: ClickhouseWriterConfig{
                table_name: String::from("metrics_db"),
                label: String::from(""),
                buffer_capacity: 1000,
                check_for_force_write_duration: rc::CDuration::from_millis(100),
                force_write_duration: rc::CDuration::from_millis(500),
            },
            metrics_task: ClickhouseWriterConfig{
                table_name: String::from("metrics_task"),
                label: String::from(""),
                buffer_capacity: 10000,
                check_for_force_write_duration: rc::CDuration::from_millis(100),
                force_write_duration: rc::CDuration::from_millis(500),
            },
            domain_discovery_insert: ClickhouseWriterConfig{
                table_name: String::from("domain_discovery"),
                label: String::from("insert"),
                buffer_capacity: 10000,
                check_for_force_write_duration: rc::CDuration::from_millis(500),
                force_write_duration: rc::CDuration::from_millis(2500),
            },
            domain_discovery_update: ClickhouseWriterConfig{
                table_name: String::from("domain_discovery"),
                label: String::from("update"),
                buffer_capacity: 10000,
                check_for_force_write_duration: rc::CDuration::from_millis(500),
                force_write_duration: rc::CDuration::from_millis(2500),
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct LogConfig {
    pub level: rc::CLevel,
    pub ansi: bool,
    pub filter: Option<Vec<String>>
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: rc::CLevel(Level::INFO),
            ansi: true,
            filter: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct CrustyConfig {
    pub host: String,
    pub app_id: String,
    pub log: LogConfig,
    pub clickhouse: ClickhouseConfig,

    pub ddc_cap: usize,
    pub ddc_lifetime: rc::CDuration,
    pub queue_monitor_interval: rc::CDuration,
    pub parser_processor_stack_size: rc::CBytes,

    pub networking_profile: rc::NetworkingProfile,
    pub concurrency_profile: rc::ConcurrencyProfile,
    pub job_reader: job_reader::JobReaderConfig,
}

impl Default for CrustyConfig {
    fn default() -> Self {
        Self {
            host: String::from("crawler-1"),
            app_id: String::from("rusty-spider"),
            log: LogConfig::default(),
            clickhouse: ClickhouseConfig::default(),

            ddc_cap: 25_000_000,
            ddc_lifetime: rc::CDuration::from_secs(60 * 60),
            queue_monitor_interval: rc::CDuration::from_secs(1),
            parser_processor_stack_size: rc::CBytes(1024 * 1024 * 32),

            networking_profile: rc::NetworkingProfile::default(),
            concurrency_profile: rc::ConcurrencyProfile::default(),
            job_reader: job_reader::JobReaderConfig::default()
        }
    }
}

pub fn load() -> Result<()> {
    let cfg_str = fs::read_to_string("./config.yaml")?;
    let mut config = CONFIG.lock().unwrap();

    let mut e = None;
    match serde_yaml::from_str(&cfg_str) {
        Ok(cfg) => {*config = cfg}
        Err(err) => {e = Some(err)}
    }

    if let Ok(seeds) = env::var("CRUSTY_SEEDS") {
        config.job_reader.seeds.extend(
            seeds.split(',').map(String::from).collect::<Vec<_>>()
        );
    }

    if let Some(err) = e {
        return Err(err.into())
    }
    Ok(())
}
