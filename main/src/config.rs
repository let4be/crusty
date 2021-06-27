use std::{env, fs};

use crusty_core::config as rc;
use once_cell::sync::OnceCell;
use serde::Deserialize;

use crate::{_prelude::*, types::*};

pub static CONFIG: OnceCell<CrustyConfig> = OnceCell::new();
static DEFAULT_CONFIG: OnceCell<CrustyConfig> = OnceCell::new();
static DEFAULT_CONFIG_STR: &str = include_str!("../config.yaml");

pub fn config<'a>() -> &'a CrustyConfig {
	CONFIG.get().unwrap()
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RulesConfig {
	pub skip_no_follow_links:  bool,
	pub total_link_budget:     usize,
	pub links_per_task_budget: usize,
	pub max_level:             usize,
	pub robots_txt:            bool,
	pub max_redirect:          usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownConfig {
	pub graceful_timeout: rc::CDuration,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConfig {
	pub redis: RedisConfig,
	pub jobs:  JobsConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopKOptions {
	pub name:             String,
	pub k:                usize,
	pub width:            usize,
	pub depth:            usize,
	pub decay:            f64,
	pub consume_interval: rc::CDuration,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopKCollectConfig {
	pub second_level_only: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopKConfig {
	pub redis:   RedisConfig,
	pub collect: TopKCollectConfig,
	pub options: TopKOptions,
	pub driver:  RedisDriverConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
	pub hosts: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobReaderConfig {
	pub shard_min_last_read: rc::CDuration,
	pub seeds:               Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisDriverConfig {
	pub soft_cap:      usize,
	pub hard_cap:      usize,
	pub release_after: rc::CDuration,
}

impl RedisDriverConfig {
	pub fn to_thresholds(&self) -> relabuf::RelaBufConfig {
		relabuf::RelaBufConfig {
			release_after: *self.release_after,
			soft_cap:      self.soft_cap,
			hard_cap:      self.hard_cap,
			backoff:       Some(relabuf::ExponentialBackoff::default()),
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsEnqueueConfig {
	pub options: JobsEnqueueOptions,
	pub driver:  RedisDriverConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsEnqueueOptions {
	pub ttl: rc::CDuration,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsFinishConfig {
	pub options: JobsFinishOptions,
	pub driver:  RedisDriverConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsFinishOptions {
	pub ttl:                 rc::CDuration,
	pub bf_initial_capacity: usize,
	pub bf_error_rate:       f64,
	pub bf_expansion_factor: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsDequeueConfig {
	pub options: JobsDequeueOptions,
	pub driver:  RedisDriverConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsDequeueOptions {
	pub limit:             usize,
	pub ttl:               rc::CDuration,
	pub emit_permit_delay: rc::CDuration,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsConfig {
	pub shard_min:     usize,
	pub shard_max:     usize,
	pub shard_total:   usize,
	pub addr_key_mask: u8,
	pub enqueue:       JobsEnqueueConfig,
	pub finish:        JobsFinishConfig,
	pub dequeue:       JobsDequeueConfig,
	pub reader:        JobReaderConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseWriterConfig {
	pub table_name: String,
	pub label: String,
	pub buffer_capacity: usize,
	pub check_for_force_write_duration: rc::CDuration,
	pub force_write_duration: rc::CDuration,
	pub concurrency: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
	pub url:      String,
	pub username: String,
	pub password: String,
	pub database: String,

	pub metrics_queue: ClickhouseWriterConfig,
	pub metrics_db:    ClickhouseWriterConfig,
	pub metrics_task:  ClickhouseWriterConfig,
	pub metrics_job:   ClickhouseWriterConfig,
	pub topk:          ClickhouseWriterConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
	pub level:  rc::CLevel,
	pub ansi:   bool,
	pub filter: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrustyConfig {
	pub host: String,

	pub rules: RulesConfig,
	pub shutdown: ShutdownConfig,
	pub log: LogConfig,
	pub clickhouse: ClickhouseConfig,
	pub queue: QueueConfig,
	pub topk: TopKConfig,
	pub resolver: ResolverConfig,
	pub networking_profile: rc::NetworkingProfile,
	pub concurrency_profile: rc::ConcurrencyProfile,
	pub default_crawling_settings: rc::CrawlingSettings,

	pub ddc_cap:                     usize,
	pub ddc_lifetime:                rc::CDuration,
	pub queue_monitor_interval:      rc::CDuration,
	pub parser_processor_stack_size: rc::CBytes,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolverConfig {
	pub concurrency: usize,
}

pub fn load() -> Result<()> {
	let default_config: CrustyConfig =
		serde_yaml::from_str(DEFAULT_CONFIG_STR).context("uh-oh, cannot parse default config...")?;
	DEFAULT_CONFIG.set(default_config).unwrap();

	let mut read_err = None;
	let cfg_str = fs::read_to_string("./config.yaml")?;
	let mut config: CrustyConfig = serde_yaml::from_str(&cfg_str).unwrap_or_else(|err| {
		read_err = Some(err);
		DEFAULT_CONFIG.get().unwrap().clone()
	});

	if let Ok(seeds) = env::var("CRUSTY_SEEDS") {
		config
			.queue
			.jobs
			.reader
			.seeds
			.extend(seeds.split(',').filter(|v| !v.is_empty()).map(String::from).collect::<Vec<_>>());
	}

	CONFIG.set(config).unwrap();

	if let Some(err) = read_err {
		return Err(err.into())
	}
	Ok(())
}
