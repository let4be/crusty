use std::{env, fs};

use crusty_core::config as rc;
use once_cell::sync::OnceCell;
use serde::Deserialize;

#[allow(unused_imports)]
use crate::{_prelude::*, types::*};

pub static CONFIG: OnceCell<CrustyConfig> = OnceCell::new();
static DEFAULT_CONFIG: OnceCell<CrustyConfig> = OnceCell::new();
static DEFAULT_CONFIG_STR: &str = include_str!("../config.yaml");

pub fn config<'a>() -> &'a CrustyConfig {
	CONFIG.get().unwrap()
}

#[derive(Clone, Debug, Deserialize)]
pub struct RulesConfig {
	pub skip_no_follow_links:  bool,
	pub total_link_budget:     usize,
	pub links_per_task_budget: usize,
	pub max_level:             usize,
	pub robots_txt:            bool,
	pub max_redirect:          usize,
}

impl Default for RulesConfig {
	fn default() -> Self {
		Self {
			skip_no_follow_links:  true,
			total_link_budget:     1000,
			links_per_task_budget: 100,
			max_level:             25,
			robots_txt:            true,
			max_redirect:          5,
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct ShutdownConfig {
	pub graceful_timeout: rc::CDuration,
}

impl Default for ShutdownConfig {
	fn default() -> Self {
		Self { graceful_timeout: rc::CDuration::from_secs(0) }
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct RedisConfig {
	pub hosts: Vec<String>,
}

impl Default for RedisConfig {
	fn default() -> Self {
		Self { hosts: vec![String::from("redis://localhost:6379/")] }
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobReaderConfig {
	pub shard_min_last_read: rc::CDuration,
	pub seeds:               Vec<String>,
}

impl Default for JobReaderConfig {
	fn default() -> Self {
		Self { shard_min_last_read: rc::CDuration::from_secs(1), seeds: vec![] }
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct RedisDriverConfig {
	pub soft_cap:      usize,
	pub hard_cap:      usize,
	pub release_after: rc::CDuration,
}

impl Default for RedisDriverConfig {
	fn default() -> Self {
		Self { soft_cap: 500, hard_cap: 1000, release_after: rc::CDuration::from_secs(1) }
	}
}

#[allow(clippy::from_over_into)]
impl Into<relabuf::RelaBufConfig> for RedisDriverConfig {
	fn into(self) -> relabuf::RelaBufConfig {
		relabuf::RelaBufConfig {
			release_after: *self.release_after,
			soft_cap:      self.soft_cap,
			hard_cap:      self.hard_cap,
			backoff:       Some(relabuf::ExponentialBackoff::default()),
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobsEnqueueConfig {
	pub options: JobsEnqueueOptions,
	pub driver:  RedisDriverConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobsEnqueueOptions {
	pub ttl: rc::CDuration,
}

impl Default for JobsEnqueueOptions {
	fn default() -> Self {
		Self { ttl: rc::CDuration::from_secs(60 * 10) }
	}
}

impl Default for JobsEnqueueConfig {
	fn default() -> Self {
		Self { options: JobsEnqueueOptions::default(), driver: RedisDriverConfig::default() }
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobsFinishConfig {
	pub options: JobsFinishOptions,
	pub driver:  RedisDriverConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobsFinishOptions {
	pub ttl:                 rc::CDuration,
	pub bf_initial_capacity: usize,
	pub bf_error_rate:       f64,
	pub bf_expansion_factor: usize,
}

impl Default for JobsFinishOptions {
	fn default() -> Self {
		Self {
			ttl:                 rc::CDuration::from_secs(60 * 10),
			bf_initial_capacity: 10000000,
			bf_error_rate:       0.001,
			bf_expansion_factor: 2,
		}
	}
}

impl Default for JobsFinishConfig {
	fn default() -> Self {
		Self { options: JobsFinishOptions::default(), driver: RedisDriverConfig::default() }
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobsDequeueConfig {
	pub options: JobsDequeueOptions,
	pub driver:  RedisDriverConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobsDequeueOptions {
	pub limit:             usize,
	pub ttl:               rc::CDuration,
	pub emit_permit_delay: rc::CDuration,
}

impl Default for JobsDequeueOptions {
	fn default() -> Self {
		Self {
			limit:             10000,
			ttl:               rc::CDuration::from_secs(60 * 10),
			emit_permit_delay: rc::CDuration::from_millis(1000),
		}
	}
}

impl Default for JobsDequeueConfig {
	fn default() -> Self {
		Self { options: JobsDequeueOptions::default(), driver: RedisDriverConfig::default() }
	}
}

#[derive(Clone, Debug, Deserialize)]
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

impl Default for JobsConfig {
	fn default() -> Self {
		let shard_min = 1;
		let shard_max = 25;
		Self {
			shard_min,
			shard_max,
			shard_total: shard_max - shard_min + 1,
			addr_key_mask: 24,
			enqueue: JobsEnqueueConfig::default(),
			finish: JobsFinishConfig::default(),
			dequeue: JobsDequeueConfig::default(),
			reader: JobReaderConfig::default(),
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClickhouseWriterConfig {
	pub table_name: String,
	pub label: String,
	pub buffer_capacity: usize,
	pub check_for_force_write_duration: rc::CDuration,
	pub force_write_duration: rc::CDuration,
	pub concurrency: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClickhouseConfig {
	pub url:      String,
	pub username: String,
	pub password: String,
	pub database: String,

	pub metrics_queue: ClickhouseWriterConfig,
	pub metrics_db:    ClickhouseWriterConfig,
	pub metrics_task:  ClickhouseWriterConfig,
	pub metrics_job:   ClickhouseWriterConfig,
}

impl Default for ClickhouseConfig {
	fn default() -> Self {
		Self {
			url:      String::from("http://localhost:8123"),
			username: String::from("default"),
			password: String::from(""),
			database: String::from("default"),

			metrics_queue: ClickhouseWriterConfig {
				table_name: String::from("metrics_queue"),
				label: String::from(""),
				buffer_capacity: 1000,
				check_for_force_write_duration: rc::CDuration::from_millis(100),
				force_write_duration: rc::CDuration::from_millis(500),
				concurrency: 3,
			},
			metrics_db:    ClickhouseWriterConfig {
				table_name: String::from("metrics_db"),
				label: String::from(""),
				buffer_capacity: 1000,
				check_for_force_write_duration: rc::CDuration::from_millis(100),
				force_write_duration: rc::CDuration::from_millis(500),
				concurrency: 3,
			},
			metrics_task:  ClickhouseWriterConfig {
				table_name: String::from("metrics_task"),
				label: String::from(""),
				buffer_capacity: 10000,
				check_for_force_write_duration: rc::CDuration::from_millis(100),
				force_write_duration: rc::CDuration::from_millis(500),
				concurrency: 3,
			},
			metrics_job:   ClickhouseWriterConfig {
				table_name: String::from("metrics_job"),
				label: String::from(""),
				buffer_capacity: 100,
				check_for_force_write_duration: rc::CDuration::from_millis(100),
				force_write_duration: rc::CDuration::from_millis(500),
				concurrency: 3,
			},
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct LogConfig {
	pub level:  rc::CLevel,
	pub ansi:   bool,
	pub filter: Option<Vec<String>>,
}

impl Default for LogConfig {
	fn default() -> Self {
		Self { level: rc::CLevel(Level::INFO), ansi: true, filter: None }
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct CrustyConfig {
	pub host: String,

	#[serde(default)]
	pub rules: RulesConfig,
	#[serde(default)]
	pub shutdown: ShutdownConfig,
	#[serde(default)]
	pub log: LogConfig,
	#[serde(default)]
	pub clickhouse: ClickhouseConfig,
	#[serde(default)]
	pub redis: RedisConfig,
	#[serde(default)]
	pub jobs: JobsConfig,
	#[serde(default)]
	pub resolver: ResolverConfig,
	#[serde(default)]
	pub networking_profile: rc::NetworkingProfile,
	#[serde(default)]
	pub concurrency_profile: rc::ConcurrencyProfile,
	#[serde(default)]
	pub default_crawling_settings: rc::CrawlingSettings,

	pub ddc_cap:                     usize,
	pub ddc_lifetime:                rc::CDuration,
	pub queue_monitor_interval:      rc::CDuration,
	pub parser_processor_stack_size: rc::CBytes,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ResolverConfig {
	pub concurrency: usize,
}

impl Default for ResolverConfig {
	fn default() -> Self {
		let physical_cores = num_cpus::get_physical();
		Self { concurrency: physical_cores * 6 }
	}
}

impl Default for CrustyConfig {
	fn default() -> Self {
		DEFAULT_CONFIG.get().unwrap().clone()
	}
}

pub fn load() -> Result<()> {
	let default_config: CrustyConfig =
		serde_yaml::from_str(DEFAULT_CONFIG_STR).context("uh-oh, cannot parse default config...")?;
	DEFAULT_CONFIG.set(default_config).unwrap();

	let mut read_err = None;
	let cfg_str = fs::read_to_string("./config.yaml")?;
	let mut config: CrustyConfig = serde_yaml::from_str(&cfg_str).unwrap_or_else(|err| {
		read_err = Some(err);
		CrustyConfig::default()
	});

	if let Ok(seeds) = env::var("CRUSTY_SEEDS") {
		config
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
