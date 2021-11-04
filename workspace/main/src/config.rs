use std::{env, fs};

use crusty_core::config as rc;
use once_cell::sync::OnceCell;
use serde::{de, Deserialize, Deserializer};

use crate::{_prelude::*, types::*};

pub static CONFIG: OnceCell<CrustyConfig> = OnceCell::new();
static DEFAULT_CONFIG: OnceCell<CrustyConfig> = OnceCell::new();
static DEFAULT_CONFIG_STR: &str = include_str!("../config.yaml");

pub fn config() -> &'static CrustyConfig {
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
	pub shard_min:        usize,
	pub shard_max:        usize,
	pub shard_total:      usize,
	pub addr_key_v4_mask: u8,
	pub addr_key_v6_mask: u8,
	pub enqueue:          JobsEnqueueConfig,
	pub finish:           JobsFinishConfig,
	pub dequeue:          JobsDequeueConfig,
	pub reader:           JobReaderConfig,
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
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
	pub clickhouse: ClickhouseConfig,

	pub queue_monitor_interval: rc::CDuration,
	pub metrics_queue:          ClickhouseWriterConfig,
	pub metrics_db:             ClickhouseWriterConfig,
	pub metrics_task:           ClickhouseWriterConfig,
	pub metrics_job:            ClickhouseWriterConfig,
	pub topk:                   ClickhouseWriterConfig,
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
pub struct DomainDiscoveryConfig {
	pub cap: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrustyConfig {
	pub host:     String,
	pub log:      LogConfig,
	pub shutdown: ShutdownConfig,

	pub queue:   QueueConfig,
	pub metrics: MetricsConfig,
	pub topk:    TopKConfig,

	pub rules:                     RulesConfig,
	pub default_crawling_settings: rc::CrawlingSettings,
	pub networking:                rc::NetworkingProfile,
	pub domain_discovery:          DomainDiscoveryConfig,
	pub resolver:                  ResolverConfig,

	#[serde(default)]
	pub parser:      rc::ParserProfile,
	#[serde(default)]
	pub concurrency: rc::ConcurrencyProfile,
}

#[derive(Clone, Debug, EnumString)]
pub enum ResolverAddrIpv6Policy {
	Disabled,
	Preferred,
	Fallback,
}

impl<'de> Deserialize<'de> for ResolverAddrIpv6Policy {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<ResolverAddrIpv6Policy, D::Error> {
		let s: String = Deserialize::deserialize(deserializer)?;
		s.parse::<ResolverAddrIpv6Policy>().map_err(de::Error::custom)
	}
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolverConfig {
	pub concurrency:      usize,
	pub addr_ipv6_policy: ResolverAddrIpv6Policy,
}

fn expand_vars<S: ToString>(s: S) -> String {
	let replacements = HashMap::from([("GIT_SHA", env!("VERGEN_GIT_SHA"))]);

	let mut r = s.to_string();
	for (var, val) in replacements {
		r = r.replace(format!("{{{}}}", var).as_str(), val);
	}

	r
}

pub fn load() -> Result<()> {
	let default_config: CrustyConfig =
		serde_yaml::from_str(&expand_vars(DEFAULT_CONFIG_STR)).context("uh-oh, cannot parse default config...")?;
	DEFAULT_CONFIG.set(default_config).unwrap();

	let mut read_err = None;
	let cfg_str = expand_vars(fs::read_to_string("./config.yaml")?);
	let mut config: CrustyConfig = serde_yaml::from_str(&cfg_str).unwrap_or_else(|err| {
		read_err = Some(err);
		DEFAULT_CONFIG.get().unwrap().clone()
	});

	if let Ok(seeds) = env::var("CRUSTY_SEEDS") {
		config.queue.jobs.reader.seeds.extend(seeds.split(',').filter(|v| !v.is_empty()).map(String::from));
	}

	CONFIG.set(config).unwrap();

	if let Some(err) = read_err {
		return Err(err.into())
	}
	Ok(())
}
