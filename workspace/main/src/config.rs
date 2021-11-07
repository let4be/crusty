use std::{env, fs};

use config::{Config, ConfigError, Environment, File, FileFormat};
use crusty_core::config as rc;
use once_cell::sync::OnceCell;
use serde::{de, Deserialize, Deserializer};

use crate::{_prelude::*, types::*};

pub static CONFIG: OnceCell<CrustyConfig> = OnceCell::new();

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

impl CrustyConfig {
	fn expand_vars<S: ToString>(s: S) -> String {
		let replacements =
			HashMap::from([("GIT_SHA", env!("VERGEN_GIT_SHA")), ("BUILD_TIMESTAMP", env!("VERGEN_BUILD_TIMESTAMP"))]);

		let mut r = s.to_string();
		for (var, val) in replacements {
			r = r.replace(format!("{{{}}}", var).as_str(), val);
		}

		r
	}

	pub fn new_from_content(content: &str) -> std::result::Result<Self, ConfigError> {
		let mut s = Config::default();

		s.merge(File::from_str(&Self::expand_vars(content), FileFormat::Yaml))?;

		if let Some(env_profile) = env::var("CRUSTY_PROFILE").ok().filter(|p| !p.is_empty()) {
			s.merge(File::with_name(&format!("./conf/profile-{}.yaml", env_profile)).required(true))?;
		}

		s.merge(File::with_name("./conf/local.yaml").required(false))?;

		s.merge(Environment::with_prefix("APP").separator("_"))?;

		s.try_into()
	}

	pub fn new() -> std::result::Result<Self, ConfigError> {
		let default_config_str =
			fs::read_to_string("./conf/default.yaml").map_err(|err| ConfigError::Message(err.to_string()))?;
		Self::new_from_content(&default_config_str)
	}
}

pub fn load() -> Result<()> {
	let mut config = CrustyConfig::new()?;

	if let Ok(seeds) = env::var("CRUSTY_SEEDS") {
		config.queue.jobs.reader.seeds.extend(seeds.split(',').filter(|v| !v.is_empty()).map(String::from));
	}

	CONFIG.set(config).unwrap();
	Ok(())
}
