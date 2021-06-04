use clickhouse::Row;
use crusty_core::{types as ct, types::StatusResult};
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use crate::prelude::*;
use crate::{clickhouse_utils as chu, config::CONFIG};

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug, Clone)]
pub struct Domain {
	is_discovery:     bool,
	pub shard:        u16,
	pub shards_total: usize,

	pub url:    Option<Url>,
	pub domain: String,
	addr_key:   [u8; 4],
}

impl Domain {
	pub fn new(
		domain: String,
		addrs: Vec<[u8; 4]>,
		shards_total: usize,
		addr_key_mask: u8,
		url: Option<Url>,
		is_discovery: bool,
	) -> Domain {
		let mut addrs = addrs;
		addrs.sort_unstable();
		let mut addr = addrs.into_iter().next().unwrap_or([255, 255, 255, 255]);

		let mut left = addr_key_mask;
		for a in &mut addr {
			if left >= 8 {
				left -= 8;
			} else {
				let mut mask = 0;
				for k in 0..left {
					mask |= 1 << k;
				}
				*a &= mask;
			}
		}

		let mut domain = Domain { addr_key: addr, is_discovery, shard: 0, shards_total, url, domain };
		domain.calc_shard();
		domain
	}

	fn calc_shard(&mut self) {
		let mut hasher = crc32fast::Hasher::new();
		hasher.update(&self.addr_key);
		self.shard = (hasher.finalize() % self.shards_total as u32 + 1) as u16;
	}
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct DomainDBEntry {
	pub shard:      u16,
	pub addr_key:   [u8; 4],
	pub domain:     String,
	pub created_at: u32,
	pub updated_at: u32,
}

impl From<Domain> for DomainDBEntry {
	fn from(s: Domain) -> Self {
		let now = now();

		Self {
			shard:      s.shard,
			addr_key:   s.addr_key,
			domain:     s.domain,
			created_at: now,
			updated_at: if s.is_discovery { 644616000 } else { now },
		}
	}
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct DBRWNotificationDBEntry {
	host:          String,
	app_id:        String,
	created_at:    u32,
	table_name:    String,
	label:         String,
	took_ms:       u32,
	since_last_ms: u32,
	items:         u32,
}

impl From<chu::GenericNotification> for DBRWNotificationDBEntry {
	fn from(s: chu::GenericNotification) -> Self {
		let c = CONFIG.lock().unwrap();
		DBRWNotificationDBEntry {
			host:          c.host.clone(),
			app_id:        c.app_id.clone(),
			created_at:    now(),
			table_name:    s.table_name,
			label:         s.label,
			took_ms:       s.duration.as_millis() as u32,
			since_last_ms: s.since_last.as_millis() as u32,
			items:         s.items as u32,
		}
	}
}

#[derive(Debug, Clone)]
pub struct TaskMeasurementData {
	pub status_code:    u16,
	pub wait_time_ms:   u32,
	pub status_time_ms: u32,
	pub load_time_ms:   u32,
	pub parse_time_ms:  u32,
	pub write_size_b:   u32,
	pub read_size_b:    u32,
}

#[derive(Clone, Debug)]
pub struct TaskMeasurement {
	pub time: u32,
	pub url:  String,
	pub md:   Option<TaskMeasurementData>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct TaskMeasurementDBEntry {
	host:           String,
	app_id:         String,
	created_at:     u32,
	url:            String,
	error:          u8,
	status_code:    u16,
	wait_time_ms:   u32,
	status_time_ms: u32,
	load_time_ms:   u32,
	parse_time_ms:  u32,
	write_size_b:   u32,
	read_size_b:    u32,
}

impl From<TaskMeasurement> for TaskMeasurementDBEntry {
	fn from(s: TaskMeasurement) -> Self {
		let c = CONFIG.lock().unwrap();
		if s.md.is_none() {
			return Self {
				host:           c.host.clone(),
				app_id:         c.app_id.clone(),
				created_at:     s.time,
				url:            s.url,
				error:          1,
				status_code:    0,
				wait_time_ms:   0,
				status_time_ms: 0,
				load_time_ms:   0,
				parse_time_ms:  0,
				write_size_b:   0,
				read_size_b:    0,
			}
		}
		let md = s.md.unwrap();
		Self {
			host:           c.host.clone(),
			app_id:         c.app_id.clone(),
			created_at:     s.time,
			url:            s.url,
			error:          0,
			status_code:    md.status_code,
			wait_time_ms:   md.wait_time_ms,
			status_time_ms: md.status_time_ms,
			load_time_ms:   md.load_time_ms,
			parse_time_ms:  md.parse_time_ms,
			write_size_b:   md.write_size_b,
			read_size_b:    md.read_size_b,
		}
	}
}

#[derive(Debug, Clone)]
pub struct QueueStats {
	pub len:  usize,
	pub time: u32,
}

#[derive(Debug, Clone)]
pub enum QueueKind {
	Job,
	JobUpdate,
	MetricsTask,
	MetricsQueue,
	MetricsDB,
	DomainUpdate,
	DomainInsert,

	DomainUpdateNotifyP,
	DomainUpdateNotify,
	DomainInsertNotify,
}

#[derive(Debug, Clone)]
pub struct QueueMeasurement {
	pub kind:  QueueKind,
	pub stats: QueueStats,
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct QueueMeasurementDBEntry {
	host:           String,
	app_id:         String,
	pub name:       String,
	pub updated_at: u32,
	pub len:        u32,
}

impl From<QueueMeasurement> for QueueMeasurementDBEntry {
	fn from(s: QueueMeasurement) -> Self {
		let c = CONFIG.lock().unwrap();
		Self {
			host:       c.host.clone(),
			app_id:     c.app_id.clone(),
			name:       format!("{:?}", s.kind),
			updated_at: s.stats.time,
			len:        s.stats.len as u32,
		}
	}
}
impl<JS: ct::JobStateValues, TS: ct::TaskStateValues> From<ct::JobUpdate<JS, TS>> for TaskMeasurement {
	fn from(r: ct::JobUpdate<JS, TS>) -> Self {
		if let ct::JobStatus::Processing(Ok(ref load_data)) = r.status {
			let parse_time_ms = if let ct::FollowResult::Ok(ref follow) = load_data.follow {
				follow.metrics.duration.as_millis() as u32
			} else {
				0
			};

			let (load_time_ms, write_size_b, read_size_b) = if let ct::LoadResult::Ok(ref load) = load_data.load {
				(
					load.metrics.duration.as_millis() as u32,
					load.metrics.write_size as u32,
					load.metrics.read_size as u32,
				)
			} else {
				(0, 0, 0)
			};

			if let StatusResult::Ok(status) = &load_data.status {
				return TaskMeasurement {
					time: now(),
					url:  r.task.link.url.to_string(),
					md:   Some(TaskMeasurementData {
						status_code: status.code as u16,
						wait_time_ms: status.metrics.wait_duration.as_millis() as u32,
						status_time_ms: status.metrics.duration.as_millis() as u32,
						load_time_ms,
						write_size_b,
						read_size_b,
						parse_time_ms,
					}),
				}
			}
		}

		TaskMeasurement { time: now(), url: r.task.link.url.to_string(), md: None }
	}
}
