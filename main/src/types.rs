use std::net::{IpAddr, SocketAddr};

use clickhouse::Row;
use crusty_core::types as ct;
use serde::{Deserialize, Serialize};

use crate::config::config;
#[allow(unused_imports)]
use crate::prelude::*;

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug, Clone)]
pub struct Domain {
	pub addrs:    Vec<SocketAddr>,
	pub url:      Option<Url>,
	pub domain:   String,
	pub addr_key: String,
}

impl From<interop::Domain> for Domain {
	fn from(s: interop::Domain) -> Self {
		Self { domain: s.name, addr_key: s.addr_key, addrs: s.addrs, url: None }
	}
}

impl Domain {
	pub fn new(domain: String, addrs: Vec<SocketAddr>, addr_key_mask: u8, url: Option<Url>) -> Domain {
		let mut addrs_sorted = addrs.clone();
		addrs_sorted.sort_unstable();
		let mut addr = addrs_sorted
			.into_iter()
			.next()
			.filter(|a| a.ip().is_ipv4())
			.map(|ip| if let IpAddr::V4(ip) = ip.ip() { ip.octets() } else { panic!("not supposed to happen") })
			.unwrap_or([255, 255, 255, 255]);

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
				left = 0;
			}
		}

		Domain { addr_key: base64::encode(addr), addrs, url, domain }
	}

	pub fn to_interop(&self) -> interop::Domain {
		interop::Domain::new(&self.domain, &self.addr_key, &self.addrs)
	}

	pub fn to_interop_descriptor(&self) -> interop::DomainDescriptor {
		interop::DomainDescriptor::new(&self.domain, &self.addr_key)
	}

	pub fn _calc_shard(&mut self, shards_total: usize) -> u32 {
		let mut hasher = crc32fast::Hasher::new();
		hasher.update(self.addr_key.as_bytes());
		hasher.finalize() % shards_total as u32 + 1
	}
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct DomainDBEntry {
	pub domain: String,
	pub addrs:  Vec<SocketAddr>,
}

impl From<Domain> for DomainDBEntry {
	fn from(s: Domain) -> Self {
		Self { domain: s.domain, addrs: s.addrs }
	}
}

#[derive(Debug, Clone)]
pub struct DBNotification<A: Clone + Send> {
	pub table_name: String,
	pub label:      String,
	pub since_last: Duration,
	pub duration:   Duration,
	pub items:      Vec<A>,
}

#[derive(Debug, Clone)]
pub struct DBGenericNotification {
	pub table_name: String,
	pub label:      String,
	pub since_last: Duration,
	pub duration:   Duration,
	pub items:      usize,
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

impl From<DBGenericNotification> for DBRWNotificationDBEntry {
	fn from(s: DBGenericNotification) -> Self {
		DBRWNotificationDBEntry {
			host:          config().host.clone(),
			app_id:        config().app_id.clone(),
			created_at:    now().as_secs() as u32,
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
		if s.md.is_none() {
			return Self {
				host:           config().host.clone(),
				app_id:         config().app_id.clone(),
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
			host:           config().host.clone(),
			app_id:         config().app_id.clone(),
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

	Parse,
	DomainResolveAggregator,
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
		Self {
			host:       config().host.clone(),
			app_id:     config().app_id.clone(),
			name:       format!("{:?}", s.kind),
			updated_at: s.stats.time,
			len:        s.stats.len as u32,
		}
	}
}
impl<JS: ct::JobStateValues, TS: ct::TaskStateValues> From<ct::JobUpdate<JS, TS>> for TaskMeasurement {
	fn from(r: ct::JobUpdate<JS, TS>) -> Self {
		if let ct::JobStatus::Processing(Ok(ref job_processing)) = r.status {
			let parse_time_ms =
				job_processing.follow.as_ref().map(|d| d.metrics.duration.as_millis() as u32).unwrap_or(0);

			let (load_time_ms, write_size_b, read_size_b) = job_processing
				.load
				.as_ref()
				.map(|load| {
					(
						load.metrics.duration.as_millis() as u32,
						load.metrics.write_size as u32,
						load.metrics.read_size as u32,
					)
				})
				.unwrap_or((0, 0, 0));

			if let Ok(status) = job_processing.status.as_ref() {
				return TaskMeasurement {
					time: now().as_secs() as u32,
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

		TaskMeasurement { time: now().as_secs() as u32, url: r.task.link.url.to_string(), md: None }
	}
}
