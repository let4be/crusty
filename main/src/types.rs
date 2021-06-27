use std::net::{IpAddr, SocketAddr};

use clickhouse::Row;
use crusty_core::types as ct;
use serde::{Deserialize, Serialize};

use crate::{_prelude::*, config::config};

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

	pub fn calc_shard(&self, shards_total: usize) -> usize {
		let mut hasher = crc32fast::Hasher::new();
		hasher.update(self.addr_key.as_bytes());
		hasher.finalize() as usize % shards_total
	}
}

#[derive(Debug)]
pub struct DomainLinks {
	pub name:           String,
	pub linked_domains: Vec<String>,
}

impl DomainLinks {
	pub fn new(name: &str, links: Vec<String>) -> Self {
		Self { name: String::from(name), linked_domains: links }
	}
}

#[derive(Debug)]
pub struct DBNotification<A: Send> {
	pub table_name: String,
	pub label:      String,
	pub since_last: Duration,
	pub duration:   Duration,
	pub items:      Vec<A>,
}

#[derive(Debug)]
pub struct DBGenericNotification {
	pub table_name: String,
	pub label:      String,
	pub since_last: Duration,
	pub duration:   Duration,
	pub items:      usize,
}

impl<A: Send> From<&DBNotification<A>> for DBGenericNotification {
	fn from(s: &DBNotification<A>) -> Self {
		DBGenericNotification {
			table_name: s.table_name.clone(),
			label:      s.label.clone(),
			since_last: s.since_last,
			duration:   s.duration,
			items:      s.items.len(),
		}
	}
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct DBNotificationDBE {
	pub host:          &'static str,
	pub created_at:    u32,
	pub table_name:    String,
	pub label:         String,
	pub took_ms:       u32,
	pub since_last_ms: u32,
	pub items:         u32,
}

impl<T: Send> From<DBNotification<T>> for DBGenericNotification {
	fn from(s: DBNotification<T>) -> Self {
		DBGenericNotification {
			table_name: s.table_name,
			label:      s.label,
			since_last: s.since_last,
			duration:   s.duration,
			items:      s.items.len(),
		}
	}
}

impl<A: Send> From<DBNotification<A>> for DBNotificationDBE {
	fn from(s: DBNotification<A>) -> Self {
		DBNotificationDBE {
			host:          config().host.as_str(),
			created_at:    now().as_secs() as u32,
			table_name:    s.table_name,
			label:         s.label,
			took_ms:       s.duration.as_millis() as u32,
			since_last_ms: s.since_last.as_millis() as u32,
			items:         s.items.len() as u32,
		}
	}
}

impl From<DBGenericNotification> for DBNotificationDBE {
	fn from(s: DBGenericNotification) -> Self {
		DBNotificationDBE {
			host:          config().host.as_str(),
			created_at:    now().as_secs() as u32,
			table_name:    s.table_name,
			label:         s.label,
			took_ms:       s.duration.as_millis() as u32,
			since_last_ms: s.since_last.as_millis() as u32,
			items:         s.items as u32,
		}
	}
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct TaskMeasurementDBE {
	host:             &'static str,
	url:              String,
	created_at:       u32,
	//
	term:             u8,
	term_by_filter:   &'static str,
	term_by_name:     &'static str,
	term_kind:        &'static str, // Err/Panic/Reason
	term_reason:      &'static str,
	//
	error:            u8,
	error_known:      &'static str,
	//
	status_ok:        u8,
	status_code:      u16,
	wait_time_ms:     u32,
	status_time_ms:   u32,
	//
	load_ok:          u8,
	load_time_ms:     u32,
	write_size_b:     u32,
	read_size_b:      u32,
	//
	follow_ok:        u8,
	parse_time_micro: u32,
}

struct TaskTermBy {
	term_by_filter: &'static str,
	term_by_name:   &'static str,
	term_kind:      &'static str,
	term_reason:    &'static str,
}

impl From<&ct::ExtStatusError> for TaskTermBy {
	fn from(s: &ct::ExtStatusError) -> Self {
		match s {
			ct::ExtStatusError::Err { kind, name, .. } => TaskTermBy {
				term_by_filter: kind.into(),
				term_by_name:   name,
				term_kind:      "Error",
				term_reason:    "",
			},
			ct::ExtStatusError::Panic { kind, name, .. } => TaskTermBy {
				term_by_filter: kind.into(),
				term_by_name:   name,
				term_kind:      "Panic",
				term_reason:    "",
			},
			ct::ExtStatusError::Term { kind, name, reason } => TaskTermBy {
				term_by_filter: kind.into(),
				term_by_name:   name,
				term_kind:      "Reason",
				term_reason:    reason,
			},
		}
	}
}

impl TaskMeasurementDBE {
	fn new(url: &str) -> TaskMeasurementDBE {
		TaskMeasurementDBE {
			host:       config().host.as_str(),
			url:        String::from(url),
			created_at: now().as_secs() as u32,

			term:           0,
			term_by_filter: "",
			term_by_name:   "",
			term_kind:      "", // Err/Panic/Reason:{}
			term_reason:    "",

			error:       0,
			error_known: "",

			status_ok:      0,
			status_code:    0,
			wait_time_ms:   0,
			status_time_ms: 0,

			load_ok:      0,
			load_time_ms: 0,
			write_size_b: 0,
			read_size_b:  0,

			follow_ok:        0,
			parse_time_micro: 0,
		}
	}

	fn set_term_by(&mut self, t: TaskTermBy) {
		self.term = 1;
		self.term_by_filter = t.term_by_filter;
		self.term_by_name = t.term_by_name;
		self.term_kind = t.term_kind;
		self.term_reason = t.term_reason;
	}
}

impl<JS: ct::JobStateValues, TS: ct::TaskStateValues> From<ct::JobUpdate<JS, TS>> for TaskMeasurementDBE {
	fn from(r: ct::JobUpdate<JS, TS>) -> Self {
		let mut def = TaskMeasurementDBE::new(r.task.link.url.as_str());

		if let ct::JobStatus::Processing(Ok(ref job_processing)) = r.status {
			if let ct::StatusResult(Some(Ok(status_data))) = &job_processing.status {
				def.status_ok = 1;
				let m = &status_data.metrics;
				def.status_code = status_data.code;
				def.wait_time_ms = m.wait_duration.as_millis() as u32;
				def.status_time_ms = m.duration.as_millis() as u32;

				if let Some(err) = &status_data.filter_err {
					def.set_term_by(err.into());
				}
			}
			if let ct::LoadResult(Some(Ok(load_data))) = &job_processing.load {
				def.load_ok = 1;
				let m = &load_data.metrics;
				def.load_time_ms = m.duration.as_millis() as u32;
				def.write_size_b = m.write_size as u32;
				def.read_size_b = m.read_size as u32;

				if let Some(err) = &load_data.filter_err {
					def.set_term_by(err.into());
				}
			}
			if let ct::FollowResult(Some(Ok(follow_data))) = &job_processing.follow {
				def.follow_ok = 1;
				let m = &follow_data.metrics;
				def.parse_time_micro = m.duration.as_micros() as u32;

				if let Some(err) = &follow_data.filter_err {
					def.set_term_by(err.into());
				}
			}

			def.error = (job_processing.head_status.as_ref().map(|r| r.is_err()).unwrap_or(false)
				|| job_processing.status.as_ref().map(|r| r.is_err()).unwrap_or(false)
				|| job_processing.load.as_ref().map(|r| r.is_err()).unwrap_or(false)
				|| job_processing.follow.as_ref().map(|r| r.is_err()).unwrap_or(false)) as u8;

			if let Some(Err(ct::Error::StatusTimeout)) = &job_processing.head_status.0 {
				def.error_known = "HeadStatusTimeout"
			}

			if let Some(Err(ct::Error::StatusTimeout)) = &job_processing.status.0 {
				def.error_known = "StatusTimeout"
			}

			if let Some(Err(ct::Error::LoadTimeout)) = &job_processing.load.0 {
				def.error_known = "LoadTimeout"
			}
		}

		def
	}
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct JobMeasurementDBE {
	host:         &'static str,
	url:          String,
	created_at:   u32,
	//
	duration_sec: u32,
	term_by:      &'static str,
}

impl<JS: ct::JobStateValues, TS: ct::TaskStateValues> From<ct::JobUpdate<JS, TS>> for JobMeasurementDBE {
	fn from(r: ct::JobUpdate<JS, TS>) -> Self {
		let mut def = JobMeasurementDBE {
			host:       config().host.as_str(),
			url:        r.task.link.url.to_string(),
			created_at: now().as_secs() as u32,

			duration_sec: r.task.queued_at.elapsed().as_secs() as u32,
			term_by:      "Ok",
		};

		if let ct::JobStatus::Finished(Err(ref err)) = r.status {
			def.term_by = match err {
				ct::JobError::JobFinishedBySoftTimeout => "SoftTimeout",
				ct::JobError::JobFinishedByHardTimeout => "HardTimeout",
			};
		}

		def
	}
}

#[derive(Debug)]
pub struct QueueMeasurement {
	pub time:  Duration,
	pub name:  String,
	pub index: usize,
	pub len:   usize,
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct QueueMeasurementDBE {
	host:       &'static str,
	name:       String,
	name_index: u32,
	updated_at: u32,
	len:        u32,
}

impl From<QueueMeasurement> for QueueMeasurementDBE {
	fn from(s: QueueMeasurement) -> Self {
		Self {
			host:       config().host.as_str(),
			updated_at: s.time.as_secs() as u32,
			name:       s.name,
			name_index: s.index as u32,
			len:        s.len as u32,
		}
	}
}

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct TopHitsDBE {
	pub created_at: u32,
	pub tld:        String,
	pub domain:     String,
	pub hits:       u64,
}

impl From<&interop::TopHit> for TopHitsDBE {
	fn from(s: &interop::TopHit) -> Self {
		Self {
			created_at: now().as_secs() as u32,
			domain:     s.domain.clone(),
			hits:       s.hits as u64,
			tld:        s.tld.clone(),
		}
	}
}
