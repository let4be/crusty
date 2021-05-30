use clickhouse::Reflection;
use crusty_core::{types as ct, types::StatusResult};
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use crate::prelude::*;
use crate::{clickhouse_utils as chu, config::CONFIG};

pub type Result<T> = anyhow::Result<T>;
pub type Job = ct::Job<JobState, TaskState>;

#[derive(Debug, Clone)]
pub struct Domain {
	is_discovery:     bool,
	pub shard:        u16,
	pub shards_total: usize,

	pub url:    Option<Url>,
	pub domain: String,
	pub head:   String,
	pub tail:   String,
}

impl Domain {
	fn _new(domain: String, shards_total: usize, url: Option<Url>, is_discovery: bool) -> Domain {
		let split: Vec<String> = domain.split('.').map(String::from).collect();

		if split.len() < 2 {
			return Domain { is_discovery, shard: 0, shards_total, url, domain: domain.clone(), head: domain, tail: "".into() }
		}
		if split.len() == 2 {
			return Domain { is_discovery, shard: 0, shards_total, url, domain, head: split.join("."), tail: "".into() }
		}

		Domain { is_discovery, shard: 0, shards_total, url, domain, head: split[split.len() - 2..].join("."), tail: split[..split.len() - 2].join(".") }
	}

	pub fn new(domain: String, shards_total: usize, url: Option<Url>, is_discovery: bool) -> Domain {
		let mut domain = Domain::_new(domain, shards_total, url, is_discovery);
		domain.calc_shard();
		domain
	}

	fn calc_shard(&mut self) {
		let mut hasher = crc32fast::Hasher::new();
		hasher.update(self.head.as_bytes());
		self.shard = (hasher.finalize() % self.shards_total as u32 + 1) as u16;
	}
}

#[derive(Clone, Debug, Reflection, Serialize, Deserialize)]
pub struct DomainDBEntry {
	pub shard:       u16,
	pub domain:      String,
	pub domain_tail: String,
	pub created_at:  u32,
	pub updated_at:  u32,
}

impl From<Domain> for DomainDBEntry {
	fn from(s: Domain) -> Self {
		let now = now();

		Self { shard: s.shard, domain: s.head, domain_tail: s.tail, created_at: now, updated_at: if s.is_discovery { 644616000 } else { now } }
	}
}

#[derive(Debug, Clone)]
pub struct JobState {
	pub selected_domain: Domain,
}

#[derive(Debug, Default, Clone)]
pub struct TaskState {}

pub struct CrawlingRules {}

impl ct::JobRules<JobState, TaskState> for CrawlingRules {
	fn task_filters(&self) -> ct::TaskFilters<JobState, TaskState> {
		vec![
			Box::new(crusty_core::task_filters::MaxRedirect::new(5)),
			Box::new(crusty_core::task_filters::SameDomain::new(true)),
			Box::new(crusty_core::task_filters::TotalPageBudget::new(50)),
			Box::new(crusty_core::task_filters::LinkPerPageBudget::new(10)),
			Box::new(crusty_core::task_filters::PageLevel::new(10)),
			Box::new(crusty_core::task_filters::HashSetDedup::new()),
		]
	}

	fn status_filters(&self) -> ct::StatusFilters<JobState, TaskState> {
		vec![Box::new(crusty_core::status_filters::ContentType::new(vec![String::from("text/html")])), Box::new(crusty_core::status_filters::Redirect::new())]
	}

	fn load_filters(&self) -> ct::LoadFilters<JobState, TaskState> {
		vec![]
	}

	fn task_expanders(&self) -> ct::TaskExpanders<JobState, TaskState> {
		vec![Box::new(crusty_core::task_expanders::FollowLinks::new(ct::LinkTarget::Follow))]
	}
}

#[derive(Clone, Debug, Reflection, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Reflection, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize, Reflection)]
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
		Self { host: c.host.clone(), app_id: c.app_id.clone(), name: format!("{:?}", s.kind), updated_at: s.stats.time, len: s.stats.len as u32 }
	}
}
impl<JS: ct::JobStateValues, TS: ct::TaskStateValues> From<ct::JobUpdate<JS, TS>> for TaskMeasurement {
	fn from(r: ct::JobUpdate<JS, TS>) -> Self {
		if let ct::JobStatus::Processing(Ok(ref load_data)) = r.status {
			let parse_time_ms = if let ct::FollowResult::Ok(ref follow_data) = load_data.follow_data { follow_data.metrics.parse_dur.as_millis() as u32 } else { 0 };

			let (load_time_ms, write_size_b, read_size_b) = if let ct::LoadResult::Ok(ref load_data) = load_data.load_data {
				(load_data.metrics.load_dur.as_millis() as u32, load_data.metrics.write_size as u32, load_data.metrics.read_size as u32)
			} else {
				(0, 0, 0)
			};

			if let StatusResult::Ok(status) = &load_data.status {
				return TaskMeasurement {
					time: now(),
					url:  r.task.link.url.to_string(),
					md:   Some(TaskMeasurementData {
						status_code: status.status_code as u16,
						wait_time_ms: status.metrics.wait_dur.as_millis() as u32,
						status_time_ms: status.metrics.status_dur.as_millis() as u32,
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
