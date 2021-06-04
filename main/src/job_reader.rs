use std::cell::RefCell;

use clickhouse::Row;
use crusty_core::config as rc;
use futures::{stream::FuturesUnordered, StreamExt};
use serde::Deserialize;

#[allow(unused_imports)]
use crate::prelude::*;
use crate::{clickhouse_utils as chu, rules::*, types::*};

#[derive(Clone, Debug, Deserialize)]
pub struct JobReaderConfig {
	pub re_after_days:             usize,
	pub shard_min_last_read:       rc::CDuration,
	pub addr_key_mask:             u8,
	pub shard_min:                 usize,
	pub shard_max:                 usize,
	pub shard_total:               usize,
	pub shard_select_limit:        usize,
	pub job_buffer:                usize,
	pub domain_top_n:              usize,
	pub domain_table_name:         String,
	pub default_crawling_settings: rc::CrawlingSettings,
	pub seeds:                     Vec<String>,
}

impl Default for JobReaderConfig {
	fn default() -> Self {
		let shard_min = 1;
		let shard_max = 25;

		Self {
			domain_table_name: String::from("domain_discovery"),
			addr_key_mask: 24,
			domain_top_n: 2,
			shard_min_last_read: rc::CDuration::from_secs(1),
			shard_min,
			shard_max,
			shard_total: shard_max - shard_min + 1,
			re_after_days: 3,
			shard_select_limit: 100_000,
			job_buffer: 100_000,
			default_crawling_settings: rc::CrawlingSettings {
				user_agent: Some(String::from("crusty/0.8.0")),
				..rc::CrawlingSettings::default()
			},

			seeds: vec![],
		}
	}
}

pub struct JobReader {
	cfg: JobReaderConfig,
}

struct JobReaderState {
	shard_min_last_read: Duration,
	shard_last_read:     RefCell<HashMap<u16, Instant>>,
	free_shards:         RefCell<LinkedList<u16>>,
	busy_shards:         RefCell<HashMap<u16, HashSet<String>>>,
	jobs:                RefCell<LinkedList<Domain>>,
}

impl JobReaderState {
	fn new(shard_min: usize, shard_max: usize, shard_min_last_read: Duration) -> Self {
		Self {
			shard_min_last_read,
			shard_last_read: RefCell::new(HashMap::new()),
			free_shards: RefCell::new((shard_min as u16..shard_max as u16 + 1).collect()),
			busy_shards: RefCell::new(HashMap::new()),
			jobs: RefCell::new(LinkedList::new()),
		}
	}

	fn next_job_and_shard(&self, task_buffer: usize) -> (Option<u16>, Option<Domain>) {
		let mut jobs = self.jobs.borrow_mut();

		let mut shard: Option<u16> = None;
		if jobs.len() < task_buffer {
			let mut free_shards = self.free_shards.borrow_mut();

			for _ in 0..free_shards.len() {
				shard = free_shards.pop_front();
				if shard.is_none() || self.reserve_busy_shard(shard.unwrap()) {
					break
				}
				free_shards.push_back(shard.unwrap());
				shard = None
			}
		}

		let job = jobs.pop_front();

		(shard, job)
	}

	fn check_shard(&self, shard: u16) {
		let mut busy_shards = self.busy_shards.borrow_mut();
		let sh = busy_shards.get_mut(&shard);
		if let Some(sh) = sh {
			if sh.is_empty() {
				busy_shards.remove(&shard);
				self.free_shards.borrow_mut().push_back(shard);
				info!("Busy shard {} evicted", shard);
			}
		}
	}

	fn reserve_busy_shard(&self, shard: u16) -> bool {
		let mut busy_shards = self.busy_shards.borrow_mut();
		let mut shard_last_read = self.shard_last_read.borrow_mut();

		if let Some(last_read) = shard_last_read.get(&shard) {
			if last_read.elapsed() < self.shard_min_last_read {
				return false
			}
		}
		shard_last_read.insert(shard, Instant::now());

		if let Some(v) = busy_shards.insert(shard, HashSet::new()) {
			panic!("Shard {} is not supposed to be busy: {:?}", shard, v)
		}

		info!("Busy shard {} reserved", shard);
		true
	}

	fn add_job(&self, shard: u16, domain: Domain) {
		let mut busy_shards = self.busy_shards.borrow_mut();
		let busy_shard = busy_shards.get_mut(&shard).unwrap();
		if !busy_shard.insert(domain.domain.clone()) {
			panic!("Shard {} already contains a job for {}!", shard, &domain.domain)
		}

		self.jobs.borrow_mut().push_back(domain.clone());

		info!("Job {}/{} added!", shard, &domain.domain);
	}

	fn finish_job(&self, domain: &Domain) {
		let mut busy_shards = self.busy_shards.borrow_mut();

		if let Some(busy_shard) = busy_shards.get_mut(&domain.shard) {
			if !busy_shard.remove(&domain.domain) {
				panic!(
					"Got notification about finished job '{domain}' but couldn't locate it inside the shard {shard}",
					shard = domain.shard,
					domain = &domain.domain
				);
			}
		} else {
			panic!(
				"Got notification about finished job '{domain}' but couldn't locate shard {shard}",
				shard = domain.shard,
				domain = &domain.domain
			);
		}

		info!("Job {}/{} is finished!", domain.shard, &domain.domain);
	}
}

#[derive(Deserialize, Row)]
struct JobReaderRow {
	addr_key: [u8; 4],
	domains:  Vec<String>,
}

#[derive(Debug)]
enum FutureResult {
	JobsRead(Result<Vec<Domain>>),
	JobsSent,
	MetricsSent,
	Notify(core::result::Result<chu::Notification<Domain>, RecvError>),
	IdleCheckTimeout,
}

impl JobReader {
	pub fn new(cfg: JobReaderConfig) -> Self {
		Self { cfg }
	}

	fn read_jobs<'a>(&'a self, client: &'a clickhouse::Client, shard: u16) -> TracingTask<'a, Vec<Domain>> {
		TracingTask::new_short_lived(span!(), async move {
			let r = client
				.query(
					format!(
						"SELECT addr_key, groupArray(?)(domain) as domains FROM (
							SELECT addr_key, domain FROM {}
							WHERE shard = ?
							GROUP BY shard, addr_key, domain
							HAVING max(updated_at) <= date_sub(DAY, ?, NOW())
						)
						GROUP BY addr_key
						LIMIT ?",
						self.cfg.domain_table_name.as_str()
					)
					.as_str(),
				)
				.bind(self.cfg.domain_top_n as u32)
				.bind(shard)
				.bind(self.cfg.re_after_days as u32)
				.bind(self.cfg.shard_select_limit as u32)
				.fetch::<JobReaderRow>();

			let mut cursor = r.context("cannot get cursor for domain_discovery")?;

			let mut domains = vec![];
			while let Some(row) = cursor.next().await.context("cannot read from domain_discovery")? {
				let addr = row.addr_key;
				row.domains.into_iter().fold(&mut domains, |domains, t| {
					let domain = Domain::new(t, vec![addr], self.cfg.shard_total, self.cfg.addr_key_mask, None, false);
					if domain.shard != shard {
						panic!(
							"domain calced shard {} mismatch to selection shard {} for {:?}",
							domain.shard, shard, &domain.domain
						)
					}

					domains.push(domain);
					domains
				});
			}

			Ok(domains)
		})
	}

	fn handle_read_jobs(&self, state: &JobReaderState, shard: u16, jobs: Vec<Domain>, queried_for: Duration) {
		for domain in jobs {
			state.add_job(shard, domain);
		}

		state.check_shard(shard);
		trace!("->jobs present: {}, query took: {}ms", state.jobs.borrow().len(), queried_for.as_millis());
	}

	fn handle_read_jobs_err(&self, state: &JobReaderState, shard: u16) {
		state.check_shard(shard);
	}

	async fn send_job(&self, tx: &Sender<Job>, job: Domain) {
		let url = job.url.clone().map(|u| u.to_string()).unwrap_or_else(|| format!("http://{}", &job.domain));
		let job_obj = Job::new(
			&url,
			self.cfg.default_crawling_settings.clone(),
			CrawlingRules {},
			JobState { selected_domain: job.clone() },
		);

		if let Ok(job_obj) = job_obj {
			let _ = tx.send_async(job_obj).await;
			trace!("->sending task  for {}", &job.domain);
		} else {
			warn!("->cannot create task for {}: invalid url - {}", &job.domain, &url);
		}
	}

	fn handle_sent_job(&self, state: &JobReaderState, job: Domain) {
		info!(
			"->new task sent for {}, jobs left: {}, free shards: {}, busy shards: {}",
			job.domain,
			state.jobs.borrow().len(),
			state.free_shards.borrow().len(),
			state.busy_shards.borrow().len()
		);
	}

	fn handle_confirmation(&self, state: &JobReaderState, domains: Vec<Domain>) {
		let mut shards = HashSet::new();
		for domain in &domains {
			state.finish_job(domain);
			shards.insert(domain.shard);
		}
		for shard in shards {
			state.check_shard(shard);
		}
		info!(
			"<-{} tasks completed, jobs left: {}, free shards: {}, busy shards: {}",
			domains.len(),
			state.jobs.borrow().len(),
			state.free_shards.borrow().len(),
			state.busy_shards.borrow().len()
		);
	}

	pub async fn go(
		self,
		client: clickhouse::Client,
		tx_jobs: Sender<Job>,
		rx_sig: Receiver<()>,
		tx_metrics_db: Sender<Vec<chu::GenericNotification>>,
		rx_confirmation: Receiver<chu::Notification<Domain>>,
	) -> Result<()> {
		let state = JobReaderState::new(self.cfg.shard_min, self.cfg.shard_max, *self.cfg.shard_min_last_read);

		let mut seed_domains: Vec<_> = self
			.cfg
			.seeds
			.iter()
			.filter_map(|seed| Url::parse(seed).ok())
			.map(|seed| {
				Domain::new(
					seed.domain().unwrap().into(),
					vec![],
					self.cfg.shard_total,
					self.cfg.addr_key_mask,
					Some(seed.clone()),
					false,
				)
			})
			.collect();

		let mut last_read = Instant::now();
		while !rx_sig.is_disconnected() {
			let (shard, job) = state.next_job_and_shard(self.cfg.job_buffer);
			trace!("selected shard {:?} job {:?}", shard, job);

			if let (Some(shard), false) = (shard, seed_domains.is_empty()) {
				for i in 0..seed_domains.len() {
					let d = &seed_domains[i];
					if shard == d.shard {
						state.add_job(d.shard, d.clone());
						seed_domains.remove(i);
						break
					}
				}
			}

			type BoxedFuture<'a> = Pin<Box<dyn Future<Output = FutureResult> + Send + 'a>>;
			let mut futures = FuturesUnordered::<BoxedFuture>::new();

			if shard.is_some() {
				futures.push(Box::pin(async {
					let res = self.read_jobs(&client, shard.unwrap()).instrument().await;
					FutureResult::JobsRead(res)
				}))
			}

			if let Some(job) = job.clone() {
				futures.push(Box::pin(async {
					self.send_job(&tx_jobs, job).await;
					FutureResult::JobsSent
				}))
			}

			let mut awaiting_notification = true;
			let read_notification = Box::pin(rx_confirmation.recv_async());
			futures.push(Box::pin(async move { FutureResult::Notify(read_notification.await) }));

			if futures.len() <= 1 {
				futures.push(Box::pin(async move {
					time::sleep(Duration::from_secs(1)).await;
					FutureResult::IdleCheckTimeout
				}));
			}

			let t = Instant::now();
			while let Some(r) = futures.next().await {
				match r {
					FutureResult::JobsRead(Ok(jobs)) => {
						let queried_for = t.elapsed();
						if !jobs.is_empty() {
							let notification = chu::GenericNotification {
								table_name: self.cfg.domain_table_name.clone(),
								label:      String::from("read"),
								since_last: last_read.elapsed(),
								duration:   queried_for,
								items:      jobs.len(),
							};
							futures.push(Box::pin(async {
								let _ = tx_metrics_db.send_async(vec![notification]).await;
								FutureResult::MetricsSent
							}));
						}

						self.handle_read_jobs(&state, shard.unwrap(), jobs, queried_for);
						last_read = Instant::now();
					}
					FutureResult::JobsRead(Err(_)) => {
						self.handle_read_jobs_err(&state, shard.unwrap());
					}
					FutureResult::JobsSent => {
						self.handle_sent_job(&state, job.as_ref().unwrap().clone());
					}
					FutureResult::Notify(Ok(notification)) => {
						awaiting_notification = false;
						self.handle_confirmation(&state, notification.items);
					}
					FutureResult::Notify(Err(_)) => {
						awaiting_notification = false;
					}
					FutureResult::MetricsSent => {}
					FutureResult::IdleCheckTimeout => {}
				}

				if futures.len() <= 1 && awaiting_notification {
					break
				}
			}
		}

		Ok(())
	}
}
