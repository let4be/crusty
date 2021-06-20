use clickhouse::Client;
use crusty_core::{self, prelude::AsyncHyperResolver, resolver::Resolver, types as rt, MultiCrawler};
use ttl_cache::TtlCache;

#[allow(unused_imports)]
use crate::{
	_prelude::*,
	{clickhouse_utils as chu, config::CrustyConfig, rules::*, types::*},
};
use crate::{
	config,
	redis_utils::{RedisDriver, RedisOperator},
};

#[derive(Clone)]
struct CrustyState {
	client:             Client,
	queue_measurements: Vec<Arc<Box<dyn Fn() -> QueueMeasurement + Send + Sync>>>,
}

pub struct Crusty {
	handles: Vec<tokio::task::JoinHandle<Result<()>>>,
	state:   CrustyState,
	cfg:     config::CrustyConfig,
}

struct EnqueueOperator {
	shard: usize,
	cfg:   config::JobsEnqueueOptions,
}

struct DequeueOperator {
	shard: usize,
	cfg:   config::JobsDequeueOptions,
}

struct FinishOperator {
	shard: usize,
	cfg:   config::JobsFinishOptions,
}

impl RedisOperator<Domain, Domain> for EnqueueOperator {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, domains: &[Domain]) {
		pipeline.cmd("crusty.enqueue").arg("N").arg(self.shard).arg("TTL").arg(self.cfg.ttl.as_secs()).arg("Domains");
		for domain in domains {
			pipeline.arg(serde_json::to_string(&domain.to_interop()).unwrap());
		}
	}

	fn filter(&mut self, domains: Vec<Domain>, _: String) -> Vec<Domain> {
		domains
	}
}

impl RedisOperator<(), Domain> for DequeueOperator {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, _: &[()]) {
		pipeline
			.cmd("crusty.dequeue")
			.arg("N")
			.arg(self.shard)
			.arg("TTL")
			.arg(self.cfg.ttl.as_secs())
			.arg("Limit")
			.arg(self.cfg.limit);
	}

	fn filter(&mut self, _: Vec<()>, r: String) -> Vec<Domain> {
		let domains: Vec<interop::Domain> = serde_json::from_str(&r).unwrap_or_else(|_| Vec::new());
		domains.into_iter().map(Domain::from).collect()
	}
}

impl RedisOperator<Domain, Domain> for FinishOperator {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, domains: &[Domain]) {
		pipeline
			.cmd("crusty.finish")
			.arg("N")
			.arg(self.shard)
			.arg("TTL")
			.arg(self.cfg.ttl.as_secs())
			.arg("BF_Capacity")
			.arg(self.cfg.bf_initial_capacity)
			.arg("BF_Error_Rate")
			.arg(self.cfg.bf_error_rate)
			.arg("BF_EXPANSION")
			.arg(self.cfg.bf_expansion_factor)
			.arg("Domains");
		for domain in domains {
			pipeline.arg(serde_json::to_string(&domain.to_interop_descriptor()).unwrap());
		}
	}

	fn filter(&mut self, domains: Vec<Domain>, _: String) -> Vec<Domain> {
		domains
	}
}

type MyMultiCrawler = MultiCrawler<JobState, TaskState, AsyncHyperResolver, Document>;

impl Crusty {
	async fn try_connect(&mut self) -> Result<()> {
		info!(
			"trying to connect to {} as {}, db: {}",
			&self.cfg.clickhouse.url, &self.cfg.clickhouse.username, &self.cfg.clickhouse.database
		);

		let r = self.state.client.query("SELECT 'ok'").fetch_one::<String>().await?;
		if r == "ok" {
			return Ok(())
		}
		Err(anyhow!("something went wrong"))
	}

	pub fn new(cfg: config::CrustyConfig) -> Self {
		let client = Client::default()
			.with_url(&cfg.clickhouse.url)
			.with_user(&cfg.clickhouse.username)
			.with_password(&cfg.clickhouse.password)
			.with_database(&cfg.clickhouse.database);

		Self { handles: vec![], state: CrustyState { client, queue_measurements: vec![] }, cfg }
	}

	fn metrics_queue_handler(&mut self) -> Sender<QueueMeasurement> {
		let (tx, rx) = bounded_ch::<QueueMeasurement>(self.cfg.concurrency_profile.transit_buffer_size());
		for _ in 0..self.cfg.clickhouse.metrics_queue.concurrency {
			let state = self.state.clone();
			let cfg = self.cfg.clone();
			let rx = rx.clone();
			self.spawn(TracingTask::new(span!(), async move {
				let writer = chu::Writer::new(cfg.clickhouse.metrics_queue);
				writer.go_with_retry(state.client, rx, None, QueueMeasurementDBEntry::from).await
			}));
		}
		tx
	}

	fn metrics_db_handler(&mut self) -> Sender<DBGenericNotification> {
		let (tx, rx) = bounded_ch::<DBGenericNotification>(self.cfg.concurrency_profile.transit_buffer_size());
		for _ in 0..self.cfg.clickhouse.metrics_db.concurrency {
			let state = self.state.clone();
			let cfg = self.cfg.clone();
			let rx = rx.clone();
			self.spawn(TracingTask::new(span!(), async move {
				let writer = chu::Writer::new(cfg.clickhouse.metrics_db);
				writer.go_with_retry(state.client, rx, None, DBRWNotificationDBEntry::from).await
			}));
		}
		tx
	}

	fn metrics_task_handler(&mut self) -> Sender<TaskMeasurement> {
		let (tx, rx) = bounded_ch::<TaskMeasurement>(self.cfg.concurrency_profile.transit_buffer_size());
		for _ in 0..self.cfg.clickhouse.metrics_task.concurrency {
			let state = self.state.clone();
			let cfg = self.cfg.clone();
			let rx = rx.clone();
			self.spawn(TracingTask::new(span!(), async move {
				let writer = chu::Writer::new(cfg.clickhouse.metrics_task);
				writer.go_with_retry(state.client, rx, None, TaskMeasurementDBEntry::from).await
			}));
		}
		tx
	}

	fn domain_enqueue_handler(&mut self, tx_notify: Sender<DBGenericNotification>) -> Sender<Domain> {
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<Domain>(cfg.concurrency_profile.transit_buffer_size());

		self.spawn(TracingTask::new(span!(), async move {
			let mut drv = RedisDriver::new(&cfg.redis.hosts[0], rx, "domains", "insert");
			drv.with_generic_notifier(tx_notify);
			drv.go(
				cfg.jobs.enqueue.driver.into(),
				Box::new(EnqueueOperator { shard: cfg.jobs.shard_min, cfg: cfg.jobs.enqueue.options }),
			)
			.await
		}));

		tx
	}

	fn domain_finish_handler(&mut self, tx_notify: Sender<DBGenericNotification>) -> Sender<Domain> {
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<Domain>(cfg.concurrency_profile.transit_buffer_size());

		self.spawn(TracingTask::new(span!(), async move {
			let mut drv = RedisDriver::new(&cfg.redis.hosts[0], rx, "domains", "update");
			drv.with_generic_notifier(tx_notify);
			drv.go(
				cfg.jobs.finish.driver.into(),
				Box::new(FinishOperator { shard: cfg.jobs.shard_min, cfg: cfg.jobs.finish.options }),
			)
			.await
		}));

		tx
	}

	fn domain_dequeue_handler(&mut self, tx_notify: Sender<DBNotification<Domain>>) {
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<()>(10);

		let emit_permit_delay = *cfg.jobs.dequeue.options.emit_permit_delay;
		self.spawn(TracingTask::new(span!(), async move {
			loop {
				let _ = tx.send_async(()).await;
				time::sleep(emit_permit_delay).await;
			}
		}));

		self.spawn(TracingTask::new(span!(), async move {
			let mut drv = RedisDriver::new(&cfg.redis.hosts[0], rx, "domains", "read");
			drv.with_notifier(tx_notify);
			drv.go(
				cfg.jobs.dequeue.driver.into(),
				Box::new(DequeueOperator { shard: cfg.jobs.shard_min, cfg: cfg.jobs.dequeue.options }),
			)
			.await
		}));
	}

	fn domain_filter_map(
		lnk: &Arc<rt::Link>,
		task_domain: &str,
		ddc: &Arc<Mutex<TtlCache<String, ()>>>,
		cfg: &CrustyConfig,
	) -> Option<String> {
		let domain = lnk.host()?;

		if domain.len() < 3 || !domain.contains('.') || domain == *task_domain {
			return None
		}

		{
			let mut ddc = ddc.lock().unwrap();
			if ddc.contains_key(&domain) {
				return None
			}
			ddc.insert(domain.clone(), (), *cfg.ddc_lifetime);
		}

		info!("new domain discovered: {}", &domain);
		Some(domain)
	}

	fn result_handler(
		ddc: Arc<Mutex<TtlCache<String, ()>>>,
		cfg: config::CrustyConfig,
		tx_metrics: Sender<TaskMeasurement>,
		tx_domain_insert: Sender<String>,
		tx_domain_update: Sender<Domain>,
		rx_job_state_update: Receiver<rt::JobUpdate<JobState, TaskState>>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while let Ok(r) = rx_job_state_update.recv_async().await {
				info!("- {}", r);

				let task_domain = r.task.link.host().unwrap();
				match r.status {
					rt::JobStatus::Processing(Ok(ref jp)) => {
						let discovered_domains =
							jp.links.iter().filter_map(|lnk| Crusty::domain_filter_map(&lnk, &task_domain, &ddc, &cfg));

						for domain in discovered_domains {
							let _ = tx_domain_insert.send_async(domain).await;
						}
						let _ = tx_metrics.send_async(r.into()).await;
					}
					rt::JobStatus::Processing(Err(ref err)) => {
						warn!(task = %r.task, err = ?err, "Error during task processing");
						let _ = tx_metrics.send_async(r.into()).await;
					}
					rt::JobStatus::Finished(ref _jd) => {
						let selected_domain = { r.ctx.job_state.lock().unwrap().selected_domain.clone() };
						let _ = tx_domain_update.send_async(selected_domain).await;
					}
				}
			}

			Ok(())
		})
	}

	fn job_sender(
		cfg: config::CrustyConfig,
		rx_domain_read_notification: Receiver<DBNotification<Domain>>,
		tx_job: Sender<Job>,
		tx_metrics_db: Sender<DBGenericNotification>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			let default_crawling_settings = Arc::new(cfg.default_crawling_settings);

			while let Ok(notify) = rx_domain_read_notification.recv_async().await {
				let _ = tx_metrics_db.send_async(DBGenericNotification::from(&notify)).await;

				for domain in notify.items {
					let url = domain
						.url
						.as_ref()
						.map(|u| u.to_string())
						.unwrap_or_else(|| format!("http://{}", &domain.domain));
					let job_obj = Job::new_with_shared_settings(
						&url,
						Arc::clone(&default_crawling_settings),
						CrawlingRules {},
						JobState { selected_domain: domain.clone() },
					);

					match job_obj {
						Ok(mut job_obj) => {
							if !domain.addrs.is_empty() {
								job_obj = job_obj.with_addrs(domain.addrs.clone());
							}

							let _ = tx_job.send_async(job_obj).await;
							info!("->sending task  for {}", &domain.domain);
						}
						Err(err) => warn!("->cannot create job for {:?}: {:#}", &domain, err),
					}
				}
			}

			Ok(())
		})
	}

	fn add_queue_measurement<F: Fn() -> usize + Send + Sync + 'static>(&mut self, kind: QueueKind, len_getter: F) {
		self.state.queue_measurements.push(Arc::new(Box::new(move || QueueMeasurement {
			kind:  kind.clone(),
			stats: QueueStats { len: len_getter(), time: now().as_secs() as u32 },
		})))
	}

	fn monitor_queues(
		state: CrustyState,
		cfg: config::CrustyConfig,
		rx_sig: Receiver<()>,
		tx_metrics_queue: Sender<QueueMeasurement>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while !rx_sig.is_disconnected() {
				for ms in state.queue_measurements.iter().map(|measure| measure()) {
					let _ = tx_metrics_queue.send_async(ms).await;
				}

				tokio::time::sleep(*cfg.queue_monitor_interval).await;
			}

			Ok(())
		})
	}

	fn signal_handler(tx_sig: Sender<()>) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while !tx_sig.is_disconnected() {
				if timeout(Duration::from_millis(100), tokio::signal::ctrl_c()).await.is_ok() {
					warn!("Ctrl-C detected - no more accepting new tasks");
					break
				}
			}

			Ok(())
		})
	}

	fn domain_resolver_worker(
		cfg: config::CrustyConfig,
		resolver: Arc<AsyncHyperResolver>,
		rx: Receiver<String>,
		tx_domain_insert: Sender<Domain>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while let Ok(domain_str) = rx.recv_async().await {
				match resolver.resolve(&domain_str).await {
					Ok(addrs) => {
						let addrs = addrs
							.filter(|a| {
								for net in crusty_core::resolver::RESERVED_SUBNETS.iter() {
									if net.contains(&a.ip()) {
										return false
									}
								}
								a.ip().is_ipv4()
							})
							.collect::<Vec<_>>();

						let domain = Domain::new(domain_str, addrs, cfg.jobs.addr_key_mask, None);
						let _ = tx_domain_insert.send_async(domain).await;
					}
					Err(_) => {
						let domain = Domain::new(domain_str, vec![], cfg.jobs.addr_key_mask, None);
						let _ = tx_domain_insert.send_async(domain).await;
					}
				}
			}
			Ok(())
		})
	}

	fn domain_resolver_aggregator(
		cfg: config::CrustyConfig,
		rx: Receiver<String>,
		tx: Sender<String>,
		tx_domain_insert: Sender<Domain>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while let Ok(domain_str) = rx.recv_async().await {
				if tx.try_send(domain_str.clone()).is_err() {
					// send overflow under not-resolved section... no ideal, but works for now
					let domain = Domain::new(domain_str, vec![], cfg.jobs.addr_key_mask, None);
					let _ = tx_domain_insert.send_async(domain).await;
				}
			}
			Ok(())
		})
	}

	fn domain_resolver(&mut self, tx_domain_insert: Sender<Domain>) -> Sender<String> {
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<String>(cfg.concurrency_profile.transit_buffer_size());
		let (tx_out, rx_out) = bounded_ch::<String>(cfg.concurrency_profile.transit_buffer_size());

		for _ in 0..cfg.resolver.concurrency {
			let network_profile = self.cfg.networking_profile.clone().resolve().unwrap();

			self.spawn(Crusty::domain_resolver_worker(
				self.cfg.clone(),
				network_profile.resolver,
				rx.clone(),
				tx_domain_insert.clone(),
			));
		}
		self.spawn(Crusty::domain_resolver_aggregator(self.cfg.clone(), rx_out, tx, tx_domain_insert));

		tx_out
	}

	pub fn spawn(&mut self, task: TracingTask<'static, ()>) {
		let h = tokio::task::spawn(task.instrument());
		self.handles.push(h);
	}

	pub fn go(
		mut self,
		tx_crawler: std::sync::mpsc::Sender<MyMultiCrawler>,
		rx_crawler_done: Receiver<()>,
	) -> TracingTask<'static, ()> {
		TracingTask::new(span!(), async move {
			info!("Checking if clickhouse is ready...");
			while let Err(ref err) = self.try_connect().await {
				warn!("cannot connect to db: {:#}", err);
				time::sleep(Duration::from_secs(1)).await;
			}

			info!("Creating parser processor...");
			let tx_pp = crusty_core::ParserProcessor::spawn(
				self.cfg.concurrency_profile.clone(),
				*self.cfg.parser_processor_stack_size,
			);

			let network_profile = self.cfg.networking_profile.clone().resolve()?;
			info!("Resolved Network Profile: {:?}", &network_profile);

			info!("Creating crawler instance...");
			let (crawler, tx_job, rx_job_state_update) =
				crusty_core::MultiCrawler::new(tx_pp.clone(), self.cfg.concurrency_profile.clone(), network_profile);

			let (tx_sig, rx_sig) = bounded_ch(1);

			self.spawn(Crusty::signal_handler(tx_sig));

			let tx_metrics_task = self.metrics_task_handler();
			let tx_metrics_queue = self.metrics_queue_handler();
			let tx_metrics_db = self.metrics_db_handler();
			let tx_domain_insert = self.domain_enqueue_handler(tx_metrics_db.clone());
			let tx_domain_update = self.domain_finish_handler(tx_metrics_db.clone());
			let tx_domain_resolve = self.domain_resolver(tx_domain_insert.clone());

			{
				let tx_metrics_task = tx_metrics_task.clone();
				let tx_metrics_queue = tx_metrics_queue.clone();
				let tx_metrics_db = tx_metrics_db.clone();
				let tx_domain_insert = tx_domain_insert;
				let tx_domain_update = tx_domain_update.clone();
				let tx_pp = tx_pp;
				let tx_job = tx_job.clone();
				let rx_job_state_update = rx_job_state_update.clone();
				let tx_domain_resolve = tx_domain_resolve.clone();

				self.add_queue_measurement(QueueKind::MetricsTask, move || tx_metrics_task.len());
				self.add_queue_measurement(QueueKind::MetricsQueue, move || tx_metrics_queue.len());
				self.add_queue_measurement(QueueKind::MetricsDB, move || tx_metrics_db.len());
				self.add_queue_measurement(QueueKind::DomainInsert, move || tx_domain_insert.len());
				self.add_queue_measurement(QueueKind::DomainUpdate, move || tx_domain_update.len());
				self.add_queue_measurement(QueueKind::Parse, move || tx_pp.len());
				self.add_queue_measurement(QueueKind::Job, move || tx_job.len());
				self.add_queue_measurement(QueueKind::JobUpdate, move || rx_job_state_update.len());
				self.add_queue_measurement(QueueKind::DomainResolveAggregator, move || tx_domain_resolve.len());
			}

			let ddc = Arc::new(Mutex::new(TtlCache::new(self.cfg.ddc_cap)));
			for _ in 0..(self.cfg.concurrency_profile.domain_concurrency as f64 / 1000_f64).ceil() as usize {
				self.spawn(Crusty::result_handler(
					Arc::clone(&ddc),
					self.cfg.clone(),
					tx_metrics_task.clone(),
					tx_domain_resolve.clone(),
					tx_domain_update.clone(),
					rx_job_state_update.clone(),
				));
			}

			drop(tx_metrics_task);
			drop(tx_domain_resolve);
			drop(tx_domain_update);
			drop(rx_job_state_update);

			let (tx_domain_read_notification, rx_domain_read_notification) = bounded_ch::<DBNotification<Domain>>(1);

			let seed_domains: Vec<_> = self
				.cfg
				.jobs
				.reader
				.seeds
				.iter()
				.filter_map(|seed| Url::parse(seed).ok())
				.map(|seed| {
					Domain::new(seed.domain().unwrap().into(), vec![], self.cfg.jobs.addr_key_mask, Some(seed.clone()))
				})
				.collect();

			let _ = &tx_domain_read_notification
				.send_async(DBNotification {
					table_name: String::from("domains"),
					label:      String::from("insert"),
					since_last: Duration::from_secs(0),
					duration:   Duration::from_secs(0),
					items:      seed_domains,
				})
				.await;

			self.domain_dequeue_handler(tx_domain_read_notification);

			self.spawn(Crusty::job_sender(
				self.cfg.clone(),
				rx_domain_read_notification,
				tx_job,
				tx_metrics_db.clone(),
			));

			self.spawn(Crusty::monitor_queues(self.state.clone(), self.cfg.clone(), rx_sig, tx_metrics_queue));
			drop(self.state);

			let _ = tx_crawler.send(crawler);
			let _ = rx_crawler_done.recv_async().await;

			info!("Waiting for pending ops to finish...");
			let _ = futures::future::join_all(&mut self.handles);
			Ok(())
		})
	}
}
