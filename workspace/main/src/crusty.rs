use cache_2q::Cache;
use clickhouse::Client;
use crusty_core::{resolver::Resolver, types as ct};

use crate::{
	_prelude::*, clickhouse_utils, config, config::ClickhouseWriterConfig, redis_operators, redis_utils::RedisDriver,
	rules::*, types::*,
};

struct ChMeasurements {
	list: Vec<Box<dyn Fn(Duration) -> QueueMeasurement + Send + Sync + 'static>>,
}

struct SenderWeak<T>(Weak<Sender<T>>);
struct ReceiverWeak<T>(Receiver<T>);

trait LenGetter: Send + Sync + 'static {
	fn len(&self) -> usize;
}
impl<T: Send + 'static> LenGetter for SenderWeak<T> {
	fn len(&self) -> usize {
		self.0.upgrade().map(|sender| sender.len()).unwrap_or(0)
	}
}
impl<T: Send + 'static> LenGetter for ReceiverWeak<T> {
	fn len(&self) -> usize {
		self.0.len()
	}
}

impl ChMeasurements {
	fn register<F: LenGetter, S: ToString>(&mut self, name: S, index: usize, len_getter: F) {
		let name = name.to_string();
		self.list.push(Box::new(move |time: Duration| {
			let name = name.clone();
			QueueMeasurement { time, name, index, len: len_getter.len() }
		}));
	}

	fn measure(&self, time: Duration) -> impl Iterator<Item = QueueMeasurement> + '_ {
		self.list.iter().map(move |measure| measure(time))
	}

	async fn monitor(self, rx_crawler_done: Receiver<()>, tx_metrics_queue: Sender<QueueMeasurementDBE>) {
		let cfg = config::config();
		while !rx_crawler_done.is_disconnected() {
			let now = now();
			for m in self.measure(now) {
				let _ = tx_metrics_queue.send_async(m.into()).await;
			}

			tokio::time::sleep(*cfg.metrics.queue_monitor_interval).await;
		}
	}
}

pub struct Crusty {
	ddc:             Arc<Mutex<Cache<String, ()>>>,
	tld:             Arc<HashSet<&'static str>>,
	handles:         Vec<tokio::task::JoinHandle<Result<()>>>,
	ch_measurements: ChMeasurements,

	client: Client,
}

pub struct CrustyHandle {
	pub crawler:       CrustyMultiCrawler,
	pub rx_force_term: Receiver<()>,
}

impl Crusty {
	async fn try_connect(&mut self) -> Result<()> {
		let cfg = &config::config().metrics.clickhouse;
		info!("trying to connect to {} as {}, db: {}", &cfg.url, &cfg.username, &cfg.database);

		let r = self.client.query("SELECT 'ok'").fetch_one::<String>().await?;
		if r == "ok" {
			return Ok(())
		}
		Err(anyhow!("something went wrong"))
	}

	fn parse_tld() -> HashSet<&'static str> {
		include_str!("../tld.txt")
			.split('\n')
			.filter_map(|s| Some(s.trim()).filter(|s| !s.is_empty() && !s.starts_with('#')))
			.collect()
	}

	pub fn new() -> Self {
		let cfg = &config::config();

		let client = Client::default()
			.with_url(&cfg.metrics.clickhouse.url)
			.with_user(&cfg.metrics.clickhouse.username)
			.with_password(&cfg.metrics.clickhouse.password)
			.with_database(&cfg.metrics.clickhouse.database);

		Self {
			ddc: Arc::new(Mutex::new(Cache::new(cfg.domain_discovery.cap))),
			tld: Arc::new(Self::parse_tld()),

			handles: vec![],
			ch_measurements: ChMeasurements { list: vec![] },
			client,
		}
	}

	fn ch<T: Send + 'static, S: ToString>(&mut self, name: S, index: usize, bounds: usize) -> (Sender<T>, Receiver<T>) {
		let (tx, rx) = bounded_ch::<T>(bounds);

		self.ch_measurements.register(name, index, ReceiverWeak(rx.clone()));

		(tx, rx)
	}

	fn ch_trans_with_index<T: Send + 'static, S: ToString>(
		&mut self,
		name: S,
		index: usize,
	) -> (Sender<T>, Receiver<T>) {
		self.ch(name, index, config::config().concurrency.transit_buffer_size())
	}

	fn ch_trans<T: Send + 'static, S: ToString>(&mut self, name: S) -> (Sender<T>, Receiver<T>) {
		self.ch_trans_with_index(name, 0)
	}

	pub fn spawn(&mut self, task: TracingTask<'static, ()>) {
		let h = tokio::task::spawn(task.instrument());
		self.handles.push(h);
	}

	pub fn spawn_bg(&mut self, task: TracingTask<'static, ()>) {
		let _ = tokio::task::spawn(task.instrument());
	}

	fn clickhouse_writer<T: clickhouse_utils::Record>(&mut self, cfg: ClickhouseWriterConfig) -> Sender<T> {
		let (tx, rx) = self.ch_trans::<T, _>(&cfg.table_name);

		for _ in 0..cfg.concurrency {
			let client = self.client.clone();
			let rx = rx.clone();
			let cfg = cfg.clone();
			self.spawn(TracingTask::new(span!(), clickhouse_utils::Writer::new(cfg).go_with_retry(client, rx)));
		}

		tx
	}

	fn domain_topk_writer(&mut self, tx_notify: Sender<DBNotificationDBE>) -> Sender<DomainLinks> {
		let cfg = &config::config().topk;
		let (tx, rx) = self.ch_trans_with_index("domain_topk_insert", 0);

		self.spawn(TracingTask::new(
			span!(),
			RedisDriver::new(&cfg.redis.hosts[0], rx, "domain_topk", "insert", cfg.driver.to_thresholds())
				.go(Box::new(redis_operators::DomainTopKWriter { options: cfg.options.clone() }), tx_notify),
		));

		tx
	}

	fn domain_topk_syncer(&mut self, rx_permit: Receiver<()>, tx_notify: Sender<DBNotification<interop::TopHits>>) {
		let cfg = &config::config().topk;

		self.spawn(TracingTask::new(
			span!(),
			RedisDriver::new(&cfg.redis.hosts[0], rx_permit, "domain_topk", "sync", cfg.driver.to_thresholds())
				.go(Box::new(redis_operators::DomainTopKSyncer { options: cfg.options.clone() }), tx_notify),
		));
	}

	fn domain_inserter(&mut self, shard: usize, tx_notify: Sender<DBNotificationDBE>) -> Sender<Domain> {
		let cfg = &config::config().queue;
		let (tx, rx) = self.ch_trans_with_index("domain_enqueue", shard);

		self.spawn(TracingTask::new(
			span!(),
			RedisDriver::new(&cfg.redis.hosts[shard], rx, "domains", "insert", cfg.jobs.enqueue.driver.to_thresholds())
				.go(Box::new(redis_operators::Enqueue { shard, cfg: cfg.jobs.enqueue.options.clone() }), tx_notify),
		));

		tx
	}

	fn domain_updater(&mut self, shard: usize, tx_notify: Sender<DBNotificationDBE>) -> Sender<Domain> {
		let cfg = &config::config().queue;
		let (tx, rx) = self.ch_trans_with_index("domain_finish", shard);

		self.spawn(TracingTask::new(
			span!(),
			RedisDriver::new(&cfg.redis.hosts[shard], rx, "domains", "update", cfg.jobs.finish.driver.to_thresholds())
				.go(Box::new(redis_operators::Finish { shard, cfg: cfg.jobs.finish.options.clone() }), tx_notify),
		));

		tx
	}

	fn domain_reader(&mut self, shard: usize, rx_permit: Receiver<()>, tx_notify: Sender<DBNotification<Domain>>) {
		let cfg = &config::config().queue;

		self.spawn(TracingTask::new(
			span!(),
			RedisDriver::new(
				&cfg.redis.hosts[shard],
				rx_permit,
				"domains",
				"read",
				cfg.jobs.dequeue.driver.to_thresholds(),
			)
			.go(Box::new(redis_operators::Dequeue { shard, cfg: cfg.jobs.dequeue.options.clone() }), tx_notify),
		));
	}

	fn permit_emitter(&mut self, name: &'static str, delay: Duration, rx_sig_term: Receiver<()>) -> Receiver<()> {
		let (tx, rx) = self.ch(name, 0, 1);

		self.spawn(TracingTask::new(span!(), async move {
			while tx.send_async(()).await.is_ok() {
				tokio::select! {
					_ = time::sleep(delay) => {},
					_ = rx_sig_term.recv_async() => break
				}
			}
			Ok(())
		}));

		rx
	}

	fn result_handler(
		&mut self,
		tx_metrics_task: Sender<TaskMeasurementDBE>,
		tx_metrics_job: Sender<JobMeasurementDBE>,
		tx_domain_insert: Sender<String>,
		tx_domain_update: Vec<Sender<Domain>>,
		rx_job_state_update: Receiver<ct::JobUpdate<JobState, TaskState>>,
		tx_domain_links: Sender<DomainLinks>,
	) {
		let ddc = Arc::clone(&self.ddc);
		let tld = Arc::clone(&self.tld);

		self.spawn(TracingTask::new(span!(), async move {
			while let Ok(r) = rx_job_state_update.recv_async().await {
				info!("- {}", r);

				let task_domain = r.task.link.host().unwrap();

				let domain_filter_map = |lnk: &Arc<ct::Link>| {
					let domain = lnk.host()?;

					if domain.len() < 4 || !domain.contains('.') || domain == *task_domain {
						return None
					}

					let domain_tld = domain.split('.').last().unwrap().to_uppercase();
					if !tld.contains(domain_tld.as_str()) {
						return None
					}

					r.ctx.job_state.lock().unwrap().link_domain(&domain);

					{
						let mut ddc = ddc.lock().unwrap();
						if ddc.contains_key(&domain) {
							return None
						}
						ddc.insert(domain.clone(), ());
					}

					info!("new domain discovered: {}", &domain);
					Some(domain)
				};

				match r.status {
					ct::JobStatus::Processing(Ok(ref jp)) => {
						let discovered_domains = jp.links.iter().filter_map(domain_filter_map);

						for domain in discovered_domains {
							let _ = tx_domain_insert.send_async(domain).await;
						}
						let _ = tx_metrics_task.send_async(r.into()).await;
					}
					ct::JobStatus::Processing(Err(ref err)) => {
						warn!(task = %r.task, err = ?err, "Error during task processing");
						let _ = tx_metrics_task.send_async(r.into()).await;
					}
					ct::JobStatus::Finished(ref _jd) => {
						let (selected_domain, linked_domains) = {
							let js = r.ctx.job_state.lock().unwrap();
							(js.selected_domain.clone(), js.linked_domains())
						};

						let _ = tx_domain_links.send_async(linked_domains).await;
						let _ = tx_domain_update[selected_domain.shard].send_async(selected_domain).await;
						let _ = tx_metrics_job.send_async(r.into()).await;
					}
				}
			}

			Ok(())
		}));
	}

	fn job_sender(
		&mut self,
		rx_domain_read_notify: Receiver<DBNotification<Domain>>,
		tx_job: Arc<Sender<Job>>,
		tx_metrics_db: Sender<DBNotificationDBE>,
	) {
		let cfg = config::config();

		self.spawn(TracingTask::new(span!(), async move {
			let default_crawling_settings = Arc::new(cfg.default_crawling_settings.clone());

			while let Ok(notify) = rx_domain_read_notify.recv_async().await {
				for domain in &notify.items {
					let url = domain
						.url
						.as_ref()
						.map(|u| u.to_string())
						.unwrap_or_else(|| format!("http://{}", &domain.domain));

					let job_obj = Job::new_with_shared_settings(
						&url,
						Arc::clone(&default_crawling_settings),
						CrawlingRules {},
						JobState::new(domain),
					);

					match job_obj {
						Ok(mut job_obj) => {
							if !domain.addrs.is_empty() {
								job_obj =
									job_obj.with_addrs(domain.addrs.first().map(|a| vec![*a]).unwrap_or_else(Vec::new));
							}

							if tx_job.send_async(job_obj).await.is_err() {
								break
							}
							info!("->sent task  for {}", &domain.domain);
						}
						Err(err) => warn!("->cannot create job for {:?}: {:#}", &domain, err),
					}
				}

				let _ = tx_metrics_db.send_async(notify.into()).await;
			}

			Ok(())
		}));
	}

	fn signal_handler(&mut self) -> (Receiver<()>, Receiver<()>) {
		let (tx_sig_term, rx_sig_term) = bounded_ch(0);
		let (tx_force_term, rx_force_term) = bounded_ch(0);

		let graceful_timeout = *config::config().shutdown.graceful_timeout;

		enum SigTerm {
			Tx(Sender<()>),
			Instant(Instant),
		}

		self.spawn_bg(TracingTask::new(span!(), async move {
			let mut sig_term = SigTerm::Tx(tx_sig_term);

			loop {
				let timeout = time::sleep(Duration::from_millis(100));

				tokio::select! {
					_ = tokio::signal::ctrl_c() => {
						if graceful_timeout.as_millis() < 1 {
							warn!("Ctrl-C detected, but graceful_timeout is zero - switching to immediate shutdown mode");
							break
						}

						warn!("Ctrl-C detected - no more accepting new jobs, awaiting graceful termination for {}s.", graceful_timeout.as_secs());
						if let SigTerm::Instant(_) = &sig_term {
							warn!("Ctrl-C detected while awaiting for graceful termination - switching to immediate shutdown mode");
							break
						}
						sig_term = SigTerm::Instant(Instant::now());
					}
					_ = timeout => {}
				}

				match &sig_term {
					SigTerm::Tx(tx_sig_term) => if tx_sig_term.is_disconnected() { break },
					SigTerm::Instant(sig_term_instant) => if sig_term_instant.elapsed() > graceful_timeout {
						warn!("Failed to exit within graceful timeout({}s.) - switching to immediate shutdown mode", graceful_timeout.as_secs());
						break
					}
				}
			}

			drop (tx_force_term);
			Ok(())
		}));

		(rx_sig_term, rx_force_term)
	}

	fn domain_resolver_worker(
		resolver: Arc<Box<dyn Resolver>>,
		rx_sig_term: Receiver<()>,
		rx: Receiver<String>,
		tx_domain_insert: Vec<Sender<Domain>>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while let Ok(domain_str) = rx.recv_async().await {
				let r = tokio::select! {
					r = resolver.resolve(&domain_str) => r,
					_ = rx_sig_term.recv_async() => break
				};

				match r {
					Ok(addrs) => {
						let domain = Domain::new(domain_str, addrs.collect::<Vec<_>>(), None);
						if let Some(domain) = domain {
							let _ = tx_domain_insert[domain.shard].send_async(domain).await;
						}
					}
					Err(err) => {
						info!(domain = %domain_str, err = ?err, "Could not resolve");
					}
				}
			}
			Ok(())
		})
	}

	fn domain_resolver_aggregator(rx: Receiver<String>, tx: Sender<String>) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			let mut overflow = 0_u32;
			let mut last_overflow_check = Instant::now();
			while let Ok(domain_str) = rx.recv_async().await {
				if tx.try_send(domain_str).is_err() {
					overflow += 1;
				}

				if last_overflow_check.elapsed().as_millis() > 3000 && overflow > 0 {
					warn!("Domains discarded as overflow(over resolver's capacity): {}", overflow);
					last_overflow_check = Instant::now();
					overflow = 0;
				}
			}
			Ok(())
		})
	}

	fn domain_resolver(&mut self, rx_sig_term: Receiver<()>, tx_domain_insert: Vec<Sender<Domain>>) -> Sender<String> {
		let cfg = config::config();
		let (tx, rx) = self.ch("domain_resolver_in", 0, 1);
		let (tx_out, rx_out) = self.ch_trans("domain_resolver_out");

		for _ in 0..1 {
			let network_profile = cfg.networking.clone().resolve().unwrap();

			self.spawn(Crusty::domain_resolver_worker(
				network_profile.resolver,
				rx_sig_term.clone(),
				rx.clone(),
				tx_domain_insert.clone(),
			));
		}
		self.spawn(Crusty::domain_resolver_aggregator(rx_out, tx));

		tx_out
	}

	fn domain_topk_plex(
		&mut self,
		rx_notify: Receiver<DBNotification<interop::TopHits>>,
		tx_ch: Sender<TopHitsDBE>,
		tx_metrics_db: Sender<DBNotificationDBE>,
	) {
		self.spawn(TracingTask::new(span!(), async move {
			while let Ok(notify) = rx_notify.recv_async().await {
				for th in &notify.items {
					let _ = tx_ch.send_async(th.into()).await;
				}
				let _ = tx_metrics_db.send_async(notify.into()).await;
			}

			Ok(())
		}));
	}

	async fn send_seed_jobs(&self, tx_domain_resolve: Sender<String>) {
		let cfg = &config::config().queue;
		let seed_domains: Vec<_> = cfg
			.jobs
			.reader
			.seeds
			.iter()
			.filter_map(|seed| Url::parse(seed).ok())
			.filter_map(|seed| {
				if let Some(domain) = seed.domain() {
					Some(domain.into())
				} else {
					warn!("Seed url {} has no domain name - skipping!", seed.to_string());
					None
				}
			})
			.collect();

		for seed_domain in seed_domains {
			tx_domain_resolve.send_async(seed_domain).await.unwrap();
		}
	}

	async fn crawler(&mut self) -> Result<(CrustyMultiCrawler, Receiver<()>, Sender<QueueMeasurementDBE>)> {
		let cfg = &config::config();

		let network_profile = cfg.networking.clone().resolve()?;
		info!("Resolved Network Profile: {:?}", &network_profile);

		let concurrency_profile = cfg.concurrency.clone();

		let (rx_sig_term, rx_force_term) = self.signal_handler();

		info!("Creating parser processor...");
		let tx_pp = crusty_core::ParserProcessor::spawn(concurrency_profile.clone(), cfg.parser.clone());
		self.ch_measurements.register("parser", 0, SenderWeak(Arc::downgrade(&tx_pp)));

		info!("Creating crawler instance...");
		let (crawler, tx_job, rx_job_state_update) =
			crusty_core::MultiCrawler::new(tx_pp, concurrency_profile.clone(), network_profile);
		let tx_job = Arc::new(tx_job);

		self.ch_measurements.register("job", 0, SenderWeak(Arc::downgrade(&tx_job)));
		self.ch_measurements.register("job_state_update", 0, ReceiverWeak(rx_job_state_update.clone()));

		let tx_ch_metrics_task = self.clickhouse_writer(cfg.metrics.metrics_task.clone());
		let tx_ch_metrics_job = self.clickhouse_writer(cfg.metrics.metrics_job.clone());
		let tx_ch_metrics_queue = self.clickhouse_writer(cfg.metrics.metrics_queue.clone());
		let tx_ch_metrics_db = self.clickhouse_writer(cfg.metrics.metrics_db.clone());
		let tx_ch_topk = self.clickhouse_writer(cfg.metrics.topk.clone());

		//
		let (tx_domain_topk_notify, rx_domain_topk_notify) = self.ch_trans("domain_topk_notify");
		let rx_domain_topk_permit =
			self.permit_emitter("domain_topk_permit", *cfg.topk.options.consume_interval, rx_sig_term.clone());
		self.domain_topk_syncer(rx_domain_topk_permit, tx_domain_topk_notify);
		self.domain_topk_plex(rx_domain_topk_notify, tx_ch_topk, tx_ch_metrics_db.clone());
		//

		let scoped_shard_range = cfg.queue.jobs.shard_min..cfg.queue.jobs.shard_max;
		let total_shard_range = 0..cfg.queue.jobs.shard_total;

		let tx_domain_insert = total_shard_range
			.clone()
			.map(|shard| self.domain_inserter(shard, tx_ch_metrics_db.clone()))
			.collect::<Vec<_>>();
		let tx_domain_update = scoped_shard_range
			.clone()
			.map(|shard| self.domain_updater(shard, tx_ch_metrics_db.clone()))
			.collect::<Vec<_>>();

		let tx_domain_links = self.domain_topk_writer(tx_ch_metrics_db.clone());

		let (tx_domain_read_notify, rx_domain_read_notify) = self.ch("domain_read_notify", 0, 1);

		let rx_domain_read_permit = self.permit_emitter(
			"domain_read_permit",
			*cfg.queue.jobs.dequeue.options.emit_permit_delay,
			rx_sig_term.clone(),
		);
		for shard in scoped_shard_range.clone() {
			self.domain_reader(shard, rx_domain_read_permit.clone(), tx_domain_read_notify.clone());
			self.job_sender(rx_domain_read_notify.clone(), tx_job.clone(), tx_ch_metrics_db.clone());
		}

		let tx_domain_resolve = self.domain_resolver(rx_force_term.clone(), tx_domain_insert.clone());

		for _ in 0..(concurrency_profile.domain_concurrency as f64 / 1000_f64).ceil() as usize {
			self.result_handler(
				tx_ch_metrics_task.clone(),
				tx_ch_metrics_job.clone(),
				tx_domain_resolve.clone(),
				tx_domain_update.clone(),
				rx_job_state_update.clone(),
				tx_domain_links.clone(),
			);
		}

		self.send_seed_jobs(tx_domain_resolve).await;

		Ok((crawler, rx_force_term, tx_ch_metrics_queue))
	}

	pub fn go(
		mut self,
		tx_crusty: std::sync::mpsc::Sender<CrustyHandle>,
		rx_crawler_done: Receiver<()>,
	) -> TracingTask<'static, ()> {
		TracingTask::new(span!(), async move {
			info!("Checking if clickhouse is ready...");
			while let Err(ref err) = self.try_connect().await {
				warn!("cannot connect to db: {:#}", err);
				time::sleep(Duration::from_secs(1)).await;
			}

			let (crawler, rx_force_term, tx_metrics_queue) = self.crawler().await?;

			tx_crusty.send(CrustyHandle { crawler, rx_force_term }).unwrap();

			self.ch_measurements.monitor(rx_crawler_done, tx_metrics_queue).await;

			info!("Waiting for pending ops to finish...");
			let _ = futures::future::join_all(&mut self.handles).await;
			Ok(())
		})
	}
}
