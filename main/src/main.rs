mod clickhouse_utils;
mod config;
mod job_reader;
mod prelude;
mod rules;
mod types;

use std::net::IpAddr;

use clickhouse::Client;
use crusty_core::{self, prelude::AsyncHyperResolver, resolver::Resolver, types as rt};
use tracing_subscriber::EnvFilter;
use ttl_cache::TtlCache;

#[allow(unused_imports)]
use crate::{
	prelude::*,
	{clickhouse_utils as chu, config::CrustyConfig, rules::*, types::*},
};

#[derive(Clone)]
struct CrustyState {
	client:             Client,
	queue_measurements: Vec<Arc<Box<dyn Fn() -> QueueMeasurement + Send + Sync>>>,
}

struct Crusty {
	handles: Vec<tokio::task::JoinHandle<Result<()>>>,
	state:   CrustyState,
	cfg:     config::CrustyConfig,
}

impl Crusty {
	async fn try_connect(cfg: &config::CrustyConfig) -> Result<Client> {
		let client = Client::default()
			.with_url(&cfg.clickhouse.url)
			.with_user(&cfg.clickhouse.username)
			.with_password(&cfg.clickhouse.password)
			.with_database(&cfg.clickhouse.database);

		let r = client.query("SELECT 'ok'").fetch_one::<String>().await?;
		if r == "ok" {
			return Ok(client)
		}
		Err(anyhow!("something went wrong"))
	}

	async fn new(cfg: config::CrustyConfig) -> Self {
		let client = loop {
			match Self::try_connect(&cfg).await {
				Err(ref err) => warn!("cannot connect to db: {:#}", err),
				Ok(c) => break c,
			}

			time::sleep(Duration::from_secs(1)).await;
		};

		Self { handles: vec![], state: CrustyState { client, queue_measurements: vec![] }, cfg }
	}

	fn metrics_queue_handler(&mut self) -> Sender<Vec<QueueMeasurement>> {
		let state = self.state.clone();
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<Vec<QueueMeasurement>>(cfg.concurrency_profile.transit_buffer_size());
		self.spawn(TracingTask::new(span!(), async move {
			let writer = clickhouse_utils::Writer::new(cfg.clickhouse.metrics_queue);
			writer.go_with_retry(state.client, rx, None, QueueMeasurementDBEntry::from).await
		}));
		tx
	}

	fn metrics_db_handler(&mut self) -> Sender<Vec<chu::GenericNotification>> {
		let state = self.state.clone();
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<Vec<chu::GenericNotification>>(cfg.concurrency_profile.transit_buffer_size());
		self.spawn(TracingTask::new(span!(), async move {
			let writer = clickhouse_utils::Writer::new(cfg.clickhouse.metrics_db);
			writer.go_with_retry(state.client, rx, None, DBRWNotificationDBEntry::from).await
		}));
		tx
	}

	fn metrics_task_handler(&mut self) -> Sender<Vec<TaskMeasurement>> {
		let state = self.state.clone();
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<Vec<TaskMeasurement>>(cfg.concurrency_profile.transit_buffer_size());
		self.spawn(TracingTask::new(span!(), async move {
			let writer = clickhouse_utils::Writer::new(cfg.clickhouse.metrics_task);
			writer.go_with_retry(state.client, rx, None, TaskMeasurementDBEntry::from).await
		}));
		tx
	}

	fn domain_insert_handler(&mut self, tx_notify: Sender<chu::Notification<Domain>>) -> Sender<Vec<Domain>> {
		let state = self.state.clone();
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<Vec<Domain>>(cfg.concurrency_profile.transit_buffer_size());
		self.spawn(TracingTask::new(span!(), async move {
			let writer = clickhouse_utils::Writer::new(cfg.clickhouse.domain_discovery_insert);
			writer.go_with_retry(state.client, rx, Some(tx_notify), DomainDBEntry::from).await
		}));
		tx
	}

	fn domain_update_handler(&mut self, tx_notify: Sender<chu::Notification<Domain>>) -> Sender<Vec<Domain>> {
		let state = self.state.clone();
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<Vec<Domain>>(cfg.concurrency_profile.transit_buffer_size());
		self.spawn(TracingTask::new(span!(), async move {
			let writer = clickhouse_utils::Writer::new(cfg.clickhouse.domain_discovery_update);
			writer.go_with_retry(state.client, rx, Some(tx_notify), DomainDBEntry::from).await
		}));
		tx
	}

	fn domain_filter_map(
		lnk: Arc<rt::Link>,
		task_domain: &str,
		ddc: &mut TtlCache<String, ()>,
		cfg: &CrustyConfig,
	) -> Option<String> {
		let domain = lnk.host()?;

		if domain.len() < 3 || !domain.contains('.') || domain == *task_domain || ddc.contains_key(&domain) {
			return None
		}
		ddc.insert(domain.clone(), (), *cfg.ddc_lifetime);

		info!("new domain discovered: {}", &domain);
		Some(domain)
	}

	fn result_handler(
		cfg: config::CrustyConfig,
		tx_metrics: Sender<Vec<TaskMeasurement>>,
		tx_domain_insert: Sender<String>,
		tx_domain_update: Sender<Vec<Domain>>,
		rx_job_state_update: Receiver<rt::JobUpdate<JobState, TaskState>>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			let mut ddc: TtlCache<String, ()> = TtlCache::new(cfg.ddc_cap);

			while let Ok(r) = rx_job_state_update.recv_async().await {
				info!("- {}", r);

				let task_domain = r.task.link.host().unwrap();
				match r.status {
					rt::JobStatus::Processing(Ok(ref jp)) => {
						let domains: Vec<_> = jp
							.links
							.iter()
							.filter_map(|lnk| Crusty::domain_filter_map(Arc::clone(lnk), &task_domain, &mut ddc, &cfg))
							.collect();

						let _ = tokio::join!(
							async {
								for domain in domains {
									let _ = tx_domain_insert.send_async(domain).await;
								}
							},
							tx_metrics.send_async(vec![r.into()])
						);
					}
					rt::JobStatus::Processing(Err(_)) => {
						let _ = tx_metrics.send_async(vec![r.into()]).await;
					}
					rt::JobStatus::Finished(ref _jd) => {
						let selected_domain = { r.ctx.job_state.lock().unwrap().selected_domain.clone() };
						let _ = tx_domain_update.send_async(vec![selected_domain]).await;
					}
				}
			}

			Ok(())
		})
	}

	fn domain_notification_plex(
		rx_domain_insert_notify: Receiver<chu::Notification<Domain>>,
		rx_domain_update_notify: Receiver<chu::Notification<Domain>>,

		tx_metrics_db: Sender<Vec<chu::GenericNotification>>,
		tx_domain_update_notify: Sender<chu::Notification<Domain>>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			loop {
				tokio::select! {
					Ok(r) = rx_domain_insert_notify.recv_async() => {
						let _ = tx_metrics_db.send_async(vec![r.into()]).await;
					},
					Ok(r) = rx_domain_update_notify.recv_async() => {
						let _ = tokio::join!(
							tx_domain_update_notify.send_async(r.clone()),
							tx_metrics_db.send_async(vec![r.into()]),
						);
					},
					else => break
				}
			}

			Ok(())
		})
	}

	fn add_queue_measurement<F: Fn() -> usize + Send + Sync + 'static>(&mut self, kind: QueueKind, len_getter: F) {
		self.state.queue_measurements.push(Arc::new(Box::new(move || QueueMeasurement {
			kind:  kind.clone(),
			stats: QueueStats { len: len_getter(), time: now() },
		})))
	}

	fn monitor_queues(
		state: CrustyState,
		cfg: config::CrustyConfig,
		rx_sig: Receiver<()>,
		tx_metrics_queue: Sender<Vec<QueueMeasurement>>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while !rx_sig.is_disconnected() {
				let ms = state.queue_measurements.iter().map(|measure| measure()).collect();

				let _ = tx_metrics_queue.send_async(ms).await;
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

	fn job_reader(
		state: CrustyState,
		cfg: config::CrustyConfig,
		tx_job: Sender<Job>,
		rx_sig: Receiver<()>,
		tx_metrics_db: Sender<Vec<chu::GenericNotification>>,
		rx_domain_update_notify_p: Receiver<chu::Notification<Domain>>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			let job_reader = job_reader::JobReader::new(cfg.job_reader);
			job_reader.go(state.client.clone(), tx_job, rx_sig, tx_metrics_db, rx_domain_update_notify_p).await?;
			Ok(())
		})
	}

	fn domain_resolver_worker(
		cfg: config::CrustyConfig,
		resolver: Arc<AsyncHyperResolver>,
		rx: Receiver<String>,
		tx_domain_insert: Sender<Vec<Domain>>,
		rx_sig: Receiver<()>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while !rx_sig.is_disconnected() {
				if let Ok(domain_str) = rx.recv_async().await {
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
								.map(|a| {
									if let IpAddr::V4(ip) = a.ip() {
										ip.octets()
									} else {
										panic!("not supposed to happen")
									}
								})
								.collect::<Vec<_>>();

							let domain = Domain::new(
								domain_str,
								addrs,
								cfg.job_reader.shard_total,
								cfg.job_reader.addr_key_mask,
								None,
								true,
							);
							let _ = tx_domain_insert.send_async(vec![domain]).await;
						}
						Err(_) => {
							let domain = Domain::new(
								domain_str,
								vec![],
								cfg.job_reader.shard_total,
								cfg.job_reader.addr_key_mask,
								None,
								true,
							);
							let _ = tx_domain_insert.send_async(vec![domain]).await;
						}
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
		tx_domain_insert: Sender<Vec<Domain>>,
		rx_sig: Receiver<()>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			while !rx_sig.is_disconnected() {
				if let Ok(domain_str) = rx.recv_async().await {
					if tx.try_send(domain_str.clone()).is_err() {
						// send overflow to db persistence under not-resolved section... no ideal, but works for now
						let domain = Domain::new(
							domain_str,
							vec![],
							cfg.job_reader.shard_total,
							cfg.job_reader.addr_key_mask,
							None,
							true,
						);
						let _ = tx_domain_insert.send_async(vec![domain]).await;
					}
				}
			}
			Ok(())
		})
	}

	fn domain_resolver(
		&mut self,
		resolver: Arc<AsyncHyperResolver>,
		tx_domain_insert: Sender<Vec<Domain>>,
		rx_sig: Receiver<()>,
	) -> Sender<String> {
		let cfg = self.cfg.clone();
		let (tx, rx) = bounded_ch::<String>(cfg.concurrency_profile.transit_buffer_size() * 10);
		let (out_tx, out_rx) = bounded_ch::<String>(cfg.concurrency_profile.transit_buffer_size());

		for _ in 0..cfg.resolver.concurrency {
			self.spawn(Crusty::domain_resolver_worker(
				self.cfg.clone(),
				resolver.clone(),
				rx.clone(),
				tx_domain_insert.clone(),
				rx_sig.clone(),
			));
		}
		self.spawn(Crusty::domain_resolver_aggregator(self.cfg.clone(), out_rx, tx, tx_domain_insert, rx_sig));

		out_tx
	}

	pub fn spawn(&mut self, task: TracingTask<'static, ()>) {
		let h = tokio::spawn(task.instrument());
		self.handles.push(h);
	}

	fn go(mut self) -> PinnedFut<'static, ()> {
		TracingTask::new(span!(), async move {
			info!("Creating parser processor...");
			let pp = crusty_core::ParserProcessor::spawn(
				self.cfg.concurrency_profile.clone(),
				*self.cfg.parser_processor_stack_size,
			);

			let network_profile = self.cfg.networking_profile.clone().resolve()?;
			info!("Resolved Network Profile: {:?}", &network_profile);

			let resolver = Arc::clone(&network_profile.resolver);

			info!("Creating crawler instance...");
			let (crawler, tx_job, rx_job_state_update) =
				crusty_core::MultiCrawler::new(&pp, self.cfg.concurrency_profile.clone(), network_profile);

			let (tx_sig, rx_sig) = bounded_ch(1);

			self.spawn(Crusty::signal_handler(tx_sig));

			let (tx_domain_insert_notify, rx_domain_insert_notify) =
				bounded_ch::<chu::Notification<Domain>>(self.cfg.concurrency_profile.transit_buffer_size());
			let (tx_domain_update_notify, rx_domain_update_notify) =
				bounded_ch::<chu::Notification<Domain>>(self.cfg.concurrency_profile.transit_buffer_size());
			let (tx_domain_update_notify_p, rx_domain_update_notify_p) =
				bounded_ch::<chu::Notification<Domain>>(self.cfg.concurrency_profile.transit_buffer_size());

			let tx_metrics_task = self.metrics_task_handler();
			let tx_metrics_queue = self.metrics_queue_handler();
			let tx_metrics_db = self.metrics_db_handler();
			let tx_domain_insert = self.domain_insert_handler(tx_domain_insert_notify.clone());
			let tx_domain_update = self.domain_update_handler(tx_domain_update_notify.clone());

			let domain_resolve_tx = self.domain_resolver(resolver, tx_domain_insert.clone(), rx_sig.clone());

			{
				let tx_domain_insert_notify = tx_domain_insert_notify;
				let tx_domain_update_notify = tx_domain_update_notify;
				let tx_domain_update_notify_p = tx_domain_update_notify_p.clone();

				let tx_metrics_task = tx_metrics_task.clone();
				let tx_metrics_queue = tx_metrics_queue.clone();
				let tx_metrics_db = tx_metrics_db.clone();
				let tx_domain_insert = tx_domain_insert;
				let tx_domain_update = tx_domain_update.clone();
				let tx_job = tx_job.clone();
				let rx_job_state_update = rx_job_state_update.clone();

				self.add_queue_measurement(QueueKind::DomainInsertNotify, move || tx_domain_insert_notify.len());
				self.add_queue_measurement(QueueKind::DomainUpdateNotify, move || tx_domain_update_notify.len());
				self.add_queue_measurement(QueueKind::DomainUpdateNotifyP, move || tx_domain_update_notify_p.len());
				self.add_queue_measurement(QueueKind::MetricsTask, move || tx_metrics_task.len());
				self.add_queue_measurement(QueueKind::MetricsQueue, move || tx_metrics_queue.len());
				self.add_queue_measurement(QueueKind::MetricsDB, move || tx_metrics_db.len());
				self.add_queue_measurement(QueueKind::DomainInsert, move || tx_domain_insert.len());
				self.add_queue_measurement(QueueKind::DomainUpdate, move || tx_domain_update.len());
				self.add_queue_measurement(QueueKind::Job, move || tx_job.len());
				self.add_queue_measurement(QueueKind::JobUpdate, move || rx_job_state_update.len());
			}

			self.spawn(Crusty::result_handler(
				self.cfg.clone(),
				tx_metrics_task,
				domain_resolve_tx,
				tx_domain_update,
				rx_job_state_update,
			));

			self.spawn(Crusty::job_reader(
				self.state.clone(),
				self.cfg.clone(),
				tx_job,
				rx_sig.clone(),
				tx_metrics_db.clone(),
				rx_domain_update_notify_p,
			));

			self.spawn(Crusty::domain_notification_plex(
				rx_domain_insert_notify,
				rx_domain_update_notify,
				tx_metrics_db,
				tx_domain_update_notify_p,
			));

			self.spawn(Crusty::monitor_queues(self.state.clone(), self.cfg.clone(), rx_sig, tx_metrics_queue));
			drop(self.state);

			info!("Crawling is a go...");
			crawler.go().await?;

			info!("Waiting for pending ops to finish...");
			let _ = futures::future::join_all(&mut self.handles);

			Ok(())
		})
		.instrument()
	}
}

async fn go() -> Result<()> {
	let cr = config::load();
	match cr {
		Ok(_) => {
			println!("Loading config: ok");
		}
		Err(err) => {
			println!("Loading config err: '{:?}' - using defaults", err)
		}
	}
	let cfg = { config::CONFIG.lock().unwrap().clone() };

	let mut filter = EnvFilter::from_default_env().add_directive((*cfg.log.level.clone()).into());
	if let Some(filters) = &cfg.log.filter {
		for filter_str in filters {
			filter = filter.add_directive(filter_str.parse()?);
		}
	}

	let collector =
		tracing_subscriber::fmt().with_env_filter(filter).with_target(false).with_ansi(cfg.log.ansi).finish();

	tracing::subscriber::set_global_default(collector)?;
	println!("Log system configured...: {} with filtering: {:?}", *cfg.log.level, cfg.log.filter);
	println!("{:#?}", &cfg);

	if cfg.job_reader.seeds.is_empty() {
		return Err(anyhow!("Consider specifying one or more seed URLs in config.toml, see job_reader.seeds property"))
	}

	let new_fd_lim = fdlimit::raise_fd_limit();
	println!("New FD limit set: {:?}", new_fd_lim);

	// ---
	let crusty = Crusty::new(cfg).await;
	crusty.go().await
}

fn main() -> Result<()> {
	println!("Starting Crusty...");
	println!(
		"Built {} on {} with {} rustc(profile: {}) for target {}",
		env!("VERGEN_GIT_SHA"),
		env!("VERGEN_BUILD_TIMESTAMP"),
		env!("VERGEN_RUSTC_SEMVER"),
		env!("VERGEN_CARGO_PROFILE"),
		env!("VERGEN_CARGO_TARGET_TRIPLE"),
	);
	let rt = tokio::runtime::Runtime::new().unwrap();
	rt.block_on(go())
}
