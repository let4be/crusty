use backoff::{future::retry, ExponentialBackoff};
use clickhouse::{Client, Row};
use serde::Serialize;

#[allow(unused_imports)]
use crate::prelude::*;
use crate::{config::*, types::*};

#[derive(Clone)]
pub struct Writer {
	cfg: ClickhouseWriterConfig,
}

pub struct WriterState<A: Clone + Send> {
	notify: Vec<A>,
}

impl<A: Clone + Send> From<&DBNotification<A>> for DBGenericNotification {
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

impl Writer {
	pub fn new(cfg: ClickhouseWriterConfig) -> Self {
		Self { cfg }
	}

	pub async fn go_with_retry<
		A: Clone + std::fmt::Debug + Send + 'static,
		R: Row + Serialize + Send + Sync + 'static,
		F: Fn(A) -> R + Send + Sync + 'static,
	>(
		&self,
		client: Client,
		rx: Receiver<A>,
		tx_notify: Option<Sender<DBNotification<A>>>,
		map: F,
	) -> Result<()> {
		let state = Arc::new(Mutex::new(WriterState { notify: Vec::new() }));
		let map = Arc::new(map);
		retry(ExponentialBackoff { max_elapsed_time: None, ..ExponentialBackoff::default() }, || async {
			Writer::go(
				self.cfg.clone(),
				client.clone(),
				rx.clone(),
				tx_notify.clone(),
				Arc::clone(&map),
				state.clone(),
			)
			.instrument()
			.await
			.map_err(backoff::Error::Transient)?;
			Ok(())
		})
		.await
	}

	pub fn go<
		A: Clone + std::fmt::Debug + Send + 'static,
		R: Row + Serialize + Send + Sync + 'static,
		F: Fn(A) -> R + Send + Sync + 'static,
	>(
		cfg: ClickhouseWriterConfig,
		client: Client,
		rx: Receiver<A>,
		tx_notify: Option<Sender<DBNotification<A>>>,
		map: Arc<F>,
		state: Arc<Mutex<WriterState<A>>>,
	) -> TracingTask<'static> {
		TracingTask::new(span!(), async move {
			let mut inserter = client
				.inserter(cfg.table_name.as_str())?
				.with_max_entries(cfg.buffer_capacity as u64)
				.with_max_duration(*cfg.force_write_duration);

			let mut last_write = Instant::now();

			let notify = { state.lock().unwrap().notify.clone() };
			for el in notify.into_iter() {
				let mapped = map(el);
				inserter.write(&mapped).await?;
			}

			let mut done = false;
			while !done {
				let t = Instant::now();

				let s = inserter.commit().await.context("error during inserter.commit")?;

				let since_last = last_write.elapsed();
				let write_took = t.elapsed();

				if s.entries > 0 {
					info!(
						"Clickhouse write to {}/{}({} rows, {} transactions, {}ms since last) finished for {}ms.",
						&cfg.table_name,
						&cfg.label,
						s.entries,
						s.transactions,
						since_last.as_millis(),
						write_took.as_millis()
					);
					if let Some(tx_notify) = &tx_notify {
						let n = DBNotification {
							label: cfg.label.clone(),
							table_name: cfg.table_name.clone(),
							since_last,
							duration: write_took,
							items: { state.lock().unwrap().notify.clone() },
						};
						let _ = tx_notify.send_async(n).await;
					}
					state.lock().unwrap().notify.clear();
					last_write = Instant::now();
				}

				if let Ok(r) = timeout(*cfg.check_for_force_write_duration, rx.recv_async()).await {
					match r {
						Ok(el) => {
							state.lock().unwrap().notify.push(el.clone());
							inserter.write(&map(el)).await.context("error during inserter.write")?;
						}
						_ => {
							done = true;
						}
					}
				}
			}

			// if we're stopping, we don't care much about notifications or logs(for now...)
			inserter.end().await?;

			Ok(())
		})
	}
}
