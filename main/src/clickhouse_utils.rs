use backoff::{future::retry, ExponentialBackoff};
use clickhouse::{Client, Row};
use serde::Serialize;

use crate::{_prelude::*, config::*, types::*};

#[derive(Clone)]
pub struct Writer {
	cfg: ClickhouseWriterConfig,
}

pub struct WriterState<A: Clone + Send> {
	notify: Vec<A>,
}

impl Writer {
	pub fn new(cfg: ClickhouseWriterConfig) -> Self {
		Self { cfg }
	}

	pub async fn go_with_retry<A: Row + Serialize + Clone + Debug + Send + Sync + 'static>(
		&self,
		client: Client,
		rx: Receiver<A>,
	) -> Result<()> {
		let state = Arc::new(Mutex::new(WriterState { notify: Vec::new() }));
		retry(ExponentialBackoff { max_elapsed_time: None, ..ExponentialBackoff::default() }, || async {
			Writer::go(self.cfg.clone(), client.clone(), rx.clone(), state.clone())
				.instrument()
				.await
				.map_err(backoff::Error::Transient)?;
			Ok(())
		})
		.await
	}

	pub fn go<A: Row + Serialize + Clone + Debug + Send + Sync + 'static>(
		cfg: ClickhouseWriterConfig,
		client: Client,
		rx: Receiver<A>,
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
				inserter.write(&el).await?;
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
					state.lock().unwrap().notify.clear();
					last_write = Instant::now();
				}

				if let Ok(r) = timeout(*cfg.check_for_force_write_duration, rx.recv_async()).await {
					match r {
						Ok(el) => {
							state.lock().unwrap().notify.push(el.clone());
							inserter.write(&el).await.context("error during inserter.write")?;
						}
						_ => {
							done = true;
						}
					}
				}
			}

			inserter.end().await?;

			Ok(())
		})
	}
}
