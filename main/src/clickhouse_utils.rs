use backoff::{future::retry, ExponentialBackoff};
use clickhouse::{Client, Row};
use serde::Serialize;

use crate::{_prelude::*, config::*, types::*};

#[derive(Clone)]
pub struct Writer {
	cfg: ClickhouseWriterConfig,
}

pub struct WriterState<A: Send> {
	buffer: Vec<A>,
}

struct LocalWriterState<A: Send> {
	items: Vec<A>,
	state: Arc<Mutex<WriterState<A>>>,
}

impl<A: Send> From<Arc<Mutex<WriterState<A>>>> for LocalWriterState<A> {
	fn from(state: Arc<Mutex<WriterState<A>>>) -> Self {
		let mut items = vec![];
		std::mem::swap(&mut state.lock().unwrap().buffer, &mut items);
		Self { items, state }
	}
}

impl<A: Send> Drop for LocalWriterState<A> {
	fn drop(&mut self) {
		std::mem::swap(&mut self.state.lock().unwrap().buffer, &mut self.items);
	}
}

impl Writer {
	pub fn new(cfg: ClickhouseWriterConfig) -> Self {
		Self { cfg }
	}

	pub async fn go_with_retry<A: Row + Serialize + Debug + Send + Sync + 'static>(
		&self,
		client: Client,
		rx: Receiver<A>,
	) -> Result<()> {
		let state = Arc::new(Mutex::new(WriterState { buffer: Vec::new() }));
		retry(ExponentialBackoff { max_elapsed_time: None, ..ExponentialBackoff::default() }, || async {
			let cfg = self.cfg.clone();
			let client = client.clone();
			let rx = rx.clone();
			let state = state.clone();
			TracingTask::new(span!(), async move {
				let mut inserter = client
					.inserter(cfg.table_name.as_str())?
					.with_max_entries(cfg.buffer_capacity as u64)
					.with_max_duration(*cfg.force_write_duration);

				let mut last_write = Instant::now();

				let mut state = LocalWriterState::from(state);
				for el in &state.items {
					inserter.write(el).await?;
				}

				loop {
					let t = Instant::now();

					let s = inserter.commit().await.context("error during inserter.commit")?;

					if s.entries > 0 {
						let since_last = last_write.elapsed();
						let write_took = t.elapsed();
						info!(
							"Clickhouse write to {}/{}({} rows, {} transactions, {}ms since last) finished for {}ms.",
							&cfg.table_name,
							&cfg.label,
							s.entries,
							s.transactions,
							since_last.as_millis(),
							write_took.as_millis()
						);
						state.items.clear();
						last_write = Instant::now();
					}

					if let Ok(r) = timeout(*cfg.check_for_force_write_duration, rx.recv_async()).await {
						if let Ok(el) = r {
							let res = inserter.write(&el).await.context("error during inserter.write");
							state.items.push(el);
							res?;
						} else {
							break
						}
					}
				}

				inserter.end().await?;

				Ok(())
			})
			.instrument()
			.await
			.map_err(backoff::Error::Transient)?;
			Ok(())
		})
		.await
	}
}
