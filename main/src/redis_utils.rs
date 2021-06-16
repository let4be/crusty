use std::fmt::Debug;

use redis::Client;

#[allow(unused_imports)]
use crate::prelude::*;
use crate::types::*;

pub struct RedisDriver<T: 'static + Send + Sync + Debug, R: 'static + Send + Sync + Clone + Debug> {
	table_name: String,
	label:      String,

	host:      String,
	rx:        Receiver<T>,
	tx_notify: Option<Sender<chu::Notification<R>>>,
}

use crate::clickhouse_utils as chu;

pub type Thresholds = relabuf::RelaBufConfig;

pub trait RedisOperator<T: 'static + Send + Sync + Debug, R: 'static + Send + Sync + Clone + Debug> {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, items: &[T]);
	fn filter(&mut self, items: Vec<T>, response: String) -> Vec<R>;
}

impl<T: 'static + Send + Sync + Debug, R: 'static + Send + Sync + Clone + Debug> RedisDriver<T, R> {
	pub fn new(host: &str, rx: Receiver<T>, table_name: &str, label: &str) -> Self {
		Self {
			host: String::from(host),
			rx,
			table_name: String::from(table_name),
			label: String::from(label),
			tx_notify: None,
		}
	}

	pub fn with_notifier(&mut self, tx_notify: Sender<chu::Notification<R>>) {
		self.tx_notify = Some(tx_notify)
	}

	pub async fn go(
		self,
		thresholds: Thresholds,
		mut operator: Box<dyn RedisOperator<T, R> + Send + Sync>,
	) -> Result<()> {
		let client = Client::open(self.host.as_str())?;
		let mut con = client.get_async_connection().await?;

		let rx = self.rx.clone();
		let buffer = relabuf::RelaBuf::<T>::new(thresholds, move || {
			let rx = rx.clone();
			Box::pin(async move { rx.recv_async().await.context("cannot read") })
		});

		let mut last_query = Instant::now();

		while let Ok(released) = buffer.next().await {
			let mut pipe = redis::pipe();
			let atomic_pipe = pipe.atomic();

			operator.apply(atomic_pipe, &released.items);

			let since_last_elapsed = last_query.elapsed();
			last_query = Instant::now();

			let t = Instant::now();
			let r = atomic_pipe.query_async::<_, Vec<String>>(&mut con).await;
			let query_took = t.elapsed();

			match r {
				Err(err) => {
					warn!("Error during redis operation: {:?} - returning back to buffer", err);
					released.return_on_err();
				}
				Ok(r) => {
					released.confirm();
					let out_items =
						operator.filter(released.items, r.into_iter().next().unwrap_or_else(|| String::from("")));
					if let Some(tx_notify) = &self.tx_notify {
						let _ = tx_notify
							.send_async(chu::Notification {
								table_name: self.table_name.clone(),
								label:      self.label.clone(),
								since_last: since_last_elapsed,
								duration:   query_took,
								items:      out_items,
							})
							.await;
					}
				}
			}
		}

		Ok(())
	}
}
