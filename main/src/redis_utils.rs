use redis::Client;
use serde::de::DeserializeOwned;

use crate::{_prelude::*, types::*};

pub trait Record: 'static + Send + Sync + Debug {}
impl<T: 'static + Send + Sync + Debug> Record for T {}

pub struct RedisDriver<T: Record> {
	table_name: &'static str,
	label:      &'static str,

	host:       String,
	rx:         Receiver<T>,
	thresholds: Thresholds,
}

pub type Thresholds = relabuf::RelaBufConfig;

pub struct RedisFilterError<T: Record> {
	pub items: Vec<T>,
	pub err:   anyhow::Error,
}

pub type RedisFilterResult<T, R> = std::result::Result<Vec<R>, RedisFilterError<T>>;

pub trait RedisOperator<T: Record, R: Record, X> {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, items: &[T]);
	fn filter(&mut self, items: Vec<T>, response: X) -> RedisFilterResult<T, R>;
}

impl<T: Record> RedisDriver<T> {
	pub fn new(
		host: &str,
		rx: Receiver<T>,
		table_name: &'static str,
		label: &'static str,
		thresholds: Thresholds,
	) -> Self {
		Self { host: String::from(host), rx, table_name, label, thresholds }
	}

	pub async fn go<X: Default + DeserializeOwned, R: Record, N: From<DBNotification<R>>>(
		self,
		mut operator: Box<dyn RedisOperator<T, R, X> + Send + Sync>,
		tx_notify: Sender<N>,
	) -> Result<()> {
		let client = Client::open(self.host.as_str())?;

		let rx = self.rx.clone();
		let buffer = relabuf::RelaBuf::<T>::new(self.thresholds, move || {
			let rx = rx.clone();
			Box::pin(async move { rx.recv_async().await.context("cannot read") })
		});

		let mut last_query = Instant::now();

		let mut con = None;
		while let Ok(mut released) = buffer.next().await {
			if con.is_none() {
				let connection = client.get_async_connection().await;
				if let Err(err) = connection {
					warn!("Cannot acquire redis connection: {:?}", err);
					released.return_on_err();
					continue
				}
				con = Some(connection.unwrap());
			}

			let mut pipe = redis::pipe();
			let atomic_pipe = pipe.atomic();

			operator.apply(atomic_pipe, &released.items);

			let since_last_elapsed = last_query.elapsed();
			last_query = Instant::now();

			let t = Instant::now();
			let r = atomic_pipe.query_async::<_, Vec<String>>(con.as_mut().unwrap()).await;
			let query_took = t.elapsed();

			match r {
				Err(err) => {
					warn!("Error during redis operation: {:?} - returning back to buffer", err);
					released.return_on_err();
					con = None;
				}
				Ok(r) => {
					let v = r
						.first()
						.map(|v| {
							if v == "OK" {
								return None
							}
							serde_json::from_str(v).ok()
						})
						.unwrap_or_else(|| Some(X::default()))
						.unwrap_or_else(X::default);

					let mut items = vec![];
					std::mem::swap(&mut items, &mut released.items);

					let out_items = match operator.filter(items, v) {
						Ok(r) => r,
						Err(err) => {
							warn!(
								"Error during redis operation during filtering: {:?} - returning back to buffer",
								err.err
							);
							released.items = err.items;
							released.return_on_err();
							continue
						}
					};
					released.confirm();

					let _ = tx_notify
						.send_async(
							DBNotification {
								table_name: self.table_name,
								label:      self.label,
								since_last: since_last_elapsed,
								duration:   query_took,
								items:      out_items,
							}
							.into(),
						)
						.await;
				}
			}
		}

		Ok(())
	}
}
