use std::{fmt::Debug, marker::PhantomData};

use redis::Client;

#[allow(unused_imports)]
use crate::{_prelude::*, types::*};

pub struct RedisDriver<
	T: 'static + Send + Sync + Debug,
	R: 'static + Send + Sync + Debug + Clone,
	N: From<DBNotification<R>>,
> {
	table_name: String,
	label:      String,

	host:      String,
	rx:        Receiver<T>,
	tx_notify: Sender<N>,
	_r:        PhantomData<R>,
}

pub type Thresholds = relabuf::RelaBufConfig;

pub trait RedisOperator<T: 'static + Send + Sync + Debug, R: 'static + Send + Sync + Debug + Clone> {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, items: &[T]);
	fn filter(&mut self, items: Vec<T>, response: String) -> Vec<R>;
}

impl<T: 'static + Send + Sync + Debug, R: 'static + Send + Sync + Debug + Clone, N: From<DBNotification<R>>>
	RedisDriver<T, R, N>
{
	pub fn new(host: &str, rx: Receiver<T>, table_name: &str, label: &str, tx_notify: Sender<N>) -> Self {
		Self {
			host: String::from(host),
			rx,
			table_name: String::from(table_name),
			label: String::from(label),
			tx_notify,
			_r: PhantomData::default(),
		}
	}

	pub async fn go(
		self,
		thresholds: Thresholds,
		mut operator: Box<dyn RedisOperator<T, R> + Send + Sync>,
	) -> Result<()> {
		let client = Client::open(self.host.as_str())?;

		let rx = self.rx.clone();
		let buffer = relabuf::RelaBuf::<T>::new(thresholds, move || {
			let rx = rx.clone();
			Box::pin(async move { rx.recv_async().await.context("cannot read") })
		});

		let mut last_query = Instant::now();

		let mut con = None;
		while let Ok(released) = buffer.next().await {
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
					released.confirm();
					let out_items =
						operator.filter(released.items, r.into_iter().next().unwrap_or_else(|| String::from("")));
					let _ = self
						.tx_notify
						.send_async(
							DBNotification {
								table_name: self.table_name.clone(),
								label:      self.label.clone(),
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
