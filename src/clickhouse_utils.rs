#[allow(unused_imports)]
use crate::prelude::*;
use crate::{
    types::*,
    config::*
};

use backoff::future::retry;
use backoff::ExponentialBackoff;
use clickhouse::{Client, Reflection};
use serde::Serialize;

#[derive(Clone)]
pub struct Writer {
    cfg: ClickhouseWriterConfig
}

pub struct WriterState<A: Clone + Send> {
    notify: Vec<A>,
}

#[derive(Debug, Clone)]
pub struct Notification<A: Clone + Send> {
    pub table_name: String,
    pub label: String,
    pub since_last: Duration,
    pub duration: Duration,
    pub items: Vec<A>,
}

#[derive(Debug, Clone)]
pub struct GenericNotification {
    pub table_name: String,
    pub label: String,
    pub since_last: Duration,
    pub duration: Duration,
    pub items: usize,
}

impl<A: Clone + Send> From<Notification<A>> for GenericNotification {
    fn from(s: Notification<A>) -> Self {
        GenericNotification{
            table_name: s.table_name,
            label: s.label,
            since_last: s.since_last,
            duration: s.duration,
            items: s.items.len(),
        }
    }
}

impl Writer {
    pub fn new(cfg: ClickhouseWriterConfig) -> Self {
        Self {
            cfg
        }
    }

    pub async fn go_with_retry<A: Clone + std::fmt::Debug + Send, R: Reflection + Serialize, F: Fn(A) -> R>(
        &self,
        client: Client,
        rx: Receiver<Vec<A>>,
        notify_tx: Option<Sender<Notification<A>>>,
        map: F,
    ) -> Result<()> {
        let state = Arc::new(Mutex::new(WriterState { notify: Vec::new() }));
        retry(ExponentialBackoff::default(), || {
            let client = client.clone();
            let rx = rx.clone();
            let notify_tx = notify_tx.clone();
            async {
                self.go(client, rx, notify_tx, &map, state.clone())
                    .await
                    .map_err(backoff::Error::Transient)?;
                Ok(())
            }
        })
        .await
    }

    pub async fn go<A: Clone + std::fmt::Debug + Send, R: Reflection + Serialize, F: Fn(A) -> R>(
        &self,
        client: Client,
        rx: Receiver<Vec<A>>,
        notify_tx: Option<Sender<Notification<A>>>,
        map: &F,
        state: Arc<Mutex<WriterState<A>>>,
    ) -> Result<()> {
        let mut inserter = client
            .inserter(self.cfg.table_name.as_str())?
            .with_max_entries(self.cfg.buffer_capacity as u64)
            .with_max_duration(*self.cfg.force_write_duration);

        let mut last_write = Instant::now();

        let notify= {state.lock().unwrap().notify.clone()};
        for el in notify.into_iter() {
            inserter.write(&map(el)).await?;
        }

        let mut done = false;
        while !done {
            if let Ok(r) = timeout(*self.cfg.check_for_force_write_duration, rx.recv_async()).await {
                for el in r.unwrap_or_else(|_| {done = true; vec![]}) {
                    state.lock().unwrap().notify.push(el.clone());
                    inserter.write(&map(el)).await.context("error during inserter.write")?;
                }
            }

            let t = Instant::now();

            let s = inserter.commit().await.context("error during inserter.commit");

            let since_last = last_write.elapsed();
            let write_took = t.elapsed();

            let s = s?;

            if s.entries > 0 {
                info!("Clickhouse write to {}/{}({} rows, {} transactions, {}ms since last) finished for {}ms.", &self.cfg.table_name, &self.cfg.label, s.entries, s.transactions, since_last.as_millis(), write_took.as_millis());
                if let Some(notify_tx) = &notify_tx {
                    let n = Notification {
                        label: self.cfg.label.clone(),
                        table_name: self.cfg.table_name.clone(),
                        since_last,
                        duration: write_took,
                        items: {state.lock().unwrap().notify.clone()},
                    };
                    let _ = notify_tx.send_async(n).await;
                }
                state.lock().unwrap().notify.clear();
                last_write = Instant::now();
            }
        }

        // if we're stopping, we don't care much about notifications or logs(for now...)
        inserter.end().await?;

        Ok(())
    }
}
