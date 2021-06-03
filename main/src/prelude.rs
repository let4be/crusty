use std::time::{SystemTime, UNIX_EPOCH};
pub use std::{
	collections::{HashMap, HashSet, LinkedList},
	future::Future,
	iter::Iterator,
	pin::Pin,
	rc::Rc,
	sync::{Arc, Mutex},
};

pub use anyhow::{anyhow, Context as _};
pub use crusty_core::flume::{bounded as bounded_ch, unbounded as unbounded_ch, Receiver, RecvError, Sender};
pub use tokio::time::{self, timeout, Duration, Instant};
pub use tracing::{debug, error, info, trace, warn, Level};
pub use tracing_tools::{span, PinnedFut, TracingTask};
pub use url::Url;

pub fn now() -> u32 {
	let start = SystemTime::now();
	let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
	since_the_epoch.as_secs() as u32
}
