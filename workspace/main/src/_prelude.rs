pub use std::{
	cmp,
	collections::{HashMap, HashSet, LinkedList},
	fmt::Debug,
	future::Future,
	iter::Iterator,
	pin::Pin,
	rc::Rc,
	sync::{Arc, Mutex, Weak},
};

pub use anyhow::{anyhow, Context as _};
pub use crusty_core::flume::{bounded as bounded_ch, unbounded as unbounded_ch, Receiver, RecvError, Sender};
pub use itertools::Itertools;
pub use strum::*;
pub use tokio::time::{self, timeout, Duration, Instant};
pub use tracing::{debug, error, info, trace, warn, Level};
pub use tracing_tools::{span, TracingTask};
pub use url::Url;

pub fn now() -> Duration {
	use std::time::{SystemTime, UNIX_EPOCH};
	let start = SystemTime::now();
	start.duration_since(UNIX_EPOCH).expect("Time went backwards")
}
