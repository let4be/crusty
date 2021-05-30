pub use std::{
	collections::{HashMap, HashSet, LinkedList},
	future::Future,
	iter::Iterator,
	pin::Pin,
	rc::Rc,
	sync::{Arc, Mutex},
};

pub use anyhow::{anyhow, Context as _};
use chrono::{Datelike, Local, TimeZone, Timelike, Utc};
pub use crusty_core::flume::{bounded as bounded_ch, unbounded as unbounded_ch, Receiver, RecvError, Sender};
pub use tokio::time::{self, timeout, Duration, Instant};
pub use tracing::{debug, error, info, trace, warn, Level};
pub use tracing_tools::{span, PinnedFut, TracingTask};
pub use url::Url;

pub fn now() -> u32 {
	let dt = Local::now();
	let u = Utc.ymd(dt.year(), dt.month(), dt.day()).and_hms_nano(dt.hour(), dt.minute(), dt.second(), dt.nanosecond());
	u.timestamp() as u32
}
