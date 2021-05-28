pub use anyhow::{anyhow, Context as _};
pub use tracing::{trace, debug, info, warn, error, Level};
pub use tracing_tools::{span, TracingTask, PinnedFut};
pub use crusty_core::types::flume::{Sender, Receiver, RecvError, bounded as bounded_ch, unbounded as unbounded_ch};
pub use tokio::time::{self, Instant, Duration};
pub use url::Url;

pub use std::{
    rc::Rc,
    sync::{Arc, Mutex},
    collections::{HashMap, HashSet, LinkedList},
    iter::Iterator,
    future::Future,
    pin::Pin
};

use chrono::{Datelike, Local, TimeZone, Timelike, Utc};

pub fn now() -> u32 {
    let dt = Local::now();
    let u = Utc.ymd(dt.year(), dt.month(), dt.day()).and_hms_nano(
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.nanosecond(),
    );
    u.timestamp() as u32
}