#[cfg(feature = "html5ever")]
mod html5ever_defs;
#[cfg(feature = "lol_html_parser")]
mod lolhtml_defs;

#[cfg(feature = "html5ever")]
mod html5ever_parser;
#[cfg(feature = "lol_html_parser")]
mod lolhtml_parser;

mod clickhouse_utils;
mod config;
mod crusty;
mod prelude;
mod redis_utils;
mod rules;
mod types;

use crusty::Crusty;
use tracing_subscriber::EnvFilter;

#[allow(unused_imports)]
use crate::{
	prelude::*,
	{config::CrustyConfig, rules::*, types::*},
};

fn main() -> Result<()> {
	println!("Starting Crusty...");
	println!(
		"Built {} on {} with {} rustc(profile: {}) for target {}",
		env!("VERGEN_GIT_SHA"),
		env!("VERGEN_BUILD_TIMESTAMP"),
		env!("VERGEN_RUSTC_SEMVER"),
		env!("VERGEN_CARGO_PROFILE"),
		env!("VERGEN_CARGO_TARGET_TRIPLE"),
	);

	let cr = config::load();
	match cr {
		Ok(_) => {
			println!("Loading config: ok");
		}
		Err(err) => {
			println!("Loading config err: '{:?}' - using defaults", err)
		}
	}
	let cfg = config::config();

	let mut filter = EnvFilter::from_default_env().add_directive((*cfg.log.level).into());
	if let Some(filters) = &cfg.log.filter {
		for filter_str in filters {
			filter = filter.add_directive(filter_str.parse()?);
		}
	}

	let collector =
		tracing_subscriber::fmt().with_env_filter(filter).with_target(false).with_ansi(cfg.log.ansi).finish();

	tracing::subscriber::set_global_default(collector)?;
	println!("Log system configured...: {} with filtering: {:?}", *cfg.log.level, cfg.log.filter);
	println!("{:#?}", &cfg);

	if cfg.jobs.reader.seeds.is_empty() {
		return Err(anyhow!("Consider specifying one or more seed URLs in config.toml, see job_reader.seeds property"))
	}

	let new_fd_lim = fdlimit::raise_fd_limit();
	println!("New FD limit set: {:?}", new_fd_lim);

	let (tx_crawler, rx_crawler) = std::sync::mpsc::channel();
	let (tx_crawler_done, rx_crawler_done) = unbounded_ch();

	std::thread::spawn(move || {
		let rt = tokio::runtime::Runtime::new().unwrap();
		rt.block_on(async move {
			let crusty = Crusty::new(cfg.clone());
			crusty.go(tx_crawler, rx_crawler_done).instrument().await
		})
	});

	let rt = tokio::runtime::Runtime::new().unwrap();
	rt.block_on(
		TracingTask::new(span!(), async move {
			let crawler = rx_crawler.recv()?;

			info!("Crawling is a go...");
			let _ = crawler.go().await?;
			info!("Crawling finished...");

			drop(tx_crawler_done);
			Ok::<(), anyhow::Error>(())
		})
		.instrument(),
	)
}
