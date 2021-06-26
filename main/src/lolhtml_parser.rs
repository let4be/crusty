use std::io;

use crusty_core::{task_expanders, types as ct};
use lol_html::{element, HtmlRewriter, Settings};

use crate::{_prelude::*, lolhtml_defs::*, rules::*};

#[derive(Debug, Default)]
pub struct LinkData {
	href: Option<String>,
	alt:  Option<String>,
	rel:  Option<String>,
}

#[derive(Debug, Default)]
pub struct Doc {
	links: Vec<LinkData>,
}

impl ct::ParsedDocument for Doc {}

pub struct LinkExtractor {}
impl task_expanders::Expander<JobState, TaskState, Doc> for LinkExtractor {
	fn expand(&self, ctx: &mut Ctx, task: &ct::Task, _: &ct::HttpStatus, doc: &Doc) -> task_expanders::Result {
		let links = doc.links.iter().filter_map(|link| {
			ct::Link::new(
				link.href.as_ref().map(|a| a as &str).unwrap_or(""),
				link.rel.as_ref().map(|a| a as &str).unwrap_or(""),
				link.alt.as_ref().map(|a| a as &str).unwrap_or(""),
				"",
				0,
				ct::LinkTarget::HeadFollow,
				&task.link,
			)
			.ok()
		});

		ctx.push_links(links);
		Ok(())
	}
}

pub fn document_parser() -> Arc<ct::DocumentParser<Doc>> {
	Arc::new(Box::new(|mut reader: Box<dyn io::Read + Sync + Send>| -> ct::Result<Doc> {
		let mut doc = Doc::default();

		let mut rewriter = HtmlRewriter::new(
			Settings {
				element_content_handlers: vec![element!("a[href]", |el| {
					let href = el.get_attribute("href");
					let rel = el.get_attribute("rel");
					let alt = el.get_attribute("alt");
					let link_data = LinkData { href, alt, rel };
					doc.links.push(link_data);

					el.remove();
					Ok(())
				})],
				..Settings::default()
			},
			|_: &[u8]| {},
		);

		let mut buffer = [0; 1024 * 32];
		while let Ok(n) = reader.read(&mut buffer[..]) {
			if n < 1 {
				break
			}
			rewriter.write(&buffer[..n]).context("cannot write to lolhtml")?;
		}
		rewriter.end().context("cannot finish writing to lolhtml")?;

		Ok(doc)
	}))
}
