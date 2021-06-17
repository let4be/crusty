use std::{borrow::Cow, io};

use crusty_core::{task_expanders, types as ct};
use html5ever::{
	tendril,
	tendril::*,
	tokenizer::{
		BufferQueue, StartTag, TagToken, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts, TokenizerResult,
	},
};

#[allow(unused_imports)]
use crate::prelude::*;
use crate::types::*;

#[derive(Debug, Clone)]
pub struct JobState {
	pub selected_domain: Domain,
}

#[derive(Debug, Default, Clone)]
pub struct TaskState {}

pub type Job = ct::Job<JobState, TaskState, Document>;
pub type Ctx = ct::JobCtx<JobState, TaskState>;

#[derive(Debug, Default)]
pub struct LinkData {
	href: Option<StrTendril>,
	alt:  Option<StrTendril>,
	rel:  Option<StrTendril>,
}

pub struct Document {
	links: Vec<LinkData>,
}

impl ct::ParsedDocument for Document {}

pub struct LinkExtractor {}
impl task_expanders::Expander<JobState, TaskState, Document> for LinkExtractor {
	fn expand(&self, ctx: &mut Ctx, task: &ct::Task, _: &ct::HttpStatus, doc: &Document) -> task_expanders::Result {
		let mut links = vec![];
		for link in &doc.links {
			if let Ok(link) = ct::Link::new(
				link.href.as_ref().map(|a| a as &str).unwrap_or(""),
				link.rel.as_ref().map(|a| a as &str).unwrap_or(""),
				link.alt.as_ref().map(|a| a as &str).unwrap_or(""),
				"",
				0,
				ct::LinkTarget::HeadFollow,
				&task.link,
			) {
				links.push(link);
			}
		}
		ctx.push_links(links);
		Ok(())
	}
}

#[derive(Default)]
struct TokenCollector {
	links: Vec<LinkData>,
}

impl TokenCollector {}

impl TokenSink for TokenCollector {
	type Handle = ();

	fn process_token(&mut self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
		if let TagToken(tag) = token {
			if tag.kind == StartTag && tag.name.to_string() == "a" {
				let mut link = LinkData::default();
				for attr in tag.attrs {
					let name = &attr.name.local;
					if link.href.is_none() && name == "href" {
						link.href = Some(attr.value);
						continue
					}
					if link.rel.is_none() && name == "rel" {
						link.rel = Some(attr.value);
						continue
					}
					if link.alt.is_none() && name == "alt" {
						link.href = Some(attr.value);
						continue
					}
				}

				self.links.push(link)
			}
		}
		TokenSinkResult::Continue
	}
}

pub struct CrawlingRules {}

struct Parser<Sink> {
	pub tokenizer:    Tokenizer<Sink>,
	pub input_buffer: BufferQueue,
}

impl<Sink: TokenSink> TendrilSink<tendril::fmt::UTF8> for Parser<Sink> {
	type Output = Self;

	fn process(&mut self, t: StrTendril) {
		self.input_buffer.push_back(t);
		while let TokenizerResult::Script(_) = self.tokenizer.feed(&mut self.input_buffer) {}
	}

	fn error(&mut self, _desc: Cow<'static, str>) {}

	fn finish(mut self) -> Self::Output {
		while let TokenizerResult::Script(_) = self.tokenizer.feed(&mut self.input_buffer) {}
		assert!(self.input_buffer.is_empty());
		self.tokenizer.end();
		self
	}
}

fn from_read<R: io::Read>(mut readable: R) -> io::Result<StrTendril> {
	let mut byte_tendril = ByteTendril::new();
	readable.read_to_tendril(&mut byte_tendril)?;

	match byte_tendril.try_reinterpret() {
		Ok(str_tendril) => Ok(str_tendril),
		Err(_) => Err(io::Error::new(io::ErrorKind::InvalidData, "stream did not contain valid UTF-8")),
	}
}

impl ct::JobRules<JobState, TaskState, Document> for CrawlingRules {
	fn task_filters(&self) -> ct::TaskFilters<JobState, TaskState> {
		let dedup_checking = crusty_core::task_filters::HashSetDedup::new(true);
		let dedup_committing = dedup_checking.committing();
		vec![
			Box::new(crusty_core::task_filters::MaxRedirect::new(5)),
			Box::new(crusty_core::task_filters::SkipNoFollowLinks::new()),
			Box::new(crusty_core::task_filters::SameDomain::new(true)),
			Box::new(dedup_checking),
			Box::new(crusty_core::task_filters::TotalPageBudget::new(150)),
			Box::new(crusty_core::task_filters::LinkPerPageBudget::new(100)),
			Box::new(crusty_core::task_filters::PageLevel::new(25)),
			Box::new(crusty_core::task_filters::RobotsTxt::new()),
			Box::new(dedup_committing),
		]
	}

	fn status_filters(&self) -> ct::StatusFilters<JobState, TaskState> {
		vec![
			Box::new(crusty_core::status_filters::ContentType::new(vec!["text/html", "text/plain"])),
			Box::new(crusty_core::status_filters::Redirect::new()),
		]
	}

	fn load_filters(&self) -> ct::LoadFilters<JobState, TaskState> {
		vec![Box::new(crusty_core::load_filters::RobotsTxt::new())]
	}

	fn task_expanders(&self) -> ct::TaskExpanders<JobState, TaskState, Document> {
		vec![Box::new(LinkExtractor {})]
	}

	fn document_parser(&self) -> Arc<ct::DocumentParser<Document>> {
		Arc::new(Box::new(|reader: Box<dyn io::Read + Sync + Send>| -> ct::Result<Document> {
			let sink = TokenCollector::default();
			let tokenizer = Tokenizer::new(sink, TokenizerOpts::default());
			let parser = Parser { tokenizer, input_buffer: BufferQueue::new() };

			let tendril = from_read(reader).context("cannot read")?;
			let parser = parser.one(tendril);

			Ok(Document { links: parser.tokenizer.sink.links })
		}))
	}
}
