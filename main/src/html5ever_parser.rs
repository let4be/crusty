use std::{borrow::Cow, io};

use crusty_core::{task_expanders, types as ct};
use html5ever::{
	tendril,
	tendril::*,
	tokenizer::{
		BufferQueue, StartTag, Tag, TagToken, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
		TokenizerResult,
	},
};

use crate::{_prelude::*, html5ever_defs::*, rules::*};

#[derive(Debug, Default)]
pub struct LinkData {
	href: Option<StrTendril>,
	alt:  Option<StrTendril>,
	rel:  Option<StrTendril>,
}

pub struct Doc {
	links: Vec<Tag>,
}

impl ct::ParsedDocument for Doc {}

pub struct LinkExtractor {}
impl task_expanders::Expander<JobState, TaskState, Doc> for LinkExtractor {
	fn expand(&self, ctx: &mut Ctx, task: &ct::Task, _: &ct::HttpStatus, doc: &Doc) -> task_expanders::Result {
		let links = doc.links.iter().filter_map(|link| {
			let mut href = None;
			let mut rel = None;
			let mut alt = None;

			for attr in &link.attrs {
				let name = &attr.name.local;
				if href.is_none() && name == "href" {
					href = Some(attr.value.clone());
					continue
				}
				if rel.is_none() && name == "rel" {
					rel = Some(attr.value.clone());
					continue
				}
				if alt.is_none() && name == "alt" {
					alt = Some(attr.value.clone());
					continue
				}
			}

			ct::Link::new(
				href.as_ref().map(|a| a as &str).unwrap_or(""),
				rel.as_ref().map(|a| a as &str).unwrap_or(""),
				alt.as_ref().map(|a| a as &str).unwrap_or(""),
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

#[derive(Default)]
struct TokenCollector {
	links: Vec<Tag>,
}

impl TokenCollector {}

impl TokenSink for TokenCollector {
	type Handle = ();

	#[inline(always)]
	fn process_token(&mut self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
		if let TagToken(tag) = token {
			if tag.kind == StartTag && tag.name.to_string() == "a" {
				self.links.push(tag)
			}
		}
		TokenSinkResult::Continue
	}
}

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

pub fn document_parser() -> Arc<ct::DocumentParser<Doc>> {
	Arc::new(Box::new(|reader: Box<dyn io::Read + Sync + Send>| -> ct::Result<Doc> {
		let sink = TokenCollector::default();
		let tokenizer = Tokenizer::new(sink, TokenizerOpts::default());
		let parser = Parser { tokenizer, input_buffer: BufferQueue::new() };

		let tendril = from_read(reader).context("cannot read")?;
		let parser = parser.one(tendril);

		Ok(Doc { links: parser.tokenizer.sink.links })
	}))
}
