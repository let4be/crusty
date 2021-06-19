use crusty_core::types as ct;

use crate::rules::*;

pub type Document = crate::lolhtml_parser::Doc;
pub type DocumentParser = ct::DocumentParser<Document>;
pub type Job = ct::Job<JobState, TaskState, Document>;
pub type Ctx = ct::JobCtx<JobState, TaskState>;
