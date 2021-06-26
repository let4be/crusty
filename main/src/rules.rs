use crusty_core::types as ct;

#[cfg(feature = "html5ever")]
pub use crate::html5ever_defs::*;
#[cfg(feature = "lol_html_parser")]
pub use crate::lolhtml_defs::*;
#[allow(unused_imports)]
use crate::{_prelude::*, config, lolhtml_parser::*, types::*};

#[derive(Debug, Clone)]
pub struct JobState {
	pub selected_domain: Domain,

	linked_from_sld:    String,
	linked_domains_set: HashSet<String>,
}

impl JobState {
	pub fn new(domain: &Domain) -> Self {
		Self {
			selected_domain:    domain.clone(),
			linked_from_sld:    Self::domain_to_second_level(&domain.domain),
			linked_domains_set: HashSet::new(),
		}
	}

	fn domain_to_second_level(domain: &str) -> String {
		domain.split('.').rev().take(2).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join(".")
	}

	pub fn link_domain(&mut self, domain: &str) {
		let sld = Self::domain_to_second_level(domain);
		if sld == self.linked_from_sld {
			return
		}

		self.linked_domains_set.insert(sld);
	}

	pub fn linked_domains(&self) -> (String, Vec<String>) {
		(self.linked_from_sld.clone(), self.linked_domains_set.iter().cloned().collect())
	}
}

#[derive(Debug, Default, Clone)]
pub struct TaskState {}

pub struct CrawlingRules {}

impl ct::JobRules<JobState, TaskState, Document> for CrawlingRules {
	fn task_filters(&self) -> ct::TaskFilters<JobState, TaskState> {
		let rules = &config::config().rules;

		let dedup_checking = crusty_core::task_filters::HashSetDedup::new(true);
		let dedup_committing = dedup_checking.committing();

		let mut filters: ct::TaskFilters<JobState, TaskState> =
			vec![Box::new(dedup_checking), Box::new(crusty_core::task_filters::SameDomain::new(true))];
		if rules.skip_no_follow_links {
			filters.push(Box::new(crusty_core::task_filters::SkipNoFollowLinks::new()));
		}
		filters.push(Box::new(crusty_core::task_filters::TotalPageBudget::new(rules.total_link_budget)));
		filters.push(Box::new(crusty_core::task_filters::LinkPerPageBudget::new(rules.links_per_task_budget)));
		filters.push(Box::new(crusty_core::task_filters::PageLevel::new(rules.max_level)));

		if rules.robots_txt {
			filters.push(Box::new(crusty_core::task_filters::RobotsTxt::new()));
		}
		filters.push(Box::new(dedup_committing));

		filters
	}

	fn status_filters(&self) -> ct::StatusFilters<JobState, TaskState> {
		let rules = &config::config().rules;

		vec![
			Box::new(crusty_core::status_filters::Redirect::new(rules.max_redirect)),
			Box::new(crusty_core::status_filters::ContentType::new(vec!["text/html", "text/plain"])),
		]
	}

	fn load_filters(&self) -> ct::LoadFilters<JobState, TaskState> {
		vec![
			Box::new(crusty_core::load_filters::RobotsTxt::new()),
			Box::new(crusty_core::load_filters::ContentType::new(vec!["text/html"])),
		]
	}

	fn task_expanders(&self) -> ct::TaskExpanders<JobState, TaskState, Document> {
		vec![Box::new(LinkExtractor {})]
	}

	fn document_parser(&self) -> Arc<DocumentParser> {
		document_parser()
	}
}
