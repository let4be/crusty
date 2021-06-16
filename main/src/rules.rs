use crusty_core::types as ct;

use crate::types::*;

#[derive(Debug, Clone)]
pub struct JobState {
	pub selected_domain: Domain,
}

#[derive(Debug, Default, Clone)]
pub struct TaskState {}

pub type Job = ct::Job<JobState, TaskState>;

pub struct CrawlingRules {}

impl ct::JobRules<JobState, TaskState> for CrawlingRules {
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
			Box::new(crusty_core::status_filters::ContentType::new(vec![
				String::from("text/html"),
				String::from("text/plain"),
			])),
			Box::new(crusty_core::status_filters::Redirect::new()),
		]
	}

	fn load_filters(&self) -> ct::LoadFilters<JobState, TaskState> {
		vec![Box::new(crusty_core::load_filters::RobotsTxt::new())]
	}

	fn task_expanders(&self) -> ct::TaskExpanders<JobState, TaskState> {
		vec![Box::new(crusty_core::task_expanders::FollowLinks::new(ct::LinkTarget::Follow))]
	}
}
