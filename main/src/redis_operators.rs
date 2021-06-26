use crate::{
	_prelude::*,
	config,
	redis_utils::{RedisFilterResult, RedisOperator},
	types::*,
};

pub struct Enqueue {
	pub shard: usize,
	pub cfg:   config::JobsEnqueueOptions,
}

pub struct Dequeue {
	pub shard: usize,
	pub cfg:   config::JobsDequeueOptions,
}

pub struct Finish {
	pub shard: usize,
	pub cfg:   config::JobsFinishOptions,
}

pub struct DomainTopKWriter {
	pub options: config::TopKOptions,
}

pub struct DomainTopKSyncer {
	pub options: config::TopKOptions,
}

impl RedisOperator<Domain, Domain, Vec<String>> for Enqueue {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, domains: &[Domain]) {
		pipeline
			.cmd("crusty.queue.enqueue")
			.arg("N")
			.arg(self.shard)
			.arg("TTL")
			.arg(self.cfg.ttl.as_secs())
			.arg("Domains");
		for domain in domains {
			pipeline.arg(serde_json::to_string(&domain.to_interop()).unwrap());
		}
	}

	fn filter(&mut self, domains: Vec<Domain>, _: Vec<String>) -> RedisFilterResult<Domain, Domain> {
		Ok(domains)
	}
}

impl RedisOperator<(), Domain, Vec<interop::Domain>> for Dequeue {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, _: &[()]) {
		pipeline
			.cmd("crusty.queue.dequeue")
			.arg("N")
			.arg(self.shard)
			.arg("TTL")
			.arg(self.cfg.ttl.as_secs())
			.arg("Limit")
			.arg(self.cfg.limit);
	}

	fn filter(&mut self, _: Vec<()>, domains: Vec<interop::Domain>) -> RedisFilterResult<(), Domain> {
		Ok(domains.into_iter().map(Domain::from).collect())
	}
}

impl RedisOperator<Domain, Domain, Vec<String>> for Finish {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, domains: &[Domain]) {
		pipeline
			.cmd("crusty.queue.finish")
			.arg("N")
			.arg(self.shard)
			.arg("TTL")
			.arg(self.cfg.ttl.as_secs())
			.arg("BF_Capacity")
			.arg(self.cfg.bf_initial_capacity)
			.arg("BF_Error_Rate")
			.arg(self.cfg.bf_error_rate)
			.arg("BF_EXPANSION")
			.arg(self.cfg.bf_expansion_factor)
			.arg("Domains");
		for domain in domains {
			pipeline.arg(serde_json::to_string(&domain.to_interop_descriptor()).unwrap());
		}
	}

	fn filter(&mut self, domains: Vec<Domain>, _: Vec<String>) -> RedisFilterResult<Domain, Domain> {
		Ok(domains)
	}
}

impl RedisOperator<DomainLinks, DomainLinks, ()> for DomainTopKWriter {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, domains: &[DomainLinks]) {
		let mut domains_cnt = HashMap::new();
		for domain in domains {
			*domains_cnt.entry(&domain.name).or_insert(0_u32) += 1;
			for linked_domain in &domain.linked_domains {
				*domains_cnt.entry(linked_domain).or_insert(0) += 1;
			}
		}

		pipeline
			.cmd("crusty.calc.topk.add")
			.arg("def_topk")
			.arg(self.options.k)
			.arg("def_width")
			.arg(self.options.width)
			.arg("def_depth")
			.arg(self.options.depth)
			.arg("def_decay")
			.arg(self.options.decay)
			.arg("name")
			.arg(&self.options.name)
			.arg("items");

		for (domain, cnt) in domains_cnt {
			pipeline.arg(format!("{}:{}", domain, cnt));
		}
	}

	fn filter(&mut self, domains: Vec<DomainLinks>, _: ()) -> RedisFilterResult<DomainLinks, DomainLinks> {
		Ok(domains)
	}
}

impl RedisOperator<(), interop::TopHit, Vec<interop::TopHit>> for DomainTopKSyncer {
	fn apply(&mut self, pipeline: &mut redis::Pipeline, _permit: &[()]) {
		pipeline
			.cmd("crusty.calc.topk.consume")
			.arg("name")
			.arg(&self.options.name)
			.arg("interval")
			.arg(self.options.consume_interval.as_secs());
	}

	fn filter(&mut self, _: Vec<()>, hits: Vec<interop::TopHit>) -> RedisFilterResult<(), interop::TopHit> {
		Ok(hits)
	}
}
