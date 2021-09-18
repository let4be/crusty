use crate::types::*;

use anyhow::{anyhow, Context as _};
use redis_module::RedisString;
use validator::Validate;

#[derive(Debug, Validate)]
pub(crate) struct Enqueue {
    #[validate(range(min = 0))]
    pub n: usize,
    #[validate(range(min = 1))]
    pub ttl: usize,
    #[validate(length(min = 1))]
    pub domains: Vec<Domain>,
    pub domains_str: Vec<String>,
}

#[derive(Debug, Validate)]
pub(crate) struct Dequeue {
    #[validate(range(min = 0))]
    pub n: usize,
    #[validate(range(min = 10))]
    pub ttl: usize,
    #[validate(range(min = 1))]
    pub limit: usize,
}

#[derive(Debug, Validate)]
pub(crate) struct Finish {
    #[validate(range(min = 0))]
    pub n: usize,
    #[validate(range(min = 10))]
    pub ttl: usize,
    #[validate(range(min = 1000000))]
    pub bf_capacity: usize,
    pub bf_error_rate: f64,
    #[validate(range(min = 2))]
    pub bf_expansion: usize,
    #[validate(length(min = 1))]
    pub domains: Vec<DomainDescriptor>,
}

impl Enqueue {
    pub(crate) fn parse(args: Vec<RedisString>) -> Result<Self> {
        enum State {
            Looking,
            N,
            Ttl,
            Domains,
        }

        let mut state = State::Looking;

        let mut n: Option<usize> = None;
        let mut ttl: Option<usize> = None;
        let mut domains = vec![];
        let mut domains_str = vec![];

        for arg in args.into_iter().skip(1).map(|s| s.to_string()) {
            match state {
                State::Looking => match arg.to_lowercase().as_str() {
                    "n" => {
                        state = State::N;
                    }
                    "ttl" => {
                        state = State::Ttl;
                    }
                    "domains" => {
                        state = State::Domains;
                    }
                    _ => return Err(anyhow!("invalid args, unexpected {}", arg).into()),
                },
                State::N => {
                    n = Some(arg.parse().context("cannot parse N - number expected")?);
                    state = State::Looking;
                }
                State::Ttl => {
                    ttl = Some(arg.parse().context("cannot parse TTL - number expected")?);
                    state = State::Looking;
                }
                State::Domains => {
                    domains.push(parse_domain(&arg)?);
                    domains_str.push(arg);
                }
            }
        }

        let r = Self {
            n: n.ok_or_else(|| anyhow!("N not found"))?,
            ttl: ttl.ok_or_else(|| anyhow!("TTL not found"))?,
            domains,
            domains_str,
        };

        r.validate().context("validation: invalid arguments")?;

        Ok(r)
    }
}

impl Dequeue {
    pub(crate) fn parse(args: Vec<RedisString>) -> Result<Self> {
        enum State {
            Looking,
            N,
            Ttl,
            Limit,
        }

        let mut state = State::Looking;

        let mut n: Option<usize> = None;
        let mut ttl: Option<usize> = None;
        let mut limit: Option<usize> = None;

        for arg in args.into_iter().skip(1).map(|s| s.to_string()) {
            match state {
                State::Looking => match arg.to_lowercase().as_str() {
                    "n" => {
                        state = State::N;
                    }
                    "ttl" => {
                        state = State::Ttl;
                    }
                    "limit" => {
                        state = State::Limit;
                    }
                    _ => return Err(anyhow!("invalid args, unexpected {}", arg).into()),
                },
                State::N => {
                    n = Some(arg.parse().context("cannot parse N - number expected")?);
                    state = State::Looking;
                }
                State::Ttl => {
                    ttl = Some(arg.parse().context("cannot parse TTL - number expected")?);
                    state = State::Looking;
                }
                State::Limit => {
                    limit = Some(
                        arg.parse()
                            .context("cannot parse Limit - number expected")?,
                    );
                    state = State::Looking;
                }
            }
        }

        let r = Self {
            n: n.ok_or_else(|| anyhow!("N not found"))?,
            ttl: ttl.ok_or_else(|| anyhow!("TTL not found"))?,
            limit: limit.ok_or_else(|| anyhow!("Limit not found"))?,
        };

        r.validate().context("validation: invalid arguments")?;

        Ok(r)
    }
}

impl Finish {
    pub(crate) fn parse(args: Vec<RedisString>) -> Result<Self> {
        enum State {
            Looking,
            N,
            Ttl,
            BfCapacity,
            BfErrorRate,
            BfExpansion,
            Domains,
        }

        let mut state = State::Looking;

        let mut n: Option<usize> = None;
        let mut ttl: Option<usize> = None;
        let mut bf_capacity: Option<usize> = None;
        let mut bf_error_rate: Option<f64> = None;
        let mut bf_expansion: Option<usize> = None;
        let mut domains = vec![];

        for arg in args.into_iter().skip(1).map(|s| s.to_string()) {
            match state {
                State::Looking => match arg.to_lowercase().as_str() {
                    "n" => {
                        state = State::N;
                    }
                    "ttl" => {
                        state = State::Ttl;
                    }
                    "bf_capacity" => {
                        state = State::BfCapacity;
                    }
                    "bf_error_rate" => {
                        state = State::BfErrorRate;
                    }
                    "bf_expansion" => {
                        state = State::BfExpansion;
                    }
                    "domains" => {
                        state = State::Domains;
                    }
                    _ => return Err(anyhow!("invalid args, unexpected {}", arg).into()),
                },
                State::N => {
                    n = Some(arg.parse().context("cannot parse N - number expected")?);
                    state = State::Looking;
                }
                State::Ttl => {
                    ttl = Some(arg.parse().context("cannot parse TTL - number expected")?);
                    state = State::Looking;
                }
                State::BfCapacity => {
                    bf_capacity = Some(
                        arg.parse()
                            .context("cannot parse BF_Capacity - number expected")?,
                    );
                    state = State::Looking;
                }
                State::BfErrorRate => {
                    bf_error_rate = Some(
                        arg.parse()
                            .context("cannot parse BF_Error_Rate - number expected")?,
                    );
                    state = State::Looking;
                }
                State::BfExpansion => {
                    bf_expansion = Some(
                        arg.parse()
                            .context("cannot parse BF_Expansion - number expected")?,
                    );
                    state = State::Looking;
                }
                State::Domains => domains.push(parse_domain_descriptor(&arg)?),
            }
        }

        let r = Self {
            n: n.ok_or_else(|| anyhow!("N not found"))?,
            ttl: ttl.ok_or_else(|| anyhow!("TTL not found"))?,
            bf_capacity: bf_capacity.ok_or_else(|| anyhow!("BF_Capacity not found"))?,
            bf_error_rate: bf_error_rate.ok_or_else(|| anyhow!("BF_Error_Rate not found"))?,
            bf_expansion: bf_expansion.ok_or_else(|| anyhow!("BF_Expansion not found"))?,
            domains,
        };

        r.validate().context("validation: invalid arguments")?;

        Ok(r)
    }
}
