use crate::types::*;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use redis_module::RedisString;
use validator::Validate;

#[derive(Debug, Validate)]
pub(crate) struct TopKAdd {
    pub name: String,
    pub def_topk: u32,
    pub def_width: u32,
    pub def_depth: u32,
    pub def_decay: f64,
    pub items: Vec<(String, u32)>,
}

#[derive(Debug, Validate)]
pub(crate) struct TopKConsume {
    pub name: String,
    #[validate(range(min = 15))]
    pub interval: u32,
}

impl TopKAdd {
    pub(crate) fn parse(args: Vec<RedisString>) -> Result<Self> {
        enum State {
            Looking,
            DefTopK,
            DefWidth,
            DefDepth,
            DefDecay,
            Name,
            Items,
        }

        let mut state = State::Looking;

        let mut def_topk = None;
        let mut def_width = None;
        let mut def_depth = None;
        let mut def_decay = None;
        let mut name = None;
        let mut items = vec![];

        for arg in args.into_iter().skip(1).map(|s| s.to_string()) {
            match state {
                State::Looking => match arg.to_lowercase().as_str() {
                    "def_topk" => {
                        state = State::DefTopK;
                    }
                    "def_width" => {
                        state = State::DefWidth;
                    }
                    "def_depth" => {
                        state = State::DefDepth;
                    }
                    "def_decay" => {
                        state = State::DefDecay;
                    }
                    "name" => {
                        state = State::Name;
                    }
                    "items" => {
                        state = State::Items;
                    }
                    _ => return Err(anyhow!("invalid args, unexpected {}", arg).into()),
                },
                State::DefTopK => {
                    def_topk = Some(
                        arg.parse()
                            .context("cannot parse def_topk - number expected")?,
                    );
                    state = State::Looking;
                }
                State::DefWidth => {
                    def_width = Some(
                        arg.parse()
                            .context("cannot parse def_width - number expected")?,
                    );
                    state = State::Looking;
                }
                State::DefDepth => {
                    def_depth = Some(
                        arg.parse()
                            .context("cannot parse def_depth - number expected")?,
                    );
                    state = State::Looking;
                }
                State::DefDecay => {
                    def_decay = Some(
                        arg.parse()
                            .context("cannot parse def_decay - number expected")?,
                    );
                    state = State::Looking;
                }
                State::Name => {
                    name = Some(arg);
                    state = State::Looking;
                }
                State::Items => items.push(arg),
            }
        }

        let r = Self {
            def_topk: def_topk.ok_or_else(|| anyhow!("def_topk not found"))?,
            def_width: def_width.ok_or_else(|| anyhow!("def_width not found"))?,
            def_depth: def_depth.ok_or_else(|| anyhow!("def_depth not found"))?,
            def_decay: def_decay.ok_or_else(|| anyhow!("def_decay not found"))?,
            name: name.ok_or_else(|| anyhow!("Name not found"))?,
            items: items
                .into_iter()
                .map(|v| {
                    let (name, incr) = v.split(':').next_tuple().unwrap();
                    (String::from(name), incr.parse().unwrap_or(1))
                })
                .collect(),
        };

        r.validate().context("validation: invalid arguments")?;

        Ok(r)
    }
}

impl TopKConsume {
    pub(crate) fn parse(args: Vec<RedisString>) -> Result<Self> {
        enum State {
            Looking,
            Name,
            ConsumeInterval,
        }

        let mut state = State::Looking;

        let mut name = None;
        let mut interval = None;

        for arg in args.into_iter().skip(1).map(|s| s.to_string()) {
            match state {
                State::Looking => match arg.to_lowercase().as_str() {
                    "name" => {
                        state = State::Name;
                    }
                    "interval" => {
                        state = State::ConsumeInterval;
                    }
                    _ => return Err(anyhow!("invalid args, unexpected {}", arg).into()),
                },
                State::Name => {
                    name = Some(arg);
                    state = State::Looking;
                }
                State::ConsumeInterval => {
                    interval =
                        Some(arg.parse().context(
                            "cannot parse consume_interval - number of seconds expected",
                        )?);
                    state = State::Looking;
                }
            }
        }

        let r = Self {
            name: name.ok_or_else(|| anyhow!("Name not found"))?,
            interval: interval.ok_or_else(|| anyhow!("ConsumeInterval not found"))?,
        };

        r.validate().context("validation: invalid arguments")?;

        Ok(r)
    }
}
