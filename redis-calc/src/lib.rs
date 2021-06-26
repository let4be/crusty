#[macro_use]
extern crate redis_module;
use redis_module::{Context, RedisResult};
use redis_utils::Cmd;

mod cmd;
pub mod types;

use interop::TopHit;
use std::collections::HashMap;
use std::time::Duration;

pub fn now() -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

fn k_topk_last_move(name: &str) -> String {
    format!("/top-k/{}/last_move", name)
}

fn k_topk_tlds(name: &str) -> String {
    format!("/top-k/{}/tld", name)
}

fn k_topk(name: &str, tld: &str) -> String {
    format!("/top-k/{}-{}", tld, name)
}

fn topk_add(ctx: &Context, args: Vec<String>) -> RedisResult {
    let cmd = cmd::TopKAdd::parse(args)?;
    println!("topk_add called with {}", cmd.name);

    let mut by_tld = HashMap::new();
    for (domain, incr_by) in &cmd.items {
        if let Some(tld) = domain.split('.').last() {
            let domains = by_tld.entry(tld).or_insert_with(Vec::new);
            domains.push((domain, incr_by));
        }
        let gen_domains = by_tld.entry("").or_insert_with(Vec::new);
        gen_domains.push((domain, incr_by));
    }

    for (tld, domains) in by_tld {
        Cmd::new("SADD", k_topk_tlds(&cmd.name))
            .arg(&tld)
            .exec(ctx)
            .check()?;

        let ok_topk = Cmd::new("TOPK.INFO", k_topk(&cmd.name, tld))
            .exec(ctx)
            .check()
            .is_ok();

        if !ok_topk {
            Cmd::new("TOPK.RESERVE", k_topk(&cmd.name, tld))
                .arg(cmd.def_topk)
                .arg(cmd.def_width)
                .arg(cmd.def_depth)
                .arg(cmd.def_decay)
                .exec(ctx)
                .check()?;
        }

        let mut add_cmd = Cmd::new("TOPK.INCRBY", k_topk(&cmd.name, tld));
        for (domain, incr_by) in domains {
            add_cmd = add_cmd.arg(domain).arg(incr_by);
        }
        add_cmd.exec(ctx).check()?;
    }

    Ok("OK".into())
}

fn topk_consume(ctx: &Context, args: Vec<String>) -> RedisResult {
    let cmd = cmd::TopKConsume::parse(args)?;
    println!("topk_consume called with {}", cmd.name);

    let set = Cmd::new("SET", k_topk_last_move(&cmd.name))
        .arg("taken")
        .arg("NX")
        .arg("EX")
        .arg(cmd.interval)
        .exec(ctx)
        .inner()?
        .str()
        .unwrap_or_else(|| String::from(""));

    let hits = if set == "OK" {
        let tlds = Cmd::new("SMEMBERS", k_topk_tlds(&cmd.name))
            .exec(ctx)
            .inner()?
            .iter()
            .map(|a| a.str().unwrap())
            .collect::<Vec<_>>();

        let mut hits = vec![];
        for tld in tlds {
            let names = Cmd::new("TOPK.LIST", k_topk(&cmd.name, &tld))
                .exec(ctx)
                .inner()?
                .iter()
                .filter_map(|r| r.str())
                .collect::<Vec<_>>();

            let mut topk_count_cmd = Cmd::new("TOPK.COUNT", k_topk(&cmd.name, &tld));
            for name in &names {
                topk_count_cmd = topk_count_cmd.arg(name);
            }

            hits.extend(
                topk_count_cmd
                    .exec(ctx)
                    .inner()?
                    .iter()
                    .map(|r| r.i64().unwrap())
                    .zip(names.into_iter())
                    .map(|(hits, name)| TopHit {
                        domain: name,
                        hits,
                        tld: tld.clone(),
                    })
                    .collect::<Vec<_>>(),
            );
        }

        hits
    } else {
        vec![]
    };

    Ok(serde_json::to_string(&hits).unwrap().into())
}

redis_module! {
    name: "crusty.calc",
    version: 1,
    data_types: [],
    commands: [
        ["crusty.calc.topk.add", topk_add, "", 0, 0, 0],
        ["crusty.calc.topk.consume", topk_consume, "", 0, 0, 0],
    ],
}
