#[macro_use]
extern crate redis_module;
use redis_module::{Context, RedisResult, RedisString};
use redis_utils::Cmd;

pub mod types;
use types::*;

mod cmd;

use std::collections::{hash_map::Entry, HashMap};

fn k_domain_in_processing(n: usize, addr_key: &str) -> String {
    format!("in-processing-{}/{}", n, addr_key)
}

fn k_domain_in_history(n: usize) -> String {
    format!("in-history-{}", n)
}

fn k_in_flight_domains_by_addr_key(n: usize, addr_key: &str) -> String {
    format!("in-flight-{}/domains_by_addr_key/{}", n, addr_key)
}

fn k_in_flight_addr_keys(n: usize) -> String {
    format!("in-flight-{}/addr_keys", n)
}

fn enqueue(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let cmd = cmd::Enqueue::parse(args)?;

    let mut already_checking = 0;
    let mut already_checked = 0;
    let mut checking_same_addr_key = 0;

    let domains_by_addr_key = cmd.domains.iter().zip(cmd.domains_str.into_iter()).fold(
        HashMap::new(),
        |mut a, (domain, domain_str)| {
            let domains: &mut Vec<_> = match a.entry(&domain.addr_key) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => v.insert(vec![]),
            };
            domains.push((domain, domain_str));
            a
        },
    );

    let mut cmd_add_addr_key = Cmd::new("SADD", k_in_flight_addr_keys(cmd.n));
    for (addr_key, domains) in domains_by_addr_key {
        let processing_domain_name = Cmd::new("GET", k_domain_in_processing(cmd.n, addr_key))
            .exec(ctx)
            .inner()?
            .str()
            .unwrap_or_else(|| "".into());

        let mut cmd_add_domain = Cmd::new("SADD", k_in_flight_domains_by_addr_key(cmd.n, addr_key));

        for (domain, domain_str) in domains {
            if processing_domain_name == domain.name {
                // we already checking this -exact- domain
                already_checking += 1;
                continue;
            }

            let is_checked_before = Cmd::new("BF.EXISTS", k_domain_in_history(cmd.n))
                .arg(&domain.name)
                .exec(ctx)
                .inner()?
                .i64()
                .unwrap_or(0);

            if is_checked_before == 1 {
                // bloom filter says we've dealt with this -exact- domain in the past
                already_checked += 1;
                continue;
            }

            cmd_add_domain = cmd_add_domain.arg(domain_str);
        }

        if cmd_add_domain.is_modified() {
            cmd_add_domain.exec(ctx).check()?;
        }

        Cmd::new("EXPIRE", k_in_flight_domains_by_addr_key(cmd.n, addr_key))
            .arg(cmd.ttl)
            .exec(ctx)
            .check()?;
        if !processing_domain_name.is_empty() {
            // some other domain with the same addr_key is being processed, do NOT make addr_key available
            checking_same_addr_key += 1;
            continue;
        }

        cmd_add_addr_key = cmd_add_addr_key.arg(addr_key);
    }

    if cmd_add_addr_key.is_modified() {
        cmd_add_addr_key.exec(ctx).check()?;
    }

    println!(
        "enqueued, already_checking: {}, already_checked: {}, checking_same_addr_key: {}",
        already_checking, already_checked, checking_same_addr_key
    );
    Ok("OK".into())
}

fn dequeue(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let cmd = cmd::Dequeue::parse(args)?;

    let available_addr_keys = Cmd::new("SPOP", k_in_flight_addr_keys(cmd.n))
        .arg(cmd.limit)
        .exec(ctx)
        .inner()?
        .iter()
        .map(|v| v.str().unwrap_or_else(|| "".into()))
        .collect::<Vec<_>>();

    let available_addr_keys_len = available_addr_keys.len();
    let mut domains = vec![];
    for addr_key in available_addr_keys {
        let domain_str = Cmd::new("SPOP", k_in_flight_domains_by_addr_key(cmd.n, &addr_key))
            .exec(ctx)
            .inner()?
            .str()
            .unwrap_or_else(|| "".into());

        if domain_str.is_empty() {
            continue;
        }
        let domain: Domain = serde_json::from_str(&domain_str)?;
        Cmd::new("SET", k_domain_in_processing(cmd.n, &addr_key))
            .arg(&domain.name)
            .arg_p("EX", cmd.ttl)
            .exec(ctx)
            .check()?;
        Cmd::new("EXPIRE", k_in_flight_domains_by_addr_key(cmd.n, &addr_key))
            .arg(cmd.ttl)
            .exec(ctx)
            .check()?;
        domains.push(domain);
    }

    println!(
        "dequeue, available_addr_keys = {}, domains = {}",
        available_addr_keys_len,
        domains.len()
    );
    Ok(serde_json::to_string(&domains).unwrap().into())
}

fn finish(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let cmd = cmd::Finish::parse(args)?;

    let mut cmd_add_inflight_addr_keys = Cmd::new("SADD", k_in_flight_addr_keys(cmd.n));
    for domain in &cmd.domains {
        Cmd::new("UNLINK", k_domain_in_processing(cmd.n, &domain.addr_key))
            .exec(ctx)
            .check()?;

        let remaining = Cmd::new(
            "SCARD",
            k_in_flight_domains_by_addr_key(cmd.n, &domain.addr_key),
        )
        .exec(ctx)
        .inner()?
        .i64()
        .unwrap_or(0);

        if remaining > 0 {
            cmd_add_inflight_addr_keys = cmd_add_inflight_addr_keys.arg(&domain.addr_key);
            Cmd::new(
                "EXPIRE",
                k_in_flight_domains_by_addr_key(cmd.n, &domain.addr_key),
            )
            .arg(cmd.ttl)
            .exec(ctx)
            .check()?;
        }
    }

    if cmd_add_inflight_addr_keys.is_modified() {
        cmd_add_inflight_addr_keys.exec(ctx).check()?;
    }

    Cmd::new("BF.INSERT", k_domain_in_history(cmd.n))
        .arg_p("CAPACITY", cmd.bf_capacity)
        .arg_p("ERROR", cmd.bf_error_rate)
        .arg_p("EXPANSION", cmd.bf_expansion)
        .arg_s("ITEMS")
        .args_s(cmd.domains.iter().map(|d| d.name.as_str()))
        .exec(ctx)
        .check()?;

    Ok("OK".into())
}

redis_module! {
    name: "crusty.queue",
    version: 1,
    data_types: [],
    commands: [
        ["crusty.queue.enqueue", enqueue, "", 0, 0, 0],
        ["crusty.queue.dequeue", dequeue, "", 0, 0, 0],
        ["crusty.queue.finish", finish, "", 0, 0, 0],
    ],
}
