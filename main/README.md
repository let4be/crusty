![crates.io](https://img.shields.io/crates/v/crusty.svg)
[![Dependency status](https://deps.rs/repo/github/let4be/crusty/status.svg)](https://deps.rs/repo/github/let4be/crusty)

# Crusty - fast, scalable && polite broad web crawler

## Introduction
Broad web crawling is an activity of going through practically boundless web by starting from a set of locations(urls) and following outgoing links.
Usually it doesn't matter where you start from as long as it has outgoing links to external domains.

It presents unique set of challenges one must overcome to get a stable, scalable && polite system, `Crusty` is an attempt to tackle on some of those challenges to see what's out here while having fun with `Rust` ;)

This particular implementation could be used to quickly fetch a subset of all observable internet && discover most popular domains

The whole system is designed to be minimalistic in nature and does not bring any heavy artillery(like graph database or distributed file system). If needed, this can be connected externally.
Also, internally some trade-off have been made(see Tradeoffs/limitations section).

Built on top of [crusty-core](https://github.com/let4be/crusty-core) which handles all low-level aspects of web crawling

## Key features
- Configurability && extensibility

  see a typical [config file](./main/config.yaml) with some explanations regarding available options

- Blazing fast single node performance (~10 gbit/s on 48 core(96HT) `c5.metal`)

  - crusty is written in `Rust` on top of green threads running on [tokio](https://github.com/tokio-rs/tokio), so it can achieve quite impressive single-node performance while staying 100% scalable

  - we parse HTML using [LoL HTML](https://github.com/cloudflare/lol-html) - very fast speed, works in tight memory boundaries and is easy to use

  - running at  light speed you will need to think twice what to do with such amount of data ;)

- Stable performance and predictable resource consumption

  - `Crusty` has small, stable and predictable memory footprint and is usually cpu/network bound. There is no GC pressure and no war over memory.

  - built on top of buffered [Flume](https://github.com/zesterer/flume) channels - which helps to build system with predictable performance && scalability. Peak loads are getting buffered, continuous over-band loads lead to producer backoff.

- Scalability

  - each `Crusty` node is essentially an independent unit which we can run hundreds of in parallel(on different machines of course),
  the tricky part is job delegation and domain discovery which is solved by a high performance sharded queue-like structure built on top of redis.

  - we leverage redis low-latency and use carefully picked up data structures along with careful memory management to achieve our goals

  - all domains are sharded based on `addr_key`, where `addr_key` = First Lexicographically going IP && netmask, so it's possible to "compress" a given IP address and store all similar addresses under the same slot

  - this is used to avoid bombing same IP(or subnet if so desired) by concurrent requests

  - each domain belongs to some shard(`crc32(addr_key) % number_of_shards`), now each `Crusty` instance can read/update from a subset of all those shards while can insert to all of them(so-called domain discovery).
  Shards can be distributed across many redis instances if needed.

  - with careful planning and minimal changes this system should scale from a single core instance to a 100+ instances(with total of tens of thousands of cores) which can easily consume your whole DC, or several if you are persistent... ;)

 - Smart Queue implemented on top of `Redis Modules` system allows to:
    - ensure we check only one domain with same `addr_key`(no DDOS!)
    - ensure high queue throughput
    - ensure high availability(pre-sharded, if some segments become temporary unavailable system will work with others)
    - ensure high scalability(pre-sharded, move shards to other machines if there's not enough CPU or more reliability desired).
      Though the fact is a single Smart Queue can easily handle 5+ `Crusty` running on top hardware(96 cores monsters with 25gbit channels), so I would be quite curious to see a use case where you need to move out redis queue shards to dedicated machines(except for reliability)

- True politeness

  - while we can crawl tens of thousands of domains in parallel - we should absolutely limit concurrency on per-domain level
  to avoid any stress to crawled sites, see `default_crawler_settings.concurrency`.

  - each domain is first resolved and then mapped to `addr_key`, Redis Queue makes sure we -never- process several domains with the same `addr_key` thus effectively limiting concurrency on per IP/Subnet basis

  - it's a good practice to introduce delays between visiting pages, see `default_crawler_settings.delay`.

  - `robots.txt` is fully supported(using Google's implementation ported to rust)

  - global IP filtering - you can easily restrict which IP blocks or maybe even countries(if you bring a good IP->Country mapper) you wish to visit(just hack on top of existing DNS resolver when configuring `crusty-core`)

- Observability

  Crusty uses [tracing](https://github.com/tokio-rs/tracing) and stores multiple metrics in
[clickhouse](https://github.com/ClickHouse/ClickHouse)
that we can observe with [grafana](https://github.com/grafana/grafana) - giving a real-time insight in crawling performance

scales from
![example](./resources/grafana.png "this is a screenshot of an actual broad web crawling run done on i9 10900k and 100mbit fiber optic channel, crusty takes less than 1 core while saturating 100mbit channel")

up to
![example](./resources/grafana-96.png "this is a screenshot of an actual broad web crawling run done on AWS c5.metal / c5.24xlarge and 25gbit fiber optic channel, crusty takes all cores available!")

## Getting started

one liner for debian/ubuntu to be used on clean, docker-less system
```
curl -fsSL https://raw.githubusercontent.com/let4be/crusty/master/infra/lazy.sh | sudo bash -s && cd crusty
```

Now be aware this one liner will do most of the job for you and ask you some useful questions along the way to help you configure your machine, BUT it requires ROOT access.
You can do all of this manually, just study the script. I was just so bored with doing it over and over again in my tests I wrote the script...

alternatively follow instructions at

https://docs.docker.com/get-docker/

https://docs.docker.com/compose/install/

then clone this repository && configure your machine manually(study the [script](./infra/lazy.sh)!) and don't forget [/etc/sysctl.conf](infra/profile/c5.metal/sysctl.conf) && [configure](./main/config.yaml) crusty

- play with it ( !CHANGE example.com to some valid domain with outgoing links not protected by robots.txt! )

```
CRUSTY_SEEDS=https://example.com docker-compose up -d --build
```

- see `Crusty` live at http://localhost:3000/d/crusty-dashboard/crusty?orgId=1&refresh=5s

- to stop background run and _erase_ crawling data(redis/clickhouse/grafana)
  `docker-compose down -v`

additionally

- study [config file](./main/config.yaml) and adapt to your needs,
  there are sensible defaults for a 100mbit channel, if you have more/less bandwidth/cpu you might need to adjust `concurrency_profile`

- to stop background run and _retain_ crawling data
`docker-compose down`

- to run && attach and see live logs from all containers (can abort with ctrl+c)
  `CRUSTY_SEEDS=https://example.com docker-compose up`

- to see running containers `docker ps`(should be 4 - `crusty-grafana`, `crusty-clickhouse`, `crusty-redis` and `crusty` and optionally `crusty-unbound`)

- to see logs: `docker logs crusty`

---

if you decide to build manually via `cargo build`, remember - `release` build is a lot faster(and default is `debug`)

In the real world usage scenario on high bandwidth channel docker might become a bit too expensive, so it might be a good idea either to run directly or at least in `network_mode = host`

### External service dependencies

- redis - smart queue(custom redis module) && top-k(custom redis module), both are using an excellent [RedisBloom module](https://oss.redislabs.com/redisbloom/)
- clickhouse - metrics
- grafana - dashboard for metrics stored in clickhouse
- unbound(optional) - run your own caching/recursive DNS resolver server(for heavy-duty setups you most likely have to)

just use `docker-compose`, it's the recommended way to play with `Crusty`

however...

to create / clean db use [this sql](./infra/clickhouse/init.sql)(must be fed to `clickhouse client` -in context- of clickhouse docker container)

grafana dashboard is exported as [json model](./infra/grafana/dashboards/crusty.json)

## Tradeoffs / limitations

- `Crusty` not only writes `top-k` in redis but also attempts to bring aggregated data into clickhouse regularly, the whole thing implemented in a way that does not create issues with concurrency(`SET NX`, i.e. some running `Crusty` instance will pick aggregated data up) - it's perfectly fine to run multiple `Crusty` instances

- top-k functionality isn't sharded, so there might be a point where it becomes a bottleneck(setting higher buffering on Crusty and optionally discarding all low profile hits might help though)

- while Crusty is blazing fast you probably would like to do something with your 10 gbit/sec data stream, this is where the real struggle begins ;)

- being minimalistic in design `Crusty` does not bring any heavy artillery(like a recommended Graph Database, or a recommended way to aggregate / persist collected data) - so bring your own

## Development

- make sure `rustup` is installed: https://rustup.rs/

- make sure `pre-commit` is installed: https://pre-commit.com/

- run `./go setup`

- run `./go check` to run all pre-commit hooks and ensure everything is ready to go for git

- run `./go release minor` to release a next minor version for crates.io

## Contributing

I'm open to discussions/contributions, - use github issues,

pull requests are welcomed
