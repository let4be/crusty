# Crusty - polite && scalable broad web crawler

## Introduction
Broad web crawling is an activity of going through practically boundless web by starting from a set of locations(urls) and following outgoing links

It presents a unique set of challenges one must overcome to get a stable and scalable system, `Crusty` is an attempt to tackle on some of those challenges to see what's out here while having fun with `Rust` ;)

This particular implementation could be used to quickly fetch a subset of all observable internet, discover most popular domains/links

Built on top of [crusty-core](https://github.com/let4be/crusty-core) which handles all low-level aspects of web crawling

## Key features 
- Configurability && extensibility
  
  see a typical [config file](config.yaml) with some explanations regarding available options
  
- Blazing fast single node performance
  
  Crusty is written in `Rust` on top of green threads running on [tokio](https://github.com/tokio-rs/tokio), so it can achieve quite impressive single-node performance even on a moderate PC
  
  Additional optimizations are possible to further improve this(mostly better html parsing, there are tasks that do not require full DOM parsing, this implementation does full DOM parsing mostly for the sake of extensibility and configurability)
  
  Additionally, `Crusty` has small and predictable memory footprint and is usually cpu/network bound. There is no GC pressure and no war over memory... A gulp of fresh air after languages with GC(hey golang) I have to say 
  
- Scalability
  
  Each `Crusty` node is essentially an independent unit which we can run hundreds of in parallel(on different machines of course), 
  the tricky part is job delegation and domain discovery which is solved by a high performance sharded queue-like structure built on top of clickhouse(huh!).
  
  One might think, "clickhouse? wtf?!" but this DB is so darn fast while providing rich querying capabilities, indexing, filtering.

  The idea is basically a huge sharded table where each domain belongs to some shard(`crc32(domain_name) % number_of_shards`), now each `Crusty` instance can read from a unique subset of all those shards while can write to all of them(so called domain discovery).
  I.e. each shard is readable by only one `Crusty` instance by a single thread(it's important, to avoid interference in multi-node setups).
  On moderate installments(~ <16 nodes) such systems is viable as this, although if someone tries to take this to a mega-scale dynamic shard manager might be required...
  
  There is additional challenge of domain discovery deduplication in multi-node setups, - right now we dedup locally and on clickhouse(AggregatingMergeTree) but the more nodes we add the less efficient local deduplication becomes

  In big setups a dedicated dedup layer might be required, alternatively one might try to simply push overflowing deduplication jobs to clickhouse by creating more shards
  
- Basic politeness
  
  While we can crawl thousands of domains in parallel - we should absolutely limit concurrency on per-domain level
  to avoid any stress to crawled sites, see `job_reader.default_crawler_settings.concurrency`.
  It's also a good practice to introduce delays between visiting pages, see `job_reader.default_crawler_settings.delay`.
  
  Additionally, there are massive sites with millions of sub-domains(usually blogs) such as tumblr.com.
  Special care should be taken when crawling them, as such we implement a sub-domain concurrency limitation as well, see `job_reader.domain_tail_top_n` setting which defaults to 3 - no more than 3 sub-domains can be crawled concurrently
  
  Currently, there's no `robots.txt` support but this can be added easily(and will be)
  
- Observability
  
  Crusty uses [tracing](https://github.com/tokio-rs/tracing) and stores multiple metrics in 
[clickhouse](https://github.com/ClickHouse/ClickHouse) 
that we can observe with [grafana](https://github.com/grafana/grafana) - giving a real-time insight in crawling performance

![example](./resources/grafana.png)

## Getting started

- before you start

install `docker` && `docker-compose`, follow instructions at
```
https://docs.docker.com/get-docker/
https://docs.docker.com/compose/install/
```

- get `Crusty` source code

```
git clone https://github.com/let4be/crusty
cd crusty
```

- build and run as easy as

```
docker-compose build
docker-compose up
```

see `Crusty` live at `http://localhost:3000/d/crusty-dashboard/crusty?orgId=1&refresh=5s`

---

if you decide to build manually via `cargo build`, remember - release build is a lot faster(and default is debug) ;

- external service dependencies - clickhouse and grafana

just use `docker-compose`, it's the recommended way to play with `Crusty`

however...

to create / clean db use [this script](infra/clickhouse/clean-clickhouse.sh)(must be executed -in context- of clickhouse docker container)

grafana dashboard is exported as [json model](./infra/grafana/dashboards/crusty.json)

## Contributing

I'm open to discussions/contributions, use github issues, pull requests are welcomed ;)
