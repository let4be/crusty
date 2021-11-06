ARG RUST_VERSION=1.56.0
ARG CARGO_CHEF_VERSION=0.1.31

FROM rust:${RUST_VERSION} as planner
ARG CARGO_CHEF_VERSION

WORKDIR app
RUN cargo install cargo-chef --version ${CARGO_CHEF_VERSION}
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM rust:${RUST_VERSION} as cacher
WORKDIR app
RUN cargo install cargo-chef
COPY --from=planner /app/recipe.json recipe.json
RUN apt-get update && apt-get -y install clang
RUN cargo chef cook --release --recipe-path recipe.json

FROM rust:${RUST_VERSION} as builder
WORKDIR app
COPY . .
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN cargo build --release

FROM rust:${RUST_VERSION} as runtime
WORKDIR app
RUN mkdir conf
COPY --from=builder /app/workspace/main/conf/* ./conf/
COPY --from=builder /app/target/release/libredis_queue.so /usr/local/lib
COPY --from=builder /app/target/release/libredis_calc.so /usr/local/lib
COPY --from=builder /app/target/release/crusty /usr/local/bin
ENV RUST_BACKTRACE=full
ENTRYPOINT ["/usr/local/bin/crusty"]
