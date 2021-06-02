FROM rust:1.52.1 as planner
WORKDIR app
RUN cargo install cargo-chef --version 0.1.20
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM rust:1.52.1 as cacher
WORKDIR app
RUN cargo install cargo-chef
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

FROM rust:1.52.1 as builder
WORKDIR app
COPY . .
COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN cargo build --release

FROM rust:1.52.1 as runtime
WORKDIR app
COPY --from=builder /app/main/config.yaml .
COPY --from=builder /app/target/release/crusty /usr/local/bin
ENTRYPOINT ["/usr/local/bin/crusty"]
