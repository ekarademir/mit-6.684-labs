FROM rust:1.47-buster as builder
WORKDIR /usr/src
RUN USER=root cargo new --bin map-reduce
WORKDIR /usr/src/map-reduce
COPY Cargo.toml .
RUN cargo build --release
RUN rm -rf src/
ADD . ./
RUN cargo build --release

FROM rust:1.47-buster as serve
WORKDIR /root
COPY --from=builder /usr/src/map-reduce/target/release/map-reduce .
ENV RUST_LOG=info
EXPOSE 3000
CMD ["./map-reduce"]
