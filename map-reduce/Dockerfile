FROM rust:1.46-buster as compile
WORKDIR /usr/src/map-reduce
COPY . .
RUN cargo build --release

FROM rust:1.46-buster as serve
WORKDIR /root
COPY --from=compile /usr/src/map-reduce/target/release/map-reduce .
ENV RUST_LOG=info
EXPOSE 3000
CMD ["./map-reduce"]
