# syntax=docker/dockerfile:1
FROM rust:1.91-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked \
 && mkdir -p /out/data \
 && cp target/release/social /out/social

FROM scratch
COPY --from=build /out/social /social
COPY --from=build --chown=10001:10001 /out/data /data
ENV PORT=8080 DATA_DIR=/data
USER 10001:10001
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/social"]
