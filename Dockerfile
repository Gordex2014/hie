# Linux image: the engine plus the Python runtime its transforms run on.
FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# The launcher pin is compiled in.
COPY hie_runtime/limits.pin hie_runtime/
RUN cargo build --release --locked --bin hie

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /hie
COPY --from=build /src/target/release/hie /usr/local/bin/hie
COPY hie_runtime/ /hie/hie_runtime/
COPY transforms/ /hie/transforms/
COPY sources/ /hie/sources/
COPY hie.yaml /hie/hie.yaml
# Listen on every interface so a published port (-p 8090:8090) reaches the API.
RUN sed -i 's/^listen: .*/listen: 0.0.0.0:8090/' /hie/hie.yaml \
    && grep -qx 'listen: 0.0.0.0:8090' /hie/hie.yaml
ENV HIE_CONFIG=/hie/hie.yaml \
    HIE_RUNTIME_DIR=/hie/hie_runtime \
    HIE_LOG=info
EXPOSE 8090
ENTRYPOINT ["hie"]
