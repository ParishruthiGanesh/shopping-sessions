# Shopping Sessions, Team Inventors: a native (Rust) pipeline with no dependencies.
# The scorers are linux/amd64 Intel Xeon, Cascade Lake or newer. They build this
# file natively, so no FROM line pins a platform. Base images are pinned by digest.
FROM rust:1.97-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3 AS build
WORKDIR /src
COPY native/ ./
# Cargo.lock pins every dependency; --locked refuses to build if it would change.
# The binary is statically linked (native/.cargo/config.toml), so it runs on an empty image.
RUN cargo build --release --locked

FROM scratch
# The scorer runs the container as uid 1000 with HOME=/scratch; nothing here needs a passwd entry.
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
# No arguments: reads $DATA_DIR, writes $OUTPUT_DIR. Exit 0 means "done".
CMD ["/app/shopping-sessions"]
