# ------------------------------------------------------------------------------
# Stage 1: Build Environment
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    g++ \
    cmake \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY CMakeLists.txt ./
COPY include/ ./include/
COPY src/ ./src/
COPY tests/ ./tests/

# Execute out-of-source build utilizing all available CPU cores
RUN mkdir build && cd build && \
    cmake .. && \
    make -j$(nproc)

# ------------------------------------------------------------------------------
# Stage 2: Production Runtime
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim

WORKDIR /app

# Establish least-privilege user
RUN groupadd -r kvstore && useradd -r -g kvstore kvuser
RUN mkdir data && chown -R kvuser:kvstore /app/data

COPY --from=builder /app/build/reactorkv .

RUN chown kvuser:kvstore reactorkv && chmod +x reactorkv
USER kvuser

EXPOSE 8080

ENTRYPOINT ["./reactorkv"]