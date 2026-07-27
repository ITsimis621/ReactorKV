# ReactorKV
**Version: 0.6.0**

A highly concurrent, multithreaded in-memory key-value database written in modern C++17. Built from scratch to demonstrate low-level Linux system programming, this database features a Reactor/Worker network architecture, lock-striped memory partitions, and background asynchronous disk persistence.

---

## Key Architectural Features

**Storage & Persistence (The Database Engine)**
*   **Optimized Storage Engine:** The core `KVStore` is heavily optimized for fast data retrieval, utilizing custom RESP string formatting for efficient memory representation. During startup, it performs strict byte-boundary validation to protect against torn disk writes and memory corruption. 
*   **Lock Striping (Sharding):** The in-memory dataset is partitioned across 16 discrete shards, each protected by its own `std::shared_mutex`. This allows infinite concurrent readers and vastly reduces lock contention during high-throughput writes.
*   **Asynchronous AOF Persistence:** Disk durability is handled by a highly optimized background thread. It utilizes POSIX vectored I/O (`writev`) and `fdatasync` to batch and flush Append-Only File transactions to disk. The I/O pipeline is designed with zero-allocation data structures and executes outside critical sections to guarantee the main reactor event loop is never blocked.

**Network & Concurrency (The Server)**
*   **Reactor/Worker Concurrency Model:** Utilizes Linux `epoll` for non-blocking event multiplexing (Reactor) paired with a fixed-size Thread Pool (Workers) to handle thousands of concurrent TCP connections.
*   **Zero-Waste I/O Pipeline:** Incoming TCP streams are parsed using a sliding index offset to completely eliminate redundant O(N) standard-library string shifts, requiring only a single buffer cleanup per epoll wake cycle. Furthermore, `epoll` event flags are passed directly to worker threads to eliminate redundant `read()` syscalls when the OS network buffer is simply draining (`EPOLLOUT`).
*   **Robust Connection Management:** Safely handles notorious edge cases, including `EMFILE`/`ENFILE` descriptor exhaustion mitigation, 8MB maximum memory-payload defense, and lock-free O(1) eviction of idle clients using atomic timestamps.
*   **Enterprise Observability:** Emits structured JSON logs natively formatted for seamless ingestion into log aggregation platforms like the ELK stack or Datadog.

---

## Prerequisites

*   **OS:** Linux (Requires POSIX/`epoll` APIs). Can be run on Windows via WSL2.
*   **Compiler:** GCC or Clang with C++17 support.
*   **Build System:** CMake (3.15+) or Docker.
*   **Dependencies:** `nlohmann/json` (automatically fetched/installed in Docker).

---

## Building and Running

### Option 1: Using Docker (Recommended)

The project includes a multi-stage Dockerfile that compiles the binary in a builder image and runs it in a minimal, secure production environment. Using a Docker volume ensures your AOF database file persists across container restarts.

    # Build the Docker image
    docker build -t reactorkv .
    
    # Run the database in the background with a persistent named volume
    # The --timeout flag sets the idle client eviction threshold in seconds (default is 60)
    docker run -d -p 8080:8080 -v reactorkv_data:/app/data --name reactorkv-db reactorkv --timeout 5

**Monitoring & Logs:** 
You can view the structured JSON logs natively through the terminal (`docker logs -f reactorkv-db`) or by using the Docker Desktop GUI. Docker Desktop also provides an easy interface to inspect the `reactorkv_data` volume and verify the AOF file contents.

### Option 2: Local CMake Build

    # Clone the repository
    git clone https://github.com/yourusername/ReactorKV.git
    cd ReactorKV
    
    # Generate build files and compile
    mkdir build && cd build
    cmake ..
    make -j$(nproc)
    
    # Start the server
    ./reactorkv

---

## OS Tuning for High Concurrency

While the database architecture is designed to handle thousands of concurrent TCP connections, the Linux operating system caps the number of open file descriptors (sockets) a process can hold by default (usually 1024).

To push ReactorKV to its limits in a production environment, you must increase the `ulimit` before starting the server:

    # Check current limits
    ulimit -n

    # Increase the open file descriptor limit to 65,535
    ulimit -n 65535

*Note: The server includes built-in fallback mitigation (`/dev/null` descriptor rotation) to gracefully drain the OS connection queue and prevent busy-loop crashes if this limit is ever breached.*

---

## API & Usage

The server communicates exclusively via JSON over raw TCP sockets. You can test it using `netcat` (`nc`) or build a custom client.

### 1. SET a Key
**Request:**
    { "command": "SET", "key": "user:100", "value": "ilias" }

**Response:**
    { "status": "SUCCESS", "message": "Key saved." }

### 2. GET a Key
**Request:**
    { "command": "GET", "key": "user:100" }

**Response:**
    { "status": "SUCCESS", "data": "ilias" }

### 3. REMOVE a Key
**Request:**
    { "command": "REMOVE", "key": "user:100" }

**Response:**
    { "status": "SUCCESS", "message": "Key removed." }

---

## Testing

This project employs a two-tiered testing strategy:

**1. Unit Testing (White-Box)**
Utilizes Google Test (GTest) to test the `KVStore` engine in isolation, ensuring memory safety, correct AOF recovery, and thread-safe data operations.
    
    cd build
    make kvstore_tests
    ctest --output-on-failure

**2. Integration & Stress Testing (Black-Box)**
A custom Python `unittest` suite that validates the Reactor's network boundaries against a live server. It tests TCP pipelining concurrency, non-blocking drip-feed I/O, `EPOLLOUT` buffer choking, zombie connection eviction, and defense against memory-exhaustion payload breaches.

    # Ensure the database is running first, then execute:
    python3 scripts/test_integration.py -v

---

## Roadmap (Not Yet Implemented)

*   [ ] **AOF Compaction & Recovery:** Transition from the current fail-fast startup behavior to a robust background compaction engine that rewrites the AOF to prevent unbounded log growth and gracefully tolerates corrupted bytes.
*   [ ] **Client Queue Backpressure:** Implement OOM protection by pausing `epoll` socket reads when the internal worker queue exceeds a safe capacity threshold.
*   [ ] **Advanced Eviction Policies (TTL & LRU):** Introduce lazy expiration to purge volatile keys based on timestamp (TTL) and a strict memory-cap LRU eviction strategy to prevent Out-Of-Memory (OOM) crashes during sustained writes.
*   [ ] **Python Client Library with Connection Pooling:** Develop an official client wrapper that funnels traffic through a pool of persistent sockets to bypass TCP handshake overhead for high-frequency requests.
*   [ ] **Zero-Copy Network Export:** Implement an `EXPORT_AOF` command utilizing the Linux `sendfile()` syscall to stream the database log directly from the disk cache to a backup client, achieving true zero-copy network performance.
*   [ ] **Consistent Hashing:** Replace the static modulo shard router with a consistent hashing algorithm to allow dynamic, lock-free resizing of the internal memory partitions and lay the groundwork for a distributed cluster mode.
*   [ ] **Enterprise Security:** Implement connection authentication state management and OpenSSL integration for TLS encrypted payloads.