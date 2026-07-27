#pragma once

/**
 * @file KVStore.h
 * @brief Thread-safe, in-memory key-value store with AOF disk persistence.
 * 
 * Utilizes lock striping (sharding) to minimize thread contention during 
 * high-throughput concurrent access. 
 */

#include "AOFManager.h"
#include <string>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <array>

/// The number of discrete memory partitions for lock striping
constexpr size_t NUM_SHARDS = 16;

/**
 * @struct Shard
 * @brief An isolated segment of the database.
 * 
 * Encapsulates a subset of the dataset and a read-write lock, allowing 
 * multiple readers or a single writer to operate on this specific shard 
 * independently of the others.
 */
struct Shard {
    std::unordered_map<std::string, std::string> data;
    std::shared_mutex mutex;
};

/**
 * @class KVStore
 * @brief Thread-safe, sharded key-value engine.
 */
class KVStore {
private:
    std::array<Shard, NUM_SHARDS> shards;   ///< Partitioned hash maps to reduce lock contention
    AOFManager aof;                         ///< Background worker for asynchronous persistence

    /**
     * @brief Mathematical router to find the correct shard for a given key.
     * @param key The key to hash.
     * @return The index of the target shard (0 to NUM_SHARDS - 1).
     */
    size_t get_shard_index(const std::string& key) const;

    /**
     * @brief Reads the append-only log file to reconstruct state.
     * 
     * Invoked automatically during database initialization. Parses length-prefixed 
     * RESP strings and detects corrupted or torn writes.
     */
    void load_from_disk();

public:
    /**
     * @brief Initializes the database and replays historical AOF data into memory.
     */
    KVStore();
    
    /**
     * @brief Default destructor. Relies on AOFManager's safe shutdown mechanics.
     */
    ~KVStore();

    /**
     * @brief Inserts or updates a key-value pair.
     * 
     * Acquires an exclusive lock on the target shard and queues the operation 
     * to the AOF manager for persistence.
     * 
     * @param key The record key.
     * @param value The data payload.
     */
    void set(const std::string& key, const std::string& value);

    /**
     * @brief Retrieves a value associated with a key.
     * 
     * Acquires a shared lock on the target shard, allowing infinite concurrent reads.
     * 
     * @param key The record key.
     * @return The associated value, or an error string if not found.
     */
    std::string get(const std::string& key);

    /**
     * @brief Deletes a key-value pair and logs the removal to disk.
     * 
     * Acquires an exclusive lock on the target shard.
     * 
     * @param key The record key.
     */
    void remove(const std::string& key);
};