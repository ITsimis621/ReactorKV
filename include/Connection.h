#pragma once

/**
 * @file Connection.h
 * @brief Lock-free connection state management for the server's Object Pool.
 */

#include <string>
#include <atomic>
#include <chrono>
#include <unistd.h>

/**
 * @struct Connection
 * @brief Encapsulates all state for a single connected TCP client.
 * 
 * Designed to live permanently inside a statically allocated std::array.
 * Uses atomic flags for lock-free state tracking and avoids dynamic 
 * memory allocation during client connect/disconnect cycles.
 */
struct Connection {
    int fd{-1};                                ///< The client's socket file descriptor
    std::string read_buffer;                   ///< Unprocessed incoming TCP stream data
    std::string write_buffer;                  ///< Queued outbound JSON responses
    std::atomic<uint64_t> last_active{0};      ///< Epoch timestamp for idle eviction
    std::atomic<bool> is_active{false};        ///< Lock-free flag indicating if slot is in use
    std::atomic<uint64_t> generation{0};       ///< ABA prevention counter to track connection lifecycles

    /**
     * @brief Default constructor required for std::array pre-allocation.
     */
    Connection() = default;
    
    // NOTE: No RAII Destructor! The object pool owns the memory permanently.
    // Socket cleanup is handled explicitly via the reset() method.

    /**
     * @brief Wipes the connection state, preparing the slot for the next client.
     * 
     * Closes the OS socket and logically clears strings (size = 0) 
     * while retaining their underlying heap capacity for ultra-fast reuse.
     */
    void reset() {
        if (fd != -1) {
            close(fd);
            fd = -1;
        }
        
        // .clear() does not deallocate memory, achieving O(1) resets
        read_buffer.clear();
        write_buffer.clear();
        last_active.store(0, std::memory_order_relaxed);
        is_active.store(false, std::memory_order_relaxed);
    }
};