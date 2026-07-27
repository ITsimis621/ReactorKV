#pragma once

/**
 * @file Connection.h
 * @brief Connection state management for the server.
 */

#include <string>
#include <atomic>
#include <chrono>
#include <unistd.h>

/**
 * @struct Connection
 * @brief Encapsulates all state for a single connected TCP client.
 * 
 * Maintains I/O buffers and lock-free metadata required for 
 * asynchronous event-driven network processing.
 */
struct Connection {
    int fd;                                            ///< The client's socket file descriptor
    std::string read_buffer;                           ///< Unprocessed incoming TCP stream data
    std::string write_buffer;                          ///< Queued outbound JSON responses
    std::atomic<uint64_t> last_active;                 ///< Lock-free epoch timestamp for idle eviction

    /**
     * @brief Initializes a new client connection.
     * @param socket_fd The accepted socket file descriptor.
     */
    explicit Connection(int socket_fd) 
        : fd(socket_fd) {
        last_active.store(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);
    }
    
    /**
     * @brief RAII Destructor.
     * 
     * Guarantees the OS socket is cleanly closed when the connection 
     * is evicted or the server shuts down, preventing descriptor leaks.
     */
    ~Connection() {
        if (fd != -1) {
            close(fd);
        }
    }
};