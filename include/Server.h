#pragma once

/**
 * @file Server.h
 * @brief TCP server implementing a Reactor/Worker concurrency model.
 * 
 * Utilizes Linux epoll for non-blocking event multiplexing (Reactor) 
 * and a fixed-size thread pool for parallel request processing (Workers), enabling 
 * high-throughput, highly concurrent database access. Memory is safely managed
 * across threads via shared ownership (std::shared_ptr).
 */

#include "KVStore.h"
#include "Connection.h"
#include <atomic>
#include <memory>
#include <vector>
#include <unordered_map>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sys/epoll.h>

/**
 * @class Server
 * @brief Manages TCP socket connections and dispatches them via a fixed-size thread pool.
 */
class Server {
private:
    int port;                               ///< Port on which the server listens
    KVStore& db;                            ///< Reference to the shared key-value engine
    int server_socket;                      ///< Primary listening socket descriptor
    int epoll_fd;                           ///< Core event loop descriptor
    int idle_fd;                            ///< Reserve file descriptor for EMFILE/ENFILE exhaustion handling
    
    static constexpr int MAX_EVENTS = 1024; ///< Maximum number of events processed per epoll_wait tick
    int client_timeout_sec;                 ///< Idle timeout threshold before dropping a client

    std::vector<std::thread> thread_pool;   ///< Fixed-size pool of worker threads
    std::mutex queue_mutex;                 ///< Protects the client queue and termination flag
    std::condition_variable condition;      ///< Signals workers when jobs are available or shutting down
    bool stop_pool;                         ///< Flag to safely terminate all worker threads

    std::queue<std::pair<std::shared_ptr<Connection>, uint32_t>> client_queue;  ///< Pending jobs representing sockets ready for I/O
    std::unordered_map<int, std::shared_ptr<Connection>> active_connections;    ///< Shared memory ownership of active clients
    std::mutex connections_mutex;           ///< Protects the active connections map

    /**
     * @brief Safely removes a connection from the active map.
     * 
     * Performs an identity check to prevent iterator invalidation and guard 
     * against Linux file descriptor recycling race conditions.
     * @param conn Shared pointer to the connection being removed.
     */
    void safe_remove_connection(std::shared_ptr<Connection> conn);

    /**
     * @brief Sets a socket file descriptor to non-blocking mode.
     * @param socket_fd The target file descriptor.
     * @return True if successful, false otherwise.
     */
    bool set_non_blocking(int socket_fd);

    /**
     * @brief Periodically evicts clients that have exceeded the idle timeout.
     */
    void sweep_idle_clients();

    /**
     * @brief Main loop executed by each worker thread in the pool.
     */
    void worker_thread();

    /**
     * @brief Parses JSON, executes DB operations, and buffers responses.
     * @param conn Shared pointer to the active connection state.
     * @param event_flags The epoll bitmask triggering the wakeup (EPOLLIN/EPOLLOUT).
     */
    void handle_client(std::shared_ptr<Connection> conn, uint32_t event_flags);

public:
    /**
     * @brief Initializes the epoll reactor and launches the thread pool.
     * @param port The TCP port to bind the server to.
     * @param db A live reference to the KVStore engine.
     * @param timeout_sec The idle timeout threshold (defaults to 60 seconds).
     * @throw std::system_error on fatal socket or epoll initialization failure.
     */
    Server(int port, KVStore& db, int timeout_sec = 60);
    
    /**
     * @brief Triggers thread pool shutdown and cleans up socket descriptors.
     */
    ~Server();

    /**
     * @brief Enters the epoll event loop, accepting clients and queuing tasks.
     * @param keep_running Atomic flag to gracefully stop the reactor loop.
     */
    void start(std::atomic<bool>& keep_running);
};