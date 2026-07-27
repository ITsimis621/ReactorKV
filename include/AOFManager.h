#pragma once

/**
 * @file AOFManager.h
 * @brief Background Append-Only File (AOF) logging mechanism.
 * 
 * Provides an asynchronous, non-blocking disk writing engine using 
 * vectored I/O to ensure fast database transaction durability.
 */

#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

/**
 * @class AOFManager
 * @brief Manages background asynchronous logging to the Append-Only File (AOF).
 * 
 * Implements a thread-safe Producer-Consumer pattern to batch and flush 
 * database commands to disk without blocking the main execution thread.
 */
class AOFManager {
private:
    std::string file_path;                  ///< Destination path for the log file
    std::vector<std::string> log_buffer;    ///< Primary buffer collecting incoming commands
    std::mutex queue_mutex;                 ///< Protects the log_buffer during pushes and swaps
    std::condition_variable cv;             ///< Signals the worker thread when data or shutdown is pending
    
    std::thread worker;                     ///< Background thread executing the disk I/O
    std::atomic<bool> keep_running;         ///< Flag to safely terminate the consumer loop

    /**
     * @brief Consumer loop executed by the background thread.
     * 
     * Sleeps until data is available, then swaps the buffer in O(1) time. 
     * Uses POSIX writev() to write batched data in a single system call.
     */
    void background_worker();

public:
    /**
     * @brief Constructs the AOFManager and initializes the background worker thread.
     * @param filepath The destination path for the append-only log file.
     * @throw std::filesystem::filesystem_error if the data directory cannot be created.
     * @throw std::system_error if the background thread fails to launch.
     */
    explicit AOFManager(const std::string& filepath);
    
    /**
     * @brief Safely halts the background thread.
     * 
     * Sets the termination flag, wakes the worker, and blocks until all 
     * pending commands in the buffer are flushed to disk.
     */
    ~AOFManager();

    /**
     * @brief Appends a command to the in-memory log buffer.
     * 
     * This method is strictly thread-safe and non-blocking (aside from minor lock contention).
     * 
     * @param command The RESP-formatted command string. Passed by value to allow move semantics.
     */
    void log_command(std::string command);
};