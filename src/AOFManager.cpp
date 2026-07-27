#include "AOFManager.h"
#include "Logger.h"

#include <iostream>
#include <filesystem>
#include <algorithm> 
#include <utility>   
#include <cstring>   

#include <fcntl.h>   
#include <unistd.h>  
#include <sys/uio.h> 
#include <limits.h>  
#include <cerrno>    

// Define IOV_MAX fallback for systems lacking the strict POSIX limit
#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

AOFManager::AOFManager(const std::string& filepath) : file_path(filepath), keep_running(true) {
    std::filesystem::create_directories("data");
    worker = std::thread(&AOFManager::background_worker, this);
}

AOFManager::~AOFManager() {
    keep_running = false;
    cv.notify_one();

    if (worker.joinable()) {
        worker.join();
    }
}

void AOFManager::log_command(std::string command) {
    { 
        std::lock_guard<std::mutex> lock(queue_mutex);
        log_buffer.push_back(std::move(command));
    }
    cv.notify_one(); 
}

void AOFManager::background_worker() {
    int fd = open(file_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    
    if (fd == -1) {
        std::cerr << format_log("FATAL", "AOF", "Cannot open AOF file for writing!") << std::endl;
        return; 
    }

    // Pre-allocate structures to eliminate heap allocations during continuous operation
    std::vector<std::string> batch;
    std::vector<struct iovec> iov;

    while (true) {
        { 
            std::unique_lock<std::mutex> lock(queue_mutex);
            cv.wait(lock, [this] { return !keep_running || !log_buffer.empty(); });

            if (!keep_running && log_buffer.empty()) break;

            std::swap(batch, log_buffer);
        } 

        // Execute disk I/O operations outside the critical section to minimize lock contention
        size_t batch_index = 0;
        
        while (batch_index < batch.size()) {
            size_t chunk_size = std::min(batch.size() - batch_index, static_cast<size_t>(IOV_MAX));
            iov.resize(chunk_size);
            
            for (size_t i = 0; i < chunk_size; ++i) {
                iov[i].iov_base = (batch[batch_index + i].data());
                iov[i].iov_len = batch[batch_index + i].size();
            }
            
            size_t iov_offset = 0; 
            
            while (iov_offset < iov.size()) {
                ssize_t bytes_written = writev(fd, iov.data() + iov_offset, iov.size() - iov_offset);
                
                if (bytes_written == -1) {
                    if (errno == EINTR) continue; 
                    
                    std::string error_msg = "Write Error: " + std::string(std::strerror(errno));
                    std::cerr << format_log("FATAL", "AOF", error_msg) << std::endl;
                    close(fd);
                    return; 
                }

                if (bytes_written == 0 && iov[iov_offset].iov_len > 0) {
                    std::cerr << format_log("FATAL", "AOF", "Write Error: OS refused to write data (0 bytes returned)") << std::endl;
                    close(fd);
                    return;
                }
                
                size_t remaining_bytes = static_cast<size_t>(bytes_written);
                
                while (iov_offset < iov.size() && remaining_bytes >= iov[iov_offset].iov_len) {
                    remaining_bytes -= iov[iov_offset].iov_len;
                    iov_offset++;   
                    batch_index++;  
                }
                
                // Handle partial writes (e.g., due to OS interrupts or full disk buffers).
                // Advance the iov_base pointer and decrease iov_len for the partially written chunk.
                if (remaining_bytes > 0 && iov_offset < iov.size()) {
                    iov[iov_offset].iov_base = static_cast<char*>(iov[iov_offset].iov_base) + remaining_bytes;
                    iov[iov_offset].iov_len -= remaining_bytes;
                }
            }
        }
        
        // Use fdatasync over fsync to avoid unnecessary metadata flushes, optimizing disk throughput
        if (!batch.empty()) {
            fdatasync(fd); 
        }

        batch.clear();
    }
    
    close(fd);
}