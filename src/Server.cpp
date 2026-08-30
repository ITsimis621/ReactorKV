#include "Server.h"
#include "Logger.h"
#include "Connection.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/select.h>
#include <fcntl.h>
#include <iostream>
#include <cerrno>
#include <string>
#include <cstdlib>

constexpr int BUFFER_SIZE = 4096;
constexpr int THREAD_POOL_SIZE = 10; 

Server::Server(int port, KVStore& db, int timeout_sec) 
    : port(port), 
      db(db), 
      server_socket(-1), 
      client_timeout_sec(timeout_sec), 
      stop_pool(false),
      connection_pool(MAX_CONNECTIONS) {
    
    std::cout << format_log("INFO", "SYSTEM", "Initializing Database Engine on Port " + std::to_string(port) + "...") << std::endl;

    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        std::cerr << format_log("FATAL", "SYSTEM", "Failed to create socket") << std::endl;
        exit(1); 
    }

    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(port);

    if (bind(server_socket, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) {
        std::cerr << format_log("FATAL", "SYSTEM", "Bind failed on port " + std::to_string(port)) << std::endl;
        exit(1);
    }

    if (listen(server_socket, 50) < 0) {
        std::cerr << format_log("FATAL", "SYSTEM", "Listen failed.") << std::endl;
        exit(1);
    }

    // Reserve a file descriptor to handle graceful EMFILE/ENFILE descriptor exhaustion
    idle_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (idle_fd == -1) {
        std::cerr << format_log("FATAL", "SYSTEM", "Failed to open reserve file descriptor.") << std::endl;
        exit(1);
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        std::cerr << format_log("FATAL", "SYSTEM", "Failed to create epoll file descriptor.") << std::endl;
        exit(1);
    }

    set_non_blocking(server_socket);

    struct epoll_event event;
    event.events = EPOLLIN; 
    event.data.fd = server_socket;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_socket, &event) == -1) {
        std::cerr << format_log("FATAL", "SYSTEM", "Failed to add server socket to epoll.") << std::endl;
        exit(1);
    }

    for (int i = 0; i < THREAD_POOL_SIZE; ++i) {
        thread_pool.emplace_back(&Server::worker_thread, this);
    }
    std::cout << format_log("INFO", "SYSTEM", "Thread pool initialized with " + std::to_string(THREAD_POOL_SIZE) + " workers.") << std::endl;
}

Server::~Server() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        stop_pool = true;
    }
    
    condition.notify_all();

    for (std::thread& worker : thread_pool) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    if (server_socket != -1) close(server_socket);
    if (idle_fd != -1) close(idle_fd);
}

void Server::start(std::atomic<bool>& keep_running) {
    std::cout << format_log("INFO", "SYSTEM", "Server actively listening (epoll mode). Awaiting connections...") << std::endl;

    struct epoll_event events[MAX_EVENTS];

    while (keep_running) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);

        if (num_events == -1) {
            if (errno == EINTR) continue; 
            std::cerr << format_log("ERROR", "SYSTEM", "epoll_wait failed.") << std::endl;
            break; 
        }

        static auto last_sweep = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_sweep).count() >= 1) {
            sweep_idle_clients(); 
            last_sweep = now;
        }

        for (int i = 0; i < num_events; ++i) {
            int ready_fd = events[i].data.fd;

            if (ready_fd == server_socket) {
                while (true) {
                    int client_socket = accept(server_socket, nullptr, nullptr);
                    
                    if (client_socket < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;

                        // EMFILE/ENFILE mitigation: clear the OS accept queue using the reserve descriptor
                        if (errno == EMFILE || errno == ENFILE) {
                            std::cerr << format_log("ERROR", "NETWORK", "Maximum open files reached. Dropping connection to prevent busy-loop.") << std::endl;
                            close(idle_fd);
                            int temp_socket = accept(server_socket, nullptr, nullptr);
                            if (temp_socket >= 0) close(temp_socket);
                            idle_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                            break;
                        }
                        break; 
                    }

                    if (client_socket >= MAX_CONNECTIONS) {
                        std::cerr << format_log("ERROR", "NETWORK", "FD exceeds maximum pool capacity! Dropping.") << std::endl;
                        close(client_socket);
                        continue;
                    }

                    set_non_blocking(client_socket);

                    Connection& conn = connection_pool[client_socket];
                    conn.fd = client_socket;
                    conn.generation.fetch_add(1, std::memory_order_relaxed);
                    conn.is_active.store(true, std::memory_order_relaxed);
                    conn.last_active.store(std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);

                    struct epoll_event client_event;
                    client_event.events = EPOLLIN | EPOLLONESHOT;
                    client_event.data.fd = client_socket;

                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_socket, &client_event);
                }
            }
            else {
                int fd = events[i].data.fd;
                if (connection_pool[fd].is_active.load(std::memory_order_relaxed)) {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        client_queue.push({
                            fd,
                            connection_pool[fd].generation.load(std::memory_order_relaxed),
                            static_cast<uint32_t>(events[i].events)
                        });
                        condition.notify_one(); 
                }
            }
        }
    }

    std::cout << format_log("INFO", "SYSTEM", "Shutdown signal received. Closing server...") << std::endl;
}

bool Server::set_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1) return false;
    flags |= O_NONBLOCK;
    if (fcntl(socket_fd, F_SETFL, flags) == -1) return false;
    return true;
}

void Server::sweep_idle_clients() {
    uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
        
    for (int i = 0; i < MAX_CONNECTIONS; ++i) {
        Connection& conn = connection_pool[i];
        
        if (conn.is_active.load(std::memory_order_relaxed)) {
            uint64_t last_active = conn.last_active.load(std::memory_order_relaxed);
            
            if (now - last_active > static_cast<uint64_t>(client_timeout_sec)) {
                std::cout << format_log("INFO", "NETWORK", 
                    "Client on FD " + std::to_string(conn.fd) + " timed out. Dropping connection.") << std::endl;
                
                conn.reset(); 
            }
        }
    }
}

void Server::worker_thread() {
    while (true) {
        int fd = -1;
        uint64_t task_generation = 0;
        uint32_t event_flags = 0;
        
        { 
            std::unique_lock<std::mutex> lock(queue_mutex);
            condition.wait(lock, [this] { return stop_pool || !client_queue.empty(); });
            
            if (stop_pool && client_queue.empty()) return;
            
            auto task = client_queue.front();
            fd = std::get<0>(task);
            task_generation = std::get<1>(task);
            event_flags = std::get<2>(task);
            client_queue.pop();
        } 
        
        Connection& conn = connection_pool[fd];

        if (conn.is_active.load(std::memory_order_relaxed) &&
            conn.generation.load(std::memory_order_relaxed) == task_generation) { 

            handle_client(conn, event_flags);
        }
    }
}

void Server::handle_client(Connection& conn, uint32_t event_flags) {

    // Only attempt I/O reads if epoll explicitly signaled incoming data
    if (event_flags & EPOLLIN) {
        char temp[BUFFER_SIZE]; 

        while (true) {
            ssize_t bytes_read = read(conn.fd, temp, sizeof(temp));
            
            if (bytes_read > 0) {
                if (!conn.read_buffer.append(temp, bytes_read)) {
                    std::cerr << format_log("ERROR", "SECURITY", "Client breached 64KB ring buffer limit.") << std::endl;
                    
                    std::string error_msg = "{\"level\":\"FATAL\",\"message\":\"Payload exceeds 64KB limit\"}\n";
                    send(conn.fd, error_msg.c_str(), error_msg.length(), MSG_NOSIGNAL);
                    
                    conn.reset();
                    return;
                }
                
                conn.last_active.store(std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);
            }
            else if (bytes_read == 0) {
                conn.reset();
                return;
            } 
            else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break; 
                } 
                else if (errno == EINTR) {
                    continue; 
                } 
                else {
                    std::cerr << format_log("WARN", "NETWORK", "Client socket error. Dropping connection.") << std::endl;
                    conn.reset();
                    return;
                }
            }
        }

        std::string raw_message;

        while (conn.read_buffer.extract_line(raw_message)) {
            
            json response_json;
            try {
                json request = json::parse(raw_message);
                std::cout << format_log("INFO", "NETWORK", "Received client payload: " + request.dump()) << std::endl;
                
                std::string command = request.value("command", "");

                if (command == "SET") {
                    std::string key = request.value("key", "");
                    std::string value = request.value("value", "");

                    if (key.empty()) {
                        response_json["status"] = "ERROR";
                        response_json["message"] = "Missing or empty 'key' field.";
                    } else {
                        db.set(key, value);
                        response_json["status"] = "SUCCESS";
                        response_json["message"] = "Key saved.";
                    }
                } 
                else if (command == "GET") {
                    std::string key = request.value("key", "");
                    
                    if (key.empty()) {
                        response_json["status"] = "ERROR";
                        response_json["message"] = "Missing or empty 'key' field.";
                    } else {
                        response_json["status"] = "SUCCESS";
                        response_json["data"] = db.get(key);
                    }
                }
                else if (command == "REMOVE") {
                    std::string key = request.value("key", "");
                    
                    if (key.empty()) {
                        response_json["status"] = "ERROR";
                        response_json["message"] = "Missing or empty 'key' field.";
                    } else {
                        db.remove(key);
                        response_json["status"] = "SUCCESS";
                        response_json["message"] = "Key removed.";
                    }
                }
                else {
                    response_json["status"] = "ERROR";
                    response_json["message"] = "Unknown or missing command.";
                }
            } catch (const json::parse_error& e) {
                response_json["status"] = "ERROR";
                response_json["message"] = "Invalid JSON payload format.";
                std::cerr << format_log("WARN", "SYSTEM", "Received malformed JSON from client.") << std::endl;
            }

            std::string response_str = response_json.dump() + "\n";
            if (!conn.write_buffer.append(response_str.c_str(), response_str.length())) {
                std::cerr << format_log("ERROR", "NETWORK", "Write buffer overflow.") << std::endl;
                conn.reset();
                return;
            }
        }
    }

    // Bypass read loop and jump straight to flushing if OS TCP buffer just drained (EPOLLOUT)
    if ((event_flags & EPOLLOUT) || !conn.write_buffer.is_empty()) {

        bool data_sent = false;

        while (!conn.write_buffer.is_empty()) {
            const char* chunk_ptr = nullptr;
            size_t chunk_len = conn.write_buffer.get_contiguous_read_chunk(chunk_ptr);

            ssize_t bytes_sent = send(conn.fd, chunk_ptr, chunk_len, MSG_NOSIGNAL);
            
            if (bytes_sent == -1) {
                if (errno == EINTR) continue; 
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;

                std::cerr << format_log("WARN", "NETWORK", "Client disconnected during write.") << std::endl;
                conn.reset();
                return; 
            }

            conn.write_buffer.consume(bytes_sent);
            data_sent = true;
        }
            
        if (data_sent) {
            conn.last_active.store(std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);
        } 
    }

    struct epoll_event event;
    
    if (!conn.write_buffer.is_empty()) {
        // Buffer is full (EAGAIN hit). Re-arm with EPOLLOUT to wait for OS TCP buffer to drain.
        event.events = EPOLLIN | EPOLLOUT | EPOLLONESHOT;
    } else {
        event.events = EPOLLIN | EPOLLONESHOT;
    }
    
    event.data.fd = conn.fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn.fd, &event);
}