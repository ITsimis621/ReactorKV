/**
 * @file main.cpp
 * @brief Application entry point.
 */

#include "KVStore.h"
#include "Server.h"
#include "Logger.h"

#include <iostream>
#include <csignal>
#include <atomic>
#include <string>

// Global atomic flag for graceful reactor shutdown
std::atomic<bool> keep_running(true);

/**
 * @brief Intercepts POSIX signals to trigger a graceful shutdown sequence.
 * @param signal The received signal integer (e.g., SIGINT, SIGTERM).
 */
void handle_signal(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        keep_running = false;
    }
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    int timeout_sec = 60; // Default production timeout

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--timeout" && i + 1 < argc) {
            timeout_sec = std::stoi(argv[++i]);
        }
    }

    KVStore myDatabase;
    Server myServer(8080, myDatabase, timeout_sec);

    myServer.start(keep_running);

    return 0;
}