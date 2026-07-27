#include "KVStore.h"
#include "Logger.h"

#include <iostream>
#include <string>
#include <stdexcept>
#include <fstream>
#include <cstdlib>

constexpr const char* LOG_FILENAME = "data/reactorkv.aof";

size_t KVStore::get_shard_index(const std::string& key) const {
    return std::hash<std::string>{}(key) % NUM_SHARDS;
}

void KVStore::load_from_disk() {
    // CRITICAL: Open in binary mode so exact byte counting for RESP parsing is accurate
    std::ifstream infile(LOG_FILENAME, std::ios::binary);
    if (!infile.is_open()) {
        std::cout << format_log("INFO", "SYSTEM", "No existing AOF log found.") << std::endl;
        return;
    }

    auto read_resp_string = [&infile](std::string& out_str) -> bool {
        std::string line;
        if (!std::getline(infile, line)) return false;
        
        if (!line.empty() && line.back() == '\r') line.pop_back();
        
        if (line.empty() || line[0] != '$') return false;

        int len = 0;
        try {
            len = std::stoi(line.substr(1));
        } catch (const std::exception&) { return false; }

        out_str.resize(len);
        infile.read(out_str.data(), len);

        // Verify the trailing \r\n exists to protect against torn disk writes
        char cr, lf;
        infile.get(cr); infile.get(lf);
        if (cr != '\r' || lf != '\n') return false;

        return true;
    };
    
    std::string line;
    while (std::getline(infile, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        
        if (line.empty() || line[0] != '*') {
            std::cout << format_log("WARN", "AOF", "AOF corruption detected. Halting load to protect memory.") << std::endl;
            exit(1); 
        }

        int array_len = std::stoi(line.substr(1));

        if (array_len == 3) {
            std::string cmd, key, val;
            if (!read_resp_string(cmd) || !read_resp_string(key) || !read_resp_string(val)) {
                std::cout << format_log("WARN", "AOF", "Torn write detected (SET). Halting AOF load.") << std::endl;
                exit(1); 
            }
            if (cmd == "SET") {
                size_t index = get_shard_index(key);
                shards[index].data[key] = val;
            }
        } 
        else if (array_len == 2) {
            std::string cmd, key;
            if (!read_resp_string(cmd) || !read_resp_string(key)) {
                std::cout << format_log("WARN", "AOF", "Torn write detected (REMOVE). Halting AOF load.") << std::endl;
                exit(1); 
            }
            if (cmd == "REMOVE") {
                size_t index = get_shard_index(key);
                shards[index].data.erase(key);
            }
        }
    }

    size_t total_records = 0;
    for (const auto& shard : shards) {
        total_records += shard.data.size();
    }

    std::cout << format_log("INFO", "SYSTEM", "Restored " + std::to_string(total_records) + " records.") << std::endl;
}

KVStore::KVStore() : aof(LOG_FILENAME) {
    load_from_disk();
}

KVStore::~KVStore() {}

void KVStore::set(const std::string& key, const std::string& value) {
    size_t index = get_shard_index(key);

    // Construct the RESP string outside the critical section to minimize lock hold time
    std::string log_cmd = "*3\r\n$3\r\nSET\r\n$" + 
                          std::to_string(key.size()) + "\r\n" + key + "\r\n$" + 
                          std::to_string(value.size()) + "\r\n" + value + "\r\n";
    
    {
        std::unique_lock<std::shared_mutex> lock(shards[index].mutex);
        shards[index].data[key] = value;
        aof.log_command(std::move(log_cmd));
    } 
    
    std::cout << format_log("INFO", "DB", "Operation: SET", key) << std::endl;
}

std::string KVStore::get(const std::string& key) {
    size_t index = get_shard_index(key);
    
    std::shared_lock<std::shared_mutex> lock(shards[index].mutex);
    auto it = shards[index].data.find(key);
    if (it != shards[index].data.end()) {
        return it->second;
    }
    return "[DB] ERROR: Key not found";
}

void KVStore::remove(const std::string& key) {
    size_t index = get_shard_index(key);
    bool removed = false;
    
    std::string log_cmd = "*2\r\n$6\r\nREMOVE\r\n$" + 
                          std::to_string(key.size()) + "\r\n" + key + "\r\n";

    {
        std::unique_lock<std::shared_mutex> lock(shards[index].mutex);
        removed = shards[index].data.erase(key);

        if (removed) {
            aof.log_command(std::move(log_cmd));
        }
    }
    
    if (removed) {
        std::cout << format_log("INFO", "DB", "Operation: REMOVE", key) << std::endl;
    } else {
        std::cout << format_log("WARN", "DB", "Cannot remove, key not found.") << std::endl;
    }
}