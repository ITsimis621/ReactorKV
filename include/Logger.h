#pragma once

/**
 * @file Logger.h
 * @brief Structured logging utility.
 */

#include <string>
#include <nlohmann/json.hpp>

/**
 * @brief Generates a structured JSON log entry.
 * 
 * Formats terminal output for automated ingestion by log aggregation platforms 
 * (e.g., ELK stack, Datadog, Splunk).
 * 
 * @param level Severity level (INFO, WARN, ERROR, FATAL).
 * @param component The subsystem generating the log (SYSTEM, DB, NETWORK, AOF).
 * @param message Human-readable log description.
 * @param key Optional database key context.
 * @return A serialized JSON string.
 */
inline std::string format_log(const std::string& level, const std::string& component, const std::string& message, const std::string& key = "") {
    nlohmann::json log_obj;
    
    log_obj["level"] = level;
    log_obj["component"] = component;
    
    if (!key.empty()) {
        log_obj["key"] = key;
    }
    
    log_obj["message"] = message;
    
    return log_obj.dump();
}