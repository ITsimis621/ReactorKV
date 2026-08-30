#pragma once

/**
 * @file RingBuffer.h
 * @brief A static, zero-allocation circular buffer for network I/O.
 */

#include <array>
#include <string>
#include <cstddef>
#include <cstring>

constexpr size_t RING_BUFFER_SIZE = 65536; // 64KB fixed payload limit

class RingBuffer {
private:
    std::array<char, RING_BUFFER_SIZE> buffer;
    size_t head{0};      ///< Read cursor (where parsing begins)
    size_t tail{0};      ///< Write cursor (where new TCP bytes are appended)
    bool full{false};    ///< Crucial flag to distinguish between empty and completely full

public:
    RingBuffer() = default;

    /**
     * @brief Resets the buffer to an empty state. O(1) operation.
     */
    void clear() {
        head = 0;
        tail = 0;
        full = false;
    }

    /**
     * @brief Calculates how many bytes of unread data are in the buffer.
     */
    size_t readable_bytes() const {
        if (full) return RING_BUFFER_SIZE;
        if (tail >= head) return tail - head;
        return RING_BUFFER_SIZE - (head - tail);
    }

    /**
     * @brief Calculates how much free space remains for incoming TCP data.
     */
    size_t available_space() const {
        return RING_BUFFER_SIZE - readable_bytes();
    }

    bool is_empty() const {
        return readable_bytes() == 0;
    }

    /**
     * @brief Gets a direct pointer to the next contiguous readable chunk.
     * @param data_ptr Reference to the pointer that will be set.
     * @return The length of the contiguous chunk.
     */
    size_t get_contiguous_read_chunk(const char*& data_ptr) const {
        if (is_empty()) return 0;
        
        data_ptr = buffer.data() + head;

        if (full || tail < head) {
            return RING_BUFFER_SIZE - head;
        }
        
        return tail - head;
    }

    /**
     * @brief Advances the head cursor after a successful network send.
     */
    void consume(size_t len) {
        if (len == 0) return;
        head = (head + len) % RING_BUFFER_SIZE;
        full = false; 
    }

    /**
     * @brief Writes incoming network bytes into the circular array.
     * @return True if successful, False if the payload exceeds available space.
     */
    bool append(const char* data, size_t len) {
        if (len == 0) return true;
        
        if (len > available_space()) {
            return false; 
        }

        size_t space_to_end = RING_BUFFER_SIZE - tail;

        if (len <= space_to_end) {
            std::memcpy(buffer.data() + tail, data, len);
        } else {
            std::memcpy(buffer.data() + tail, data, space_to_end);
            std::memcpy(buffer.data(), data + space_to_end, len - space_to_end);
        }

        tail = (tail + len) % RING_BUFFER_SIZE;
        
        if (tail == head) {
            full = true;
        }

        return true;
    }

    /**
     * @brief Scans for a newline '\n' and extracts a complete JSON string.
     * @return True if a full message was found and extracted.
     */
    bool extract_line(std::string& out_message) {
        size_t total_readable = readable_bytes();
        if (total_readable == 0) return false;

        size_t current_index = head;
        size_t msg_len = 0;
        bool found_newline = false;

        for (size_t i = 0; i < total_readable; ++i) {
            if (buffer[current_index] == '\n') {
                found_newline = true;
                msg_len = i + 1;
                break;
            }
            current_index = (current_index + 1) % RING_BUFFER_SIZE;
        }

        if (!found_newline) return false;

        out_message.clear();
        out_message.reserve(msg_len);

        size_t space_to_end = RING_BUFFER_SIZE - head;

        if (msg_len <= space_to_end) {
            out_message.append(buffer.data() + head, msg_len);
        } else {
            out_message.append(buffer.data() + head, space_to_end);
            out_message.append(buffer.data(), msg_len - space_to_end);
        }

        head = (head + msg_len) % RING_BUFFER_SIZE;
        full = false;

        return true;
    }
};