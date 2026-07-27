#!/usr/bin/env python3
"""
Integration and Stress Test Suite for ReactorKV.

This module performs black-box validation against a live ReactorKV instance.
It strictly tests the networking boundaries, evaluating TCP pipelining, 
non-blocking I/O handling, memory-exhaustion defenses, and lock-free 
background eviction.

Note:
    ReactorKV must be actively running on localhost:8080 prior to execution.
"""

import unittest
import socket
import json
import time
import threading
import concurrent.futures

try:
    import resource
    HAS_RESOURCE = True
except ImportError:
    HAS_RESOURCE = False

HOST = '127.0.0.1'
PORT = 8080
ZOMBIE_SLEEP_TIME = 7  # Must be strictly greater than the server's --timeout flag


class TestReactorKVIntegration(unittest.TestCase):
    """
    Test fixture for ReactorKV network edge cases and concurrency limits.
    """

    def setUp(self):
        """
        Validates server availability before executing each test.
        
        Raises:
            AssertionError: If the server refuses the connection on HOST:PORT.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2.0)
                s.connect((HOST, PORT))
        except ConnectionRefusedError:
            self.fail("ReactorKV is not running on localhost:8080. Start the server first.")

    def test_pipeline_concurrency(self):
        """
        Validates the server's ability to handle highly concurrent pipelined requests.

        Simulates multiple independent clients transmitting large batches of commands
        without waiting for intermediate responses. This verifies the O(N) sliding 
        offset buffer parsing and epoll event loop stability.
        
        Expected Outcome:
            All pipelined commands are parsed and responded to without data loss.
        """
        def run_client():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0)
                s.connect((HOST, PORT))
                
                # Fire 50 commands instantly (TCP Pipelining)
                for i in range(50):
                    payload = {"command": "SET", "key": f"pipe_key_{i}", "value": f"data_{i}"}
                    s.sendall(json.dumps(payload).encode('utf-8') + b'\n')

                # Validate all 50 responses are received cleanly
                responses_received = 0
                while responses_received < 50:
                    chunk = s.recv(4096).decode('utf-8')
                    if not chunk:
                        break
                    responses_received += chunk.count("\n")
                    
            return responses_received

        # Execute 20 clients concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(run_client) for _ in range(20)]
            for future in concurrent.futures.as_completed(futures):
                self.assertEqual(future.result(), 50, "A client dropped pipelined responses.")

    def test_drip_feed_non_blocking(self):
        """
        Tests the epoll non-blocking read loop against slowloris-style I/O.
        
        Client A transmits a payload 1 byte at a time. Concurrently, Client B 
        connects and executes a fast command to prove the server's worker threads 
        are not blocked by incomplete network reads.

        Expected Outcome:
            The fast client completes immediately, and the slow client eventually 
            completes its transaction successfully.
        """
        drip_success = False

        def slow_dripper():
            nonlocal drip_success
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_slow:
                # Disable Nagle's algorithm to force strict 1-byte transmissions
                s_slow.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s_slow.connect((HOST, PORT))
                payload = json.dumps({"command": "SET", "key": "drip", "value": "survived"}).encode('utf-8') + b'\n'
                
                for byte in payload:
                    s_slow.send(bytes([byte]))
                    time.sleep(0.05)  # Drip one byte every 50ms
                
                response = s_slow.recv(1024).decode('utf-8')
                if "SUCCESS" in response:
                    drip_success = True

        # 1. Start the slow dripping client in the background
        dripper_thread = threading.Thread(target=slow_dripper)
        dripper_thread.start()

        # 2. Give the dripper a moment to connect and hold the server's attention
        time.sleep(0.5)

        # 3. Fire a fast client on the main thread to prove the server isn't blocked
        fast_response = ""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_fast:
            s_fast.settimeout(1.0) 
            s_fast.connect((HOST, PORT))
            
            fast_payload = json.dumps({"command": "SET", "key": "fast", "value": "im_fast"}).encode('utf-8') + b'\n'
            s_fast.sendall(fast_payload)
            
            try:
                fast_response = s_fast.recv(1024).decode('utf-8')
            except socket.timeout:
                self.fail("Fast client timed out! The server is blocked by the slow client.")

        dripper_thread.join()

        self.assertIn("SUCCESS", fast_response, "Fast client failed to get a SUCCESS response.")
        self.assertTrue(drip_success, "Slow client failed to complete its drip-feed.")

    def test_buffer_choke(self):
        """
        Tests asynchronous flushing and EPOLLOUT re-arming logic.
        
        Forces the server to allocate a 5MB response while intentionally shrinking
        the client's OS TCP receive buffer. This triggers EAGAIN on the server side, 
        validating its ability to yield the thread and resume writing later.

        Expected Outcome:
            The complete 5MB payload is downloaded without server-side truncation.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Artificially shrink the client receive buffer to create a network bottleneck
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            s.connect((HOST, PORT))
            
            massive_value = "A" * 5_000_000 
            
            # Upload
            set_payload = json.dumps({"command": "SET", "key": "heavy", "value": massive_value}).encode('utf-8') + b'\n'
            s.sendall(set_payload)
            self.assertIn("SUCCESS", s.recv(1024).decode('utf-8'))
            
            # Download
            get_payload = json.dumps({"command": "GET", "key": "heavy"}).encode('utf-8') + b'\n'
            s.sendall(get_payload)
            
            bytes_received = 0
            while True:
                chunk = s.recv(8192)
                if not chunk:
                    break
                bytes_received += len(chunk)
                if b'\n' in chunk: 
                    break
                    
            self.assertGreater(bytes_received, 5_000_000, "Failed to download complete payload.")

    def test_c10k_connection_hold(self):
        """
        Validates descriptor limits and epoll memory efficiency.
        
        Simultaneously holds 1,000 idle connections to ensure the server 
        does not exhaust thread resources or trigger segmentation faults.

        Expected Outcome:
            Server remains stable and accepts all connections up to the OS limit.
        """
        if HAS_RESOURCE:
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft < 1050:
                self.skipTest(f"Client OS file limit ({soft}) is too low to run 1000 connections.")

        sockets = []
        try:
            for _ in range(1000):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((HOST, PORT))
                sockets.append(s)
            
            time.sleep(2)
        finally:
            for s in sockets:
                s.close()

    def test_zombie_connection_eviction(self):
        """
        Validates the lock-free background sweeper.
        
        Connects as a zombie client and waits ZOMBIE_SLEEP_TIME seconds to verify 
        the server successfully drops the connection for exceeding the idle timeout,
        utilizing the atomic timestamp validation.

        Expected Outcome:
            The server forcibly severs the TCP connection.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            
            time.sleep(ZOMBIE_SLEEP_TIME)
            
            with self.assertRaises((BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
                s.sendall(b'{"command": "GET", "key": "test"}\n')
                response = s.recv(1024)
                if not response:
                    raise ConnectionResetError("Server gracefully closed the connection.")

    def test_max_payload_breach(self):
        """
        Validates the server's defense against memory exhaustion attacks.
        
        Sends a payload exceeding the 8MB MAX_PAYLOAD_SIZE to ensure the 
        server safely drops the connection and returns the FATAL error.

        Expected Outcome:
            Server replies with a FATAL status and immediately closes the socket.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            
            massive_string = "A" * 9_000_000 
            s.sendall(massive_string.encode('utf-8'))
            
            response = s.recv(1024).decode('utf-8')
            
            self.assertIn("FATAL", response, "Server did not return a FATAL status.")
            self.assertIn("Payload Too Large", response, "Server did not specify payload error.")
            
            chunk = s.recv(1024)
            self.assertEqual(len(chunk), 0, "Server left the socket open after a FATAL breach.")

    def test_malformed_json_handling(self):
        """
        Validates the try/catch logic around the JSON parser.
        
        Transmits invalid JSON format to ensure the server gracefully intercepts
        the parse error without crashing the worker thread.

        Expected Outcome:
            Server flags the payload as ERROR and maintains the connection.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            
            bad_payload = b'{"command": "SET", "key": oops_forgot_quotes}\n'
            s.sendall(bad_payload)
            
            response = s.recv(1024).decode('utf-8')
            
            self.assertIn("ERROR", response, "Server did not flag the malformed JSON.")
            self.assertIn("Invalid JSON", response, "Server did not return the correct error message.")


if __name__ == '__main__':
    unittest.main(verbosity=2)