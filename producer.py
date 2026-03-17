import socket
import time
import json
import random
import argparse

def start_producer(host, port, rate, enable_burst):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Allows the port to be reused immediately after restarting
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(1)
    
    print(f"Producer ready. Listening on {host}:{port}...")
    print("WAITING FOR CONSUMER TO CONNECT...")
    
    conn, addr = s.accept()
    print(f"Consumer connected from {addr}. Starting stream at {rate} msg/s.")

    try:
        while True:
            messages_to_send = rate
            
            # Simulate a real-world burst (10% chance to send 5x data)
            if enable_burst and random.random() < 0.10:
                messages_to_send = rate * 5
                print(f"[BURST] Spiking traffic to {messages_to_send} messages!")

            start_time = time.time()
            
            for _ in range(messages_to_send):
                payload = {
                    "user_id": random.randint(1000, 9999),
                    "amount": round(random.uniform(1.0, 500.0), 2),
                    "timestamp": time.time()  # Crucial for latency metrics
                }
                # Send data via TCP
                conn.sendall((json.dumps(payload) + "\n").encode('utf-8'))

            # Sleep to maintain the target rate
            elapsed = time.time() - start_time
            sleep_time = max(0.0, 1.0 - elapsed)
            time.sleep(sleep_time)
            
    except (BrokenPipeError, ConnectionResetError):
        print("Consumer disconnected. Shutting down producer.")
    finally:
        conn.close()
        s.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming Data Producer")
    parser.add_argument("--rate", type=int, default=100, help="Messages per second")
    parser.add_argument("--burst", action="store_true", help="Enable random traffic bursts")
    args = parser.parse_args()
    
    start_producer("localhost", 9999, args.rate, args.burst)