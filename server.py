import socket
import json
import threading
import time
import random
from threading import Lock, Timer
from collections import defaultdict

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]
TOKEN = config["token"]

client_connections = {} 
clients_lock = Lock()
client_keepalive_timers = {}  
client_pending_responses = defaultdict(dict) 
KEEPALIVE_INTERVAL = 10
KEEPALIVE_TIMEOUT = 10 

def disconnect_client(conn, reason="No keepalive response"):
    username = None
    with clients_lock:
        if conn in client_connections:
            username = client_connections[conn]
            del client_connections[conn]
        
        if conn in client_keepalive_timers:
            try:
                client_keepalive_timers[conn].cancel()
            except:
                pass
            del client_keepalive_timers[conn]
        
        if conn in client_pending_responses:
            del client_pending_responses[conn]
    
    try:
        conn.close()
    except:
        pass
    
    print(f"Disconnected {username}: {reason}")
    
    if username:
        send_to_all_except(f"CONNECTION {username} left\n", exclude_conn=conn)

def send_to_all_except(message, exclude_conn=None):
    dead_clients = []
    with clients_lock:
        clients_to_notify = list(client_connections.keys())
    
    for client in clients_to_notify:
        if client == exclude_conn:
            continue
        try:
            client.sendall(message.encode("utf-8"))
        except:
            dead_clients.append(client)
    
    for client in dead_clients:
        with clients_lock:
            if client in client_connections:
                username = client_connections[client]
                del client_connections[client]
                if client in client_keepalive_timers:
                    try:
                        client_keepalive_timers[client].cancel()
                    except:
                        pass
                    del client_keepalive_timers[client]
                if client in client_pending_responses:
                    del client_pending_responses[client]
                try:
                    client.close()
                except:
                    pass
                print(f"Cleaned up dead client {username} during broadcast")

def broadcast_message(message, exclude_conn=None):
    dead_clients = []
    with clients_lock:
        clients_to_notify = list(client_connections.keys())
    
    for client in clients_to_notify:
        if client == exclude_conn:
            continue
        try:
            client.sendall(message.encode("utf-8"))
        except:
            dead_clients.append(client)
    
    for client in dead_clients:
        disconnect_client(client, "Failed during broadcast")

def send_keepalive(conn):
    if conn.fileno() == -1: 
        disconnect_client(conn, "Socket closed")
        return
    
    challenge = random.randint(1000, 9999)
    
    with clients_lock:
        if conn not in client_pending_responses:
            client_pending_responses[conn] = {}
        client_pending_responses[conn][challenge] = time.time()
    
    try:
        message = f"KEEPALIVE {challenge}\n"
        conn.sendall(message.encode("utf-8"))
    except:
        disconnect_client(conn, "Keepalive send failed")
        return
    
    threading.Timer(KEEPALIVE_TIMEOUT, check_keepalive_response, args=[conn, challenge]).start()
    
    with clients_lock:
        if conn in client_connections:
            client_keepalive_timers[conn] = threading.Timer(
                KEEPALIVE_INTERVAL, 
                send_keepalive, 
                args=[conn]
            )
            client_keepalive_timers[conn].daemon = True
            client_keepalive_timers[conn].start()

def check_keepalive_response(conn, expected_challenge):
    with clients_lock:
        if conn not in client_pending_responses:
            return
        
        if expected_challenge in client_pending_responses[conn]:
            disconnect_client(conn, f"No response to keepalive challenge {expected_challenge}")

def handle_keepalive_response(conn, challenge_str):
    try:
        challenge = int(challenge_str)
    except ValueError:
        print(f"Invalid keepalive response from {client_connections.get(conn, 'unknown')}")
        return False
    
    with clients_lock:
        if conn not in client_pending_responses:
            return False
        
        if challenge in client_pending_responses[conn]:
            del client_pending_responses[conn][challenge]
            return True
        else:
            print(f"Unexpected keepalive challenge {challenge} from {client_connections.get(conn, 'unknown')}")
            return False

def handle_client(conn, addr):
    print(f"Attempt from {addr}")
    username = None
    
    try:
        conn.settimeout(10)
        auth_data = conn.recv(1024).decode("utf-8").strip()
        conn.settimeout(None)

        if not auth_data.startswith("AUTH "):
            print(f"Invalid auth format from {addr}")
            conn.close()
            return
        
        parts = auth_data.split()
        if len(parts) < 3:
            print(f"Invalid auth format from {addr}")
            conn.close()
            return
        
        received_token = parts[1]
        username = parts[2]
        
        if received_token != TOKEN or not username:
            print(f"Auth failed for {addr}")
            conn.sendall(b"ERROR Invalid token\n") 
            conn.close()
            return
        
        with clients_lock:
            if username in client_connections.values():
                print(f"Username {username} already taken")
                conn.sendall(b"ERROR Username already taken\n")
                conn.close()
                return
        
        print(f"Auth success for {addr} as {username}")
        conn.sendall(b"AUTH_SUCCESS\n")
        print(f"Sent AUTH_SUCCESS to {addr}")
        
        with clients_lock:
            client_connections[conn] = username
        
        threading.Timer(KEEPALIVE_INTERVAL, send_keepalive, args=[conn]).start()
        
        broadcast_message(f"CONNECTION {username} joined\n", exclude_conn=conn)
        
        while True:
            data = conn.recv(4096)
            if not data:
                break
            
            data_str = data.decode("utf-8").strip()
            
            if data_str == "ONLINE":
                print(f"Processing ONLINE request from {username}")
                with clients_lock:
                    online_players = list(client_connections.values())
                response = f"ONLINE {','.join(online_players)}\n"
                conn.sendall(response.encode("utf-8"))
                continue
            
            if data_str.startswith("KEEPALIVE_RESPONSE "):
                parts = data_str.split()
                if len(parts) == 2:
                    handle_keepalive_response(conn, parts[1])
                continue
            
            if not data_str.startswith("MESSAGE "):
                conn.sendall(b"ERROR Invalid message\n")
                continue
            
            dead_clients = set()
            with clients_lock:
                for client in client_connections.keys():
                    try:
                        client.sendall(data + b"\n")  
                    except:
                        dead_clients.add(client)
                
                for client in dead_clients:
                    disconnect_client(client, "Message send failed")
    
    except Exception as e:
        print(f"Error with {addr} ({username}): {e}")
    
    finally:
        disconnect_client(conn, "Client disconnected")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    print(f"Server listening on {HOST}:{PORT}...")
    
    while True:
        conn, addr = s.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn, addr))
        client_thread.daemon = True
        client_thread.start()