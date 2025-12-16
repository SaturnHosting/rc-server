import socket
import json
import threading
from threading import Lock

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]
TOKEN = config["token"]

client_connections = {} 
clients_lock = Lock()

def broadcast_message(message, exclude_conn=None):
    dead_clients = set()
    with clients_lock:
        for client in client_connections.keys():
            if client == exclude_conn:
                continue
            try:
                client.sendall(message.encode("utf-8"))
            except:
                dead_clients.add(client)
        
        for client in dead_clients:
            if client in client_connections:
                del client_connections[client]

def handle_client(conn, addr):
    print(f"Attempt from {addr}")
    username = None
    
    try:
        auth_data = conn.recv(1024).decode("utf-8").strip()

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
        
        broadcast_message(f"CONNECTION {username} joined\n", exclude_conn=conn)
        
        while True:
            data = conn.recv(4096)
            if not data:
                break
            
            data_str = data.decode("utf-8").strip()
            print(f"Received from {username}: {data_str}")
            
            if data_str == "ONLINE":
                print(f"Processing ONLINE request from {username}")
                with clients_lock:
                    online_players = list(client_connections.values())
                response = f"ONLINE {','.join(online_players)}\n"
                conn.sendall(response.encode("utf-8"))
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
                    if client in client_connections:
                        del client_connections[client]
    
    except Exception as e:
        print(f"Error with {addr} ({username}): {e}")
    
    finally:
        if username:
            broadcast_message(f"CONNECTION {username} left\n", exclude_conn=None)
            
        with clients_lock:
            if conn in client_connections:
                del client_connections[conn]
        conn.close()
        print(f"{addr} ({username}) disconnected")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print("Server listening...")
    
    while True:
        conn, addr = s.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn, addr))
        client_thread.daemon = True
        client_thread.start()