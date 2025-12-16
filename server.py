import socket
import json
import threading
from threading import Lock

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]
TOKEN = config["token"]

authenticated_clients = set()
clients_lock = Lock()  

def handle_client(conn, addr):
    print(f"Attempt from {addr}")
    
    try:
        auth_data = conn.recv(1024).decode("utf-8").strip()

        if auth_data != f"AUTH {TOKEN}":
            print(f"Auth failed for {addr}")
            conn.close()
            return

        print(f"Auth success for {addr}")
        
        with clients_lock:
            authenticated_clients.add(conn)

        while True:
            data = conn.recv(4096)
            if not data:
                break  

            if not data.startswith(b"MESSAGE "):
                conn.sendall(b"ERROR Invalid message")
                continue

            dead_clients = set()
            with clients_lock:
                for client in authenticated_clients:
                    try:
                        client.sendall(data)
                    except:
                        dead_clients.add(client)
                
                for client in dead_clients:
                    authenticated_clients.discard(client)

    except Exception as e:
        print(f"Error with {addr}: {e}")

    finally:
        with clients_lock:
            authenticated_clients.discard(conn)
        conn.close()
        print(f"{addr} disconnected")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print("Server listening...")

    while True:
        conn, addr = s.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn, addr))
        client_thread.daemon = True   
        client_thread.start()