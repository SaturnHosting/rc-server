import socket
import json

with open("config.json", "r", encoding="utf-8") as f:
    data = json.load(f)

HOST = data["host"]
PORT = data["port"]
TOKEN = data["token"]

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print("Server listening...")

    while True: 
        conn, addr = s.accept()
        with conn:
            print(f"Connect from {addr}")
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                conn.sendall(data)
            print(f"{addr} disconnected.")