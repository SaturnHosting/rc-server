import socket
import json

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

HOST = config["host"]
PORT = config["port"]
TOKEN = config["token"]

authenticated_clients = set()

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print("Server listening...")

    while True:
        conn, addr = s.accept()
        print(f"Attempt from {addr}")

        try:
            auth_data = conn.recv(1024).decode("utf-8").strip()

            if auth_data != f"AUTH {TOKEN}":
                print(f"Auth failed for {addr}")
                conn.close()
                continue

            print(f"Auth success for {addr}")
            authenticated_clients.add(conn)

            while True:
                data = conn.recv(1024)
                if not data:
                    break  

                if not data.startswith(b"MESSAGE "):
                    conn.sendall(b"ERROR Invalid message")
                    continue

                dead_clients = set()
                for client in authenticated_clients:
                    try:
                        client.sendall(data)
                    except:
                        dead_clients.add(client)

                authenticated_clients -= dead_clients

        except Exception as e:
            print(f"Error with {addr}: {e}")

        finally:
            authenticated_clients.discard(conn)
            conn.close()
            print(f"{addr} disconnected")
