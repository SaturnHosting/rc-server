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


def disconnect_client(conn, reason="Disconnected"):
    username = None
    with clients_lock:
        username = client_connections.pop(conn, None)

        timer = client_keepalive_timers.pop(conn, None)
        if timer:
            try:
                timer.cancel()
            except:
                pass

        client_pending_responses.pop(conn, None)

    try:
        conn.close()
    except:
        pass

    print(f"Disconnected {username}: {reason}")

    if username:
        send_to_all_except(f"CONNECTION {username} left\n", conn)


def send_to_all_except(message, exclude):
    with clients_lock:
        clients = list(client_connections.keys())

    dead = []
    for c in clients:
        if c == exclude:
            continue
        try:
            c.sendall(message.encode())
        except:
            dead.append(c)

    for c in dead:
        disconnect_client(c, "Send failed")


def broadcast(message, exclude=None):
    with clients_lock:
        clients = list(client_connections.keys())

    dead = []
    for c in clients:
        if c == exclude:
            continue
        try:
            c.sendall(message.encode())
        except:
            dead.append(c)

    for c in dead:
        disconnect_client(c, "Broadcast failed")


def send_keepalive(conn):
    try:
        if conn.fileno() == -1:
            disconnect_client(conn)
            return
    except:
        disconnect_client(conn)
        return

    challenge = random.randint(1000, 9999)

    with clients_lock:
        client_pending_responses[conn][challenge] = time.time()
        old = client_keepalive_timers.pop(conn, None)
        if old:
            try:
                old.cancel()
            except:
                pass

    try:
        conn.sendall(f"KEEPALIVE {challenge}\n".encode())
    except:
        disconnect_client(conn, "Keepalive send failed")
        return

    t_check = Timer(KEEPALIVE_TIMEOUT, check_keepalive, args=(conn, challenge))
    t_check.daemon = True
    t_check.start()

    t_next = Timer(KEEPALIVE_INTERVAL, send_keepalive, args=(conn,))
    t_next.daemon = True
    with clients_lock:
        client_keepalive_timers[conn] = t_next
    t_next.start()


def check_keepalive(conn, challenge):
    with clients_lock:
        pending = client_pending_responses.get(conn)
        if not pending:
            return
        if challenge not in pending:
            return
        del pending[challenge]

    disconnect_client(conn, f"No keepalive response {challenge}")


def handle_keepalive_response(conn, challenge):
    try:
        challenge = int(challenge)
    except:
        return

    with clients_lock:
        pending = client_pending_responses.get(conn)
        if not pending:
            return
        pending.pop(challenge, None)


def handle_client(conn, addr):
    username = None
    try:
        conn.settimeout(10)
        auth = conn.recv(1024).decode().strip()
        conn.settimeout(None)

        if not auth.startswith("AUTH "):
            conn.close()
            return

        _, token, username = auth.split(maxsplit=2)

        if token != TOKEN or not username:
            conn.sendall(b"ERROR Invalid token\n")
            conn.close()
            return

        with clients_lock:
            if username in client_connections.values():
                conn.sendall(b"ERROR Username already taken\n")
                conn.close()
                return
            client_connections[conn] = username

        conn.sendall(b"AUTH_SUCCESS\n")

        t = Timer(KEEPALIVE_INTERVAL, send_keepalive, args=(conn,))
        t.daemon = True
        with clients_lock:
            client_keepalive_timers[conn] = t
        t.start()

        broadcast(f"CONNECTION {username} joined\n", conn)

        while True:
            data = conn.recv(4096)
            if not data:
                break

            msg = data.decode().strip()

            if msg == "ONLINE":
                with clients_lock:
                    users = ",".join(client_connections.values())
                conn.sendall(f"ONLINE {users}\n".encode())
                continue

            if msg.startswith("KEEPALIVE_RESPONSE "):
                handle_keepalive_response(conn, msg.split()[1])
                continue

            if not msg.startswith("MESSAGE "):
                conn.sendall(b"ERROR Invalid message\n")
                continue

            broadcast(msg + "\n", None)

    except:
        pass
    finally:
        disconnect_client(conn, "Client left")


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    print(f"Listening on {HOST}:{PORT}")

    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()
