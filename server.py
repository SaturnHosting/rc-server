import socket
import json

with open("config.json", "r", encoding="utf-8") as file:
    data = json.load(file)

HOST = data["host"]
PORT = data["port"]
TOKEN = data["token"]

