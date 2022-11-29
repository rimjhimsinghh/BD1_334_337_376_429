
import os
import socket
import threading
import hashlib
import signal
import time
from json import load, dump
IP = socket.gethostbyname(socket.gethostname())


PORT = 4456
ADDR = (IP, PORT)
SIZE = 1024
FORMAT = "utf-8"
PRODUCER_DATA_PATH = "DATA"


def handle_client(conn, addr):
    topicName = conn.recv(SIZE).decode(FORMAT)
    files = os.listdir(PRODUCER_DATA_PATH)

    if topicName not in files:
        # Creates a folder for the topic
        filepath = os.path.join(PRODUCER_DATA_PATH, topicName)
        os.mkdir(filepath)
        with open(f"{filepath}/{topicName}logs.txt", 'w') as f:
            final_dict = {'p': 0}
            dump(final_dict, f)
            f.close()
        print(topicName + " created.")

    print(f"[NEW CONNECTION] {addr} connected. [TOPIC] {topicName} ")
    while True:
        data = conn.recv(SIZE).decode(FORMAT)
        if data == "LOGOUT":
            break

        filepath = os.path.join(PRODUCER_DATA_PATH, topicName)
        with open(f"{filepath}/{topicName}logs.txt", 'r') as f:
            d = load(f)
            p = d['p']
            print(p)
            f.close()
        with open(f"{filepath}/partition{p}.txt", "a+") as f:
            f.write(data)
            p = (p+1) % 3
            # topicname.txt in each topic folder
            f.close()
        with open(f"{filepath}/{topicName}logs.txt", 'w') as f:
            final_dict = {'p': p}
            dump(final_dict, f)
            f.close()

    print(f"[DISCONNECTED] {addr} disconnected")
    conn.close()
    print("Connection is closed with port: ", addr[1])


def main():  # create server
    print("[STARTING]\nServer is starting")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)  # bind IP and port
    print("Binding Successful!")
    print("This is your IP: ", IP)
    server.listen()
    print(f"[LISTENING] Server is listening on {IP}  PORT: {PORT}.")

    files = os.listdir('.')  # Make a directory for all topics
    # print(files)
    if PRODUCER_DATA_PATH not in files:
        os.mkdir(PRODUCER_DATA_PATH)

    while True:
        conn, addr = server.accept()
        print("Address of client on network: ", addr)
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print("[ACTIVE CONNECTIONS]")


if __name__ == "__main__":
    main()
