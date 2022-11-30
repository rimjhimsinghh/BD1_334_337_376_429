import socket
import os
import sys
import time
from datetime import datetime
import requests

socket_server = socket.socket()
server_host = socket.gethostname()
IP = socket.gethostbyname(server_host)
IP = socket.gethostbyname(socket.gethostname())

# PORT = 4456
# ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 1024
PRODUCER_DATA_PATH = "DATA"

res = requests.get("http://localhost:8000/")
response_dict = eval(res.text)
print(response_dict)
for key in response_dict:
    IP = key[0]
    PORT = int(response_dict[key])
    ADDR = (IP, PORT)


topicName = sys.argv[1]
print("Topic Name: ", topicName)
print("This is your IP: ", IP)
socket_server.connect((IP, PORT))

socket_server.send(str.encode(topicName))
print("topic: ", topicName)
time.sleep(1)
socket_server.send(str.encode(os.path.basename(__file__)))
time.sleep(1)

flag = sys.argv[2] if len(sys.argv) > 2 else '#'
socket_server.send(str.encode(flag))

files = os.listdir(PRODUCER_DATA_PATH)
# print(files)
login = True
if topicName not in files:
    print(topicName + " does not exist.")
    login = False



while login:
    try:
        data = "@"
        socket_server.send(data.encode(FORMAT))
        response = socket_server.recv(SIZE)
        response = response.decode()
        if response != '[]':
            response = eval(response)
            for i in response:
                print(i.strip())
    except KeyboardInterrupt:
        data = "LOGOUT"
        socket_server.send(data.encode(FORMAT))
        print("Disconnected from the server.")
        # socket_server.close()
        login = False
    except ConnectionResetError:
        print("Connection Lost with Broker...Waiting for Broker")
        socket_server.close()
        time.sleep(60)  # Wait for the leader to get re elected
        print("Re initializing Connection with broker...")
        res = requests.get("http://localhost:8000/")
        response_dict = eval(res.text)
        print(response_dict)
        for key in response_dict:
            IP = key[0]
            PORT = int(response_dict[key])
            ADDR = (IP, PORT)
        socket_server = socket.socket()
        socket_server.connect((IP, PORT))
        socket_server.send(str.encode(topicName))
        time.sleep(1)
        socket_server.send(str.encode(os.path.basename(__file__)))
        socket_server.send(str.encode('#'))
        # socket_server.send(data.encode(FORMAT))
        continue
socket_server.close()