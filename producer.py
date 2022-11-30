import socket
import os
import time
import sys
import requests

socket_server = socket.socket()
server_host = socket.gethostname()
IP = socket.gethostbyname(server_host)
IP = socket.gethostbyname(socket.gethostname())

# PORT = 9092
# ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 1024
# PRODUCER_DATA_PATH = "DATA" # not needed here

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
time.sleep(5)
socket_server.send(str.encode(os.path.basename(__file__)))


login = True

while login:
    try:
        data = input("> ")
        if data == "LOGOUT":
            print("[DISCONNECTED]\n")
            login = False
        else:
            data = data + "\n"
        socket_server.send(data.encode(FORMAT))
    except KeyboardInterrupt:
        data = "LOGOUT"
        socket_server.send(data.encode(FORMAT))
        socket_server.close()
        break
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
        socket_server.send(data.encode(FORMAT))
        continue




print("Disconnected from the server.")
socket_server.close()