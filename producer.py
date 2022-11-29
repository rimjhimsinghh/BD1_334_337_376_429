import socket
import os
import time
import sys

socket_server = socket.socket()
server_host = socket.gethostname()
IP = socket.gethostbyname(server_host)
IP = socket.gethostbyname(socket.gethostname())

PORT = 8181
ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 1024
PRODUCER_DATA_PATH = "DATA"


topicName = sys.argv[1]
print("Topic Name: ", topicName)
print("This is your IP: ", IP)
socket_server.connect((IP, PORT))

socket_server.send(str.encode(topicName))
time.sleep(1)
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


print("Disconnected from the server.")
socket_server.close()