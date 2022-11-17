import socket
import os
import sys
import signal
import time
socket_server = socket.socket()
server_host = socket.gethostname()
IP = socket.gethostbyname(server_host)
IP = socket.gethostbyname(socket.gethostname())
PORT = 4456
ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 1024
PRODUCER_DATA_PATH = "DATA"

files = os.listdir('.') # Make a directory for all topics
if PRODUCER_DATA_PATH not in files:
   os.mkdir(PRODUCER_DATA_PATH)


topicName = sys.argv[1]
print("Topic Name: ", topicName)
print("This is your IP: ", IP)
socket_server.connect((IP, PORT))

socket_server.send(str.encode(topicName))

files = os.listdir(PRODUCER_DATA_PATH)
# print(files)

if topicName not in files:
    filepath = os.path.join(PRODUCER_DATA_PATH, topicName)  #Creates a folder for the topic
    os.mkdir(filepath)
    print(topicName + " created.")


login = True
# if(response=="FAIL"):
#     login = False


while login:
        data = input("> ")
        if data =="LOGOUT":
            print("[DISCONNECTED]\n")
            # break 
            login = False
        else:
            data = data + "\n"
        socket_server.send(data.encode(FORMAT))        
            

print("Disconnected from the server.")
socket_server.close()

