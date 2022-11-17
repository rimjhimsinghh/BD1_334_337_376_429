import socket
import os
socket_server = socket.socket()
server_host = socket.gethostname()
IP = socket.gethostbyname(server_host)
IP = socket.gethostbyname(socket.gethostname())
PORT = 4456
ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 1024


print("This is your IP: ", IP)
socket_server.connect((IP, PORT))


login = True
# if(response=="FAIL"):
#     login = False


while login:
        data = input("> ")
        if data =="LOGOUT":
            print("[DISCONNECTED]\n")
            # break 
            login = False
        socket_server.send(data.encode(FORMAT))        
            

print("Disconnected from the server.")
socket_server.close()

