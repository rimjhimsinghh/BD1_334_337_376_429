
import os   
import socket   
import threading    
import hashlib

IP = socket.gethostbyname(socket.gethostname())


PORT = 4456
ADDR = (IP, PORT)       
SIZE = 1024
FORMAT = "utf-8"    

def handle_client(conn, addr):
        print(f"[NEW CONNECTION] {addr} connected.")
        while True: 
            data = conn.recv(SIZE).decode()
            if data =="LOGOUT":
                break
            else:
                print(data)

                
        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()
        print("Connection is closed with port: ", addr[1])
        # return
    

def main(): #create server
    print("[STARTING]\nServer is starting")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) ##
    server.bind(ADDR) #bind IP and port
    print("Binding Successful!")
    print("This is your IP: ", IP)
    server.listen() 
    print(f"[LISTENING] Server is listening on {IP}  PORT: {PORT}.")
    #wait for client to connect (Create an infinite loop)

    while True:
        conn, addr = server.accept()
        print("Address of client on network: ",addr)
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print("[ACTIVE CONNECTIONS]")
        

if __name__ == "__main__":
    main()
