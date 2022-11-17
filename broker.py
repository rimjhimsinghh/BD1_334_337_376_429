
import os   
import socket   
import threading    
import hashlib
import signal


IP = socket.gethostbyname(socket.gethostname())


PORT = 4456
ADDR = (IP, PORT)       
SIZE = 1024
FORMAT = "utf-8"
PRODUCER_DATA_PATH="DATA"  




def handle_client(conn, addr):
        topicName = conn.recv(SIZE).decode(FORMAT)
        print(f"[NEW CONNECTION] {addr} connected. [TOPIC] {topicName} ")
        while True: 
            data = conn.recv(SIZE).decode(FORMAT)
            if data =="LOGOUT":
                break

            filepath = os.path.join(PRODUCER_DATA_PATH, topicName)
            with open(f"{filepath}/broker1.txt", "a+") as f: 
                    f.write(data)
                    f.close()
                
        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()
        print("Connection is closed with port: ", addr[1])

    

def main(): #create server
    print("[STARTING]\nServer is starting")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) ##
    server.bind(ADDR) #bind IP and port
    print("Binding Successful!")
    print("This is your IP: ", IP)
    server.listen() 
    print(f"[LISTENING] Server is listening on {IP}  PORT: {PORT}.")

    while True:
        conn, addr = server.accept()
        print("Address of client on network: ",addr)
        thread = threading.Thread(target = handle_client, args=(conn, addr))
        thread.start()
        print("[ACTIVE CONNECTIONS]")
        

if __name__ == "__main__":
    main()
