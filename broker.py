import os
import socket
import threading

import time
from json import load, dump
import multiprocessing
import sys
import requests
import datetime
IP = socket.gethostbyname(socket.gethostname())


# PORT = 9091
# ADDR = (IP, PORT)
SIZE = 1024
FORMAT = "utf-8"
PRODUCER_DATA_PATH = "DATA"
SERVERLOGS = "serverlogs"

# Constants for heartbeat
SERVERIP = socket.gethostbyname(socket.gethostname())  # local host, just for testing
HBPORT = 43278             # an arbitrary UDP port
BEATWAIT = 10              # number of seconds between heartbeats


def handle_client(conn, addr):
    log = ''
    topicName = conn.recv(SIZE).decode(FORMAT)
    log+= f"Connection Received. Topic Name: {topicName} " + "\n"
    pythonName = conn.recv(SIZE).decode(FORMAT)
    # print(f"FIle Type: {pythonName}")
    files = os.listdir(PRODUCER_DATA_PATH)

    if topicName not in files and pythonName == "producer.py":
        # Creates a folder for the topic
        filepath = os.path.join(PRODUCER_DATA_PATH, topicName)
        os.mkdir(filepath)
        with open(f"{filepath}/{topicName}logs.txt", 'w') as f:
            final_dict = {'p': 0}
            dump(final_dict, f)
            f.close()
        print(topicName + " created.")
    x = f"[NEW CONNECTION] {addr} connected. [TOPIC] {topicName} " 
    log+=x + "\n"
    print(x)

    if pythonName == "producer.py":
        while True:
            data = conn.recv(SIZE).decode(FORMAT)
            # print(data)
            if data == "LOGOUT":
                break

            filepath = os.path.join(PRODUCER_DATA_PATH, topicName)
            with open(f"{filepath}/{topicName}logs.txt", 'r') as f:
                d = load(f)
                p = d['p']
                f.close()
            with open(f"{filepath}/partition{p}.txt", "a+") as f:
                # timestr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                f.write(data)
                p = (p+1) % 3
                log+=f"{data} --moved to file: partition{p}" + "\n"
                f.close()
            with open(f"{filepath}/{topicName}logs.txt", 'w') as f:
                final_dict = {'p': p}
                dump(final_dict, f)
                f.close()
        x = f"[DISCONNECTED] {addr} disconnected"
        log+=x + "\n"
        print(x)
        conn.close()
        x = f"Connection is closed with port: {addr[1]}"
        log+=x + "\n"
        print(x)

        with open(f"serverlogs/broker1log.txt", 'a+') as f:
            log+="\n\n"
            f.write(log)
            log = ''
            f.close()
    


    if pythonName == "consumer.py":
        flag = conn.recv(SIZE).decode(FORMAT)
        prev = 0
        pres = 0
        while True:
            filepath = os.path.join(PRODUCER_DATA_PATH, topicName)
            # print("Connected")
            data = conn.recv(SIZE).decode(FORMAT)
            if data =="LOGOUT":
                break
            # print("Printing files")
            files = os.listdir(f"{PRODUCER_DATA_PATH}/{topicName}")

            files = os.listdir(f"{PRODUCER_DATA_PATH}/{topicName}")
            files.remove(f"{topicName}logs.txt")
            files.sort()
            # print(files)
            no_of_files = len(files)
            # print(no_of_files)
            filedata = list()
            for i in range (no_of_files):

                with open(f"{PRODUCER_DATA_PATH}/{topicName}/{files[i]}", "r") as f:
                    lines = f.readlines()

                    filedata.extend(lines)
                    f.close()
            # fileDATA = [['','','',...],[],[]] => []
            # final_list = Merge(filedata)
            pres = len(filedata)
            if flag == '--from-beginning':
                conn.send(str(filedata).encode(FORMAT))
                flag = None
            else: 
                conn.send(str(filedata[prev: pres]).encode(FORMAT))
            prev=pres 
            time.sleep(5)
        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()
        print("Connection is closed with port: ", addr[1])


def main(): 
    print("[STARTING]\nServer is starting")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)  # bind IP and port
    print("Binding Successful!")
    print("This is your IP: ", IP)
    server.listen()
    print(f"[LISTENING] Server is listening on {IP}  PORT: {PORT}.")

    files = os.listdir('.')

    if PRODUCER_DATA_PATH not in files:
        os.mkdir(PRODUCER_DATA_PATH) # Make a directory for all topics
    if  SERVERLOGS not in files:
        os.mkdir(SERVERLOGS)


    while True:
        conn, addr = server.accept()
        print("Address of client on network: ", addr)
        # thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread = multiprocessing.Process(target=handle_client, args=(conn, addr))
        thread.start()
        print("[ACTIVE CONNECTIONS]")

def sendBeat(my_port):
    hbsocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    hbsocket.bind((socket.gethostname(), my_port))
    print(f"PyHeartBeat client sending to IP {SERVERIP} , port {HBPORT}")
    print("\n*** Press Ctrl-C to terminate ***\n")
    while 1:
        hbsocket.sendto('XXXXXX'.encode('utf-8'), (SERVERIP, HBPORT))
        if __debug__:
            print(f"Time: {time.ctime(time.time())}")
        time.sleep(BEATWAIT)


if __name__ == "__main__":
    my_port = int(sys.argv[1])
    PORT = int(sys.argv[2])
    ADDR = (IP, PORT)
    # PRODUCER_DATA_PATH = "BROKER"
    p = multiprocessing.Process(target=sendBeat, args=(my_port, ))
    p.start()
    print("Waiting for 60 seconds")
    time.sleep(60)
    # res = requests.get('http://127.0.1.1:8080/')
    
    flag = True
    while flag:
        res = requests.get('http://localhost:8000/')
        response_dict = eval(res.text)
        print(response_dict)
        for key in response_dict:
            if key[1] == my_port and response_dict[key] == PORT:
                flag = False

    main()