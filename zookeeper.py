HBPORT = 43278
CHECKWAIT = 30
LPORT = 8000

BROKER1_PORT = 4456 # Default Leader
BROKER2_PORT = 4457
BROKER3_PORT = 4458

RW1_PORT = 9091 # Default Leader
RW2_PORT = 9092
RW3_PORT = 9093



from socket import socket, gethostbyname, gethostname, AF_INET, SOCK_DGRAM, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Lock, Thread, Event
from time import time, ctime, sleep
import sys
from multiprocessing import Process
import json

class ZookeeperLog:
    # "Manages Zookeeper Logs"
    def __init__(self, path=''):
        self.path = path
        self.host = gethostbyname(gethostname())
        self.log = {
            (self.host, BROKER1_PORT): True,
            (self.host, BROKER2_PORT): False,
            (self.host, BROKER3_PORT): False,
        }
        self.producerPort = {
            (self.host, BROKER1_PORT): RW1_PORT,
            (self.host, BROKER2_PORT): RW2_PORT,
            (self.host, BROKER3_PORT): RW3_PORT, 
        }

    def addLeader(self, entry):
        self.log[entry] = True

    def addFollower(self, entry):
        self.log[entry] = False
    
    def updateLog(self, entry, value):
        self.log[entry] = value

    def getLeader(self):
        # Returns Leader ip and port
        for key in self.log:
            if self.log[key]:
                return key
        return None
    
    def dumpLog(self):
        with open(f'{self.path}ZookeeperLogs.txt', 'a') as fptr:
            fptr.write(str(self.log))
            fptr.write('\n')
            fptr.write(str(self.producerPort))
            fptr.write('\n\n')
            # fptr.write('\n')


    def electLeader(self, running):
        currentLeader = self.getLeader()
        newLeader = None
        if running:
            for key in self.log:
                if key == currentLeader:
                    self.addFollower(key)
            newLeader = running[0]
            self.addLeader(newLeader)
        else:
            print("No Brokers Alive For Electing A Leader")
        self.dumpLog()
        return newLeader

       

class BeatDict:
    # "Manage heartbeat dictionary"
    def __init__(self):
        self.beatDict = {}
        self.dictLock = Lock()

    def __repr__(self):
        list = ''
        self.dictLock.acquire()
        for key in self.beatDict.keys():
            list = f"{list} IP address: {key} - Last time: {ctime(self.beatDict[key])}\n"
        self.dictLock.release()
        return list

    def update(self, entry):
        "Create or update a dictionary entry"
        self.dictLock.acquire()
        self.beatDict[entry] = time()
        self.dictLock.release()

    def extractSilent(self, howPast):
        "Returns a list of entries older than howPast"
        silent = [('192.168.44.1', BROKER1_PORT), ('192.168.44.1', BROKER2_PORT), ('192.168.44.1', BROKER3_PORT)]
        running = list()
        when = time() - howPast
        self.dictLock.acquire()
        for key in self.beatDict.keys():
            if self.beatDict[key] > when:
                silent.remove(key)
                running.append(key)
        self.dictLock.release()
        return silent, running
    
    def returnLength(self):
        "Returns length of Beat Dict"
        self.dictLock.acquire()
        length = len(self.beatDict)
        self.dictLock.release()
        return length
    
    def delete(self, entry):
        "Deletes an entry in Beat Dict"
        self.dictLock.acquire()
        del self.beatDict[entry]
        self.dictLock.release()

    
# new_socket = socket.socket()
# host_name = socket.gethostname() # pes1ug20cs334
# print(f"hostname is {host_name}")
# s_ip = socket.gethostbyname(host_name) # IP address
# port = 8080
# new_socket.bind((host_name, port))

class BeatRec(Thread):
    # "Receive UDP packets, log them in heartbeat dictionary"
    def __init__(self, goOnEvent, updateDictFunc, port):
        Thread.__init__(self)
        self.goOnEvent = goOnEvent
        self.updateDictFunc = updateDictFunc
        self.port = port
        self.recSocket = socket(AF_INET, SOCK_DGRAM)
        self.hostName = gethostname()
        self.recSocket.bind((gethostname(), port))
        self.s_ip = gethostbyname(gethostname())

    def __repr__(self):
        return f"Heartbeat Server on IP: {self.s_ip} port: {self.port}\n"

    def run(self):
        while self.goOnEvent.is_set():
            if __debug__:
                print("Waiting to receive...")
            data, addr = self.recSocket.recvfrom(6)
            if __debug__:
                print(f"Received packet from {addr}")
            self.updateDictFunc(addr)

    def send(self, msg, address):
        self.recSocket.sendto(f'{msg}'.encode('utf-8'), address)
            


def giveLeader(port, lport, rwPort):
    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(1)
    print(f'Listening on port {port} ... to return leader port')

    while True:    
        # Wait for client connections
        print("Waiting for connections")
        client_connection, client_address = server_socket.accept()

        # Get the client request
        request = client_connection.recv(1024).decode()
        print(request)

        # Send HTTP response
        responseDict = {
            lport: rwPort
        }
        response = f'HTTP/1.0 200 OK\n\n{responseDict}'
        client_connection.sendall(response.encode())
        client_connection.close()

        


def main():
    # "Listen to the heartbeats and detect inactive clients"
    global HBPORT, CHECKWAIT
    if len(sys.argv)>1:
        HBPORT=sys.argv[1]
    if len(sys.argv)>2:
        CHECKWAIT=sys.argv[2]
    
    beatRecGoOnEvent = Event()
    beatRecGoOnEvent.set()
    beatDictObject = BeatDict()
    beatRecThread = BeatRec(beatRecGoOnEvent, beatDictObject.update, HBPORT)
    zooLogs = ZookeeperLog()
    leaderSocket = Process(target=giveLeader, args=(LPORT, zooLogs.getLeader(), zooLogs.producerPort[zooLogs.getLeader()]))
    print("Initialized")

    if __debug__:
        print(beatRecThread)
    beatRecThread.start()
    if __name__ == '__main__':
        leaderSocket.start()
    print(f"PyHeartBeat server listening on IP: {beatRecThread.s_ip} Port: {HBPORT}")
    print ("\n*** Press Ctrl-C to stop ***\n")
    while True:
        try:
            # if __debug__:
            print("Beat Dictionary")
            print(f"{beatDictObject}")
            silent, running = beatDictObject.extractSilent(CHECKWAIT)

            if silent:
                print("Silent Brokers")
                print(f"{silent}")
                if zooLogs.getLeader() in silent:
                    newLeader = zooLogs.electLeader(running)
                    if newLeader:
                        # beatRecThread.send("You are the new Leader", newLeader)
                        leaderSocket.terminate()
                        leaderSocket = Process(target=giveLeader, args=(LPORT, zooLogs.getLeader(), zooLogs.producerPort[zooLogs.getLeader()]))
                        leaderSocket.start()


            if running:
                print("Running Brokers")
                print(f"{running}")

            
            sleep(CHECKWAIT)
        except KeyboardInterrupt:
            print("Exiting.")
            beatRecGoOnEvent.clear()
            beatRecThread.goOnEvent.clear()
            leaderSocket.terminate()
            print("In Join")
            beatRecThread.join()
            print("Out Join")

            

           
if __name__ == '__main__':
    main()

