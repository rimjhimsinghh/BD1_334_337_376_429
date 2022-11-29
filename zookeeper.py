HBPORT = 43278
CHECKWAIT = 30

BROKER1_PORT = 4456 # Default Leader
BROKER2_PORT = 4457
BROKER3_PORT = 4458

from socket import socket, gethostbyname, gethostname, AF_INET, SOCK_DGRAM
from threading import Lock, Thread, Event
from time import time, ctime, sleep
import sys


class ZookeeperLog:
    # "Manages Zookeeper Logs"
    def __init__(self):
        self.log = {
            ('192.168.44.1', BROKER1_PORT): True,
            ('192.168.44.1', BROKER2_PORT): False,
            ('192.168.44.1', BROKER3_PORT): False,
        }
        self.noLeader = True

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
    print("Initialized")

    if __debug__:
        print(beatRecThread)
    beatRecThread.start()
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
                        beatRecThread.send("You are the new Leader", newLeader)

            if running:
                print("Running Brokers")
                print(f"{running}")

            
            sleep(CHECKWAIT)
        except KeyboardInterrupt:
            print("Exiting.")
            beatRecGoOnEvent.clear()
            beatRecThread.goOnEvent.clear()
            print("In Join")
            beatRecThread.join()
            print("Out Join")

            

           
if __name__ == '__main__':
    main()

