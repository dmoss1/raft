import rpyc
import sys
import random
import time
import threading
import os
import queue
from collections import defaultdict
from threading import Timer

'''
A RAFT RPC server class.
Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader ElectionStart part of the RAFT
protocol.
'''
    #python3 process.py config.txt 0 5001
    #python3 process.py config.txt 1 5002
    #python3 process.py config.txt 2 5003
    #python3 process.py config.txt 3 5004
    #python3 process.py config.txt 4 5005

class RaftNode(rpyc.Service):
    """
    Initialize the class using the config file provided and also initialize
    any datastructures you may need.
    """
    def __init__(self, config, ID):
        self.lock = threading.Lock()
        self.hostMap={}
        self.ID = int(ID)-1
        f = open(config)
        for l in f.readlines():
            if "node" in l:
                l = l.split()
                i = int(l[0][-2])
                p = tuple(l[1].split(':'))
                self.hostMap[i]=p
        f.close()
        self.ElectionStart_timeout = random.randint(600, 1000)
        self.heartbeat_timeout = 200
        self.time = time.time()
        self.votedFor = defaultdict(bool)
        self.currentTerm = 0
        self.state = "Follower"
        #Every nodes begins in the follower state
        self.vote = 0
        try:
            if os.path.exists('/tmp/xxy_log_%d'%self.ID):
                self.recover('/tmp/xxy_log_%d'%self.ID)
            if os.path.exists('/tmp/xxy_vote_%d'%self.ID):
                self.recover('/tmp/xxy_vote_%d'%self.ID)
        except:
            pass
        self.elect_event = threading.Event()
        self.elect_event.set()
        self.heartbeat_event = threading.Event()
        self.elect_timer = threading.Timer(self.ElectionStart_timeout/1000.0, self.ElectionStart)
        self.elect_timer.setDaemon(True)
        self.elect_timer.start()

    def ElectionStart(self):
        #The ElectionStart timeout is the amount of time a follower
        #waits until becoming a candidate
        self.elect_timer = Timer(self.ElectionStart_timeout/1000, self.ElectionStart)
        self.elect_timer.setDaemon(True)
        self.elect_timer.start()
        # while not self.elect_event.is_set():
        #     self.elect_event.wait(0.1)
        if self.state=='Follower' or 'Candidate':
            q = queue.Queue()
            vote=1
            self.state = "Candidate"
            print ("Candidate ", self.ID)
            #If followers don't hear from a leader then they become a candidate
            self.currentTerm+=1
            self.votedFor[self.currentTerm]=True
            self.store()
            print("Voting start.")
            #The candidate will then request votes from other nodes
            threads = []
            for host in self.hostMap:
                #print ("Here")
                if host==self.ID:
                    continue
                t = threading.Thread(target = self.RequestVote, args = (host,q))
                t.setDaemon(True)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()

            #Nodes will reply with their vote
            while vote<0.5*len(self.hostMap) and self.state=='Candidate':
                while not q.empty():
                    message=q.get()
                    if message[1]==0:
                        vote+=1
                    elif message[1]>0:
                        self.currentTerm = message[1]
                        vote = 0
                        self.state = 'Follower'
                        print ("Follower")
                        self.store()
                        break
                break
            #Candidate becomes the leader if it gets votes from a majority of nodes
            if vote>0.5*len(self.hostMap) and self.state=='Candidate':
                self.elect_timer.cancel()
                self.elect_event.clear()
                self.heartbeat_event.set()
                self.state="Leader"
                print("I'm leader now")
                self.HeartBeat()
                #HeartBeat call

    #Heartbeat (self):
        #initialize and start hb timer -- Timer(hb timeout /1000, hb), setDaemon
        #start = time.time()
        #if self state == leader
            # initialize threads and queue
            # for host in self.hostMap:
                # if host==self.ID: continue
                # t = threading.Thread(target = self.AppendEntries, args=(host, q))
                # t.setDaemon(True)
                # t.start()
                # threads.append(t)

            # while self state == leader and time - start is less than (self hb timeout / 1000):
                # while queue not empty and time - start is less than (self hb timeout / 1000):
                    # message = q.get()
                    # if message[1] == 0:
                        # do nothing
                    # elif message[1] > 0:
                        # set current term
                        # self state == follower
                        # cancel hb timer
                        # set ElectionStart timer -- threading.Timer(self.ElectionStart_timeout/1000, self.ElectionStart)
                        # set Daemon and start ElectionStart timer

    def HeartBeat(self):
        self.hb_timer = Timer(self.heartbeat_timeout/1000, self.HeartBeat)
        self.hb_timer.setDaemon(True)
        self.hb_timer.start()
        start = time.time()
        #Leader begins sending out Append Entries messages to its followers
        if self.state=='Leader':
            threads = []
            q = queue.Queue()
            for host in self.hostMap:
                if host==self.ID: continue
                t = threading.Thread(target = self.AppendEntries, args=(host, q))
                t.setDaemon(True)
                t.start()
                threads.append(t)
            #for t in threads:
               # t.join()

            #Messages are sent in intervals specified by the heartbeat timeout
            while self.state=='Leader' and time.time()-start<self.heartbeat_timeout/1000:
                #Followers respond to each Append Entries message
                while not q.empty() and time.time()-start<self.heartbeat_timeout/1000:
                    message=q.get()
                    if message[1]==0:
                        continue
                    elif message[1]>0:
                        self.currentTerm = message[1]
                        self.state = 'Follower'
                        print ("Follower ", self.ID)
                        #ElectionStart process continues until follower no longer detects heartbeat
                        self.hb_timer.cancel()
                        self.elect_timer = threading.Timer(self.ElectionStart_timeout/1000, self.ElectionStart)
                        self.elect_timer.setDaemon(True)
                        self.elect_timer.start()
                        break

    def exposed_RequestVote(self, term, candidateID):
        print ("here")
        with self.lock:
            print(candidateID, " is asking for vote")
            if term<self.currentTerm:
                print("Stale term ask for vote")
                message = "Stale %d"%self.currentTerm
                # t = threading.Thread(target=self.send_message,args=(message, candidateID))
                # t.start()
                return (False, self.currentTerm)
            elif (not self.votedFor[term] and self.state=='Follower') or (term>self.currentTerm and self.state!='Leader'):
                # message = "Vote"
                # t = threading.Thread(target=self.send_message,args=(message, candidateID))
                # t.start()

                self.currentTerm = term
                # self.ElectionStart_timeout = random.randrange(1200, 2000)
                self.votedFor[term]=True
                self.elect_timer.cancel()
                self.elect_event.set()
                self.state='Follower'
                print ("Follower")
                # self.heartbeat_event.clear()
                # self.elect_event.set()
                self.store()
                self.elect_timer = Timer(self.ElectionStart_timeout/1000, self.ElectionStart)
                self.elect_timer.setDaemon(True)
                self.elect_timer.start()
                print("Vote for ", candidateID, " | Term ", term)
                return (True, 0)
            else:
                print("Already vote at term: ", term)
                return (False, -1)

    def exposed_AppendEntries(self, term, leaderID):
        with self.lock:
            if term<self.currentTerm:
                print("Stale leader %d"%self.currentTerm)
                message = "Stale leader %d"%self.currentTerm
                # t = threading.Thread(target=self.send_message,args=(message, leaderID))
                # t.start()
                return (False, self.currentTerm)
            elif self.state!="Leader":
                message = "Got"
                # t = threading.Thread(target=self.send_message,args=(message, leaderID))
                # t.start()
                self.currentTerm=term
                # self.ElectionStart_timeout = random.randrange(1200, 2000)
                self.state = "Follower"
                print ("Follower")
                vote = 0
                self.elect_timer.cancel()
                # self.heartbeat_event.clear()
                self.elect_event.set()
                self.elect_timer = Timer(self.ElectionStart_timeout/1000, self.ElectionStart)
                self.elect_timer.setDaemon(True)
                self.elect_timer.start()
                self.store()
                print("Receive from leader ", leaderID)
                return (True, 0)
    
    #RPYC calls to put the exposed_ functions into the Queue
    def RequestVote(self, host, q):
        try:
            #print ("here")
            conn = rpyc.connect(self.hostMap[host][0], port = int(self.hostMap[host][1]))
            #print ("here")
            q.put(conn.root.exposed_RequestVote(self.currentTerm, self.ID))
            #conn.close()
        except Exception as e: 
            print(e)
            return

    def AppendEntries(self, host, q):
        try:
            conn = rpyc.connect(self.hostMap[host][0], port = int(self.hostMap[host][1]))
            q.put(conn.root.exposed_AppendEntries(self.currentTerm, self.ID))
            #conn.close()
        except Exception as e:
            print(e)
            return

    #Storing node values for later reference
    def recover(self, fname):
        f = open(fname)
        if 'vote' in fname:
            l = f.read()
            self.votedFor=eval(l)
        if 'log' in fname:
            l = f.read()
            l = l.strip().split()
            self.currentTerm = int(l[1])
        f.close()

    def store(self):
        f = open('/tmp/xxy_log_%d'%self.ID, 'w')
        f.write('term '+str(self.currentTerm)+'\n')
        f.flush()
        os.fsync(f.fileno())
        f.close()
        f = open('/tmp/xxy_vote_%d'%self.ID, 'w')
        f.write(str(self.votedFor))
        f.flush()
        os.fsync(f.fileno())
        f.close()

    '''
    x = is_leader(): returns True or False, depending on whether
    this node is a leader
    
    As per rpyc syntax, adding the prefix 'exposed_' will expose this
    method as an RPC call
    
    CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''
    def exposed_is_leader(self):
        return self.state == "Leader"

    # def exposed_send_message(self, message):
    #     if message=="Vote" and self.state=='Candidate':
    #         with self.lock:
    #             self.vote+=1
    #     elif "Stale" in message:
    #         with self.lock:
    #             self.currentTerm=int(message.strip().split()[-1])
    #             self.vote = 0
    #             self.state="Follower"
    #             self.heartbeat_event.clear()
    #             self.elect_event.set()
    #             self.timer.cancel()
    #         self.store()
    #         self.timer = Timer(self.ElectionStart_timeout/1000, self.ElectionStart)
    #         self.timer.setDaemon(True)
    #         self.timer.start()
    #
    # def send_message(self, message, candidateID):
    #     try:
    #         conn = rpyc.connect(host = self.hostMap[candidateID][0], port = self.hostMap[candidateID][1])
    #         conn.root.exposed_send_message(message)
    #     except:
    #         if 'leader' in message:
    #             self.ElectionStart()
    #         else:
    #             pass
            
        
	#if __name__ == '__main__':
	#   if len(sys.argv) != 4:
	#       print ("Arguments invalid")
	#       sys.exit(1)
	#   config = os.getcwd() + "/" + sys.argv[1]
	#   port = sys.argv[2]
	#   node = sys.argv[3]
	#   print (config + " port number - " + port + " node - " + node)
	#   from rpyc.utils.server import ThreadPoolServer
	#   server = ThreadPoolServer(RaftNode(config, node), port) 
	#   server.start()

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print ("Arguments invalid")
        sys.exit(1)
    from rpyc.utils.server import ThreadedServer
    server = ThreadedServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
    server.start()
    
    #arguments -- "config.txt" "specific node #" "matching port number"
    #python3 process.py config.txt 0 5001
    #python3 process.py config.txt 1 5002
    #python3 process.py config.txt 2 5003
    #python3 process.py config.txt 3 5004
    #python3 process.py config.txt 4 5005