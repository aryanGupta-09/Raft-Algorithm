import random
import time
import node_pb2
import node_pb2_grpc
import grpc
import threading
import os
from collections import defaultdict
from concurrent import futures

# Define constants
ELECTION_TIMEOUT_MIN = 5  # Minimum election timeout in seconds
ELECTION_TIMEOUT_MAX = 10  # Maximum election timeout in seconds
HEARTBEAT_INTERVAL = 1  # Heartbeat interval in seconds

node_id = None
current_term = 0
voted_for = None
logs = []
commit_length = 0
current_role = 'follower'
current_leader = None
election_timeout = None
election_timer_event = None
election_timer_thread = None

def generate_election_timeout():
    return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

def reset_election_timeout():
    election_timer_event.set()

def dump_state(message):
    global node_id
    
    print(f"dump_state called with node_id: {node_id}")
    directory = f'logs_node_{node_id}'
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    try:
        with open(f'{directory}/dump.txt', 'a') as f:
            f.write(f'{message}\n')
    except Exception as e:
        print(f"An error occurred while dumping the state")

def save_state(node_id):
    global logs, commit_length, voted_for, current_term

    print(f"save_state called with node_id: {node_id}")
    directory = f'logs_node_{node_id}'
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    try:
        with open(f'{directory}/logs.txt', 'w') as f:
            for log in logs:
                f.write(f'{log.command} {log.key} {log.value} {log.term}\n')
        
        with open(f'{directory}/metadata.txt', 'w') as f:
            f.write(f'Commit Length: {commit_length}, Term: {current_term}, Voted for: {voted_for}\n')

    except Exception as e:
        print(f"An error occurred while saving the state")

def load_state(node_id):
    global logs, commit_length, voted_for, current_term

    print(f"load_state called with node_id: {node_id}")
    directory = f'logs_node_{node_id}'
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return
    
    try:
        with open(f'{directory}/logs.txt', 'r') as f:
            logs = [node_pb2.LogRequest.LogItem(command=line.split()[0], key=line.split()[1], value=line.split()[2], term=int(line.split()[3])) for line in f.readlines()]
        
        with open(f'{directory}/metadata.txt', 'r') as f:
            metadata = f.read().split(',')
            commit_length = int(metadata[0].split(':')[1].strip())
            current_term = int(metadata[1].split(':')[1].strip())
            voted_for = metadata[2].split(':')[1].strip().lower() == 'True'
    
    except Exception as e:
        print(f"An error occurred while loading the state")
            

class NodeClient:
    def __init__(self, nodeId, server_adds):
        global node_id, election_timeout, election_timer_event, election_timer_thread

        load_state(nodeId)
        node_id = nodeId
        election_timer_event = threading.Event()
        election_timer_thread = threading.Thread(target=self.start_election_timer)
        election_timer_thread.start()
        self.server_adds = server_adds
        self.votes_received = set()
        self.sent_length = defaultdict(lambda: 0)
        self.acked_length = defaultdict(lambda: 0)
        self.channel = ""
            
    def start_election_timer(self):
        while True:
            election_timeout = generate_election_timeout()
            election_timer_event.wait(election_timeout)

            if election_timer_event.is_set():
                election_timer_event.clear()
            else:
                if current_role != 'leader':
                    self.start_election()

    def get_stub(self, node_id):
        self.channel = grpc.insecure_channel(self.server_adds[node_id])
        return node_pb2_grpc.NodeStub(self.channel)
        
    def start_election(self):
        global current_leader, current_role, voted_for, current_term, node_id, logs

        dump_state(f"Node {node_id} election timer timed out, Starting election.")
        current_term += 1
        current_role = 'candidate'
        voted_for = node_id
        self.votes_received = {node_id}
        last_term = 0 if not logs else logs[-1].term
        threads = []

        for nodeId in self.server_adds.keys():
            print("start_election 1:", current_role)
            if current_role != 'candidate':
                break

            if nodeId != node_id:
                print("start_election 2:", nodeId, current_term)
                thread = threading.Thread(target=self.request_vote, args=(nodeId, current_term, len(logs), last_term))
                threads.append(thread)
                thread.start()
        # wait for all threads to complete
        for thread in threads:
            thread.join()
    
    def request_vote(self, nodeId, current_term, log_length, last_term):
        try:
            request = node_pb2.VoteRequest(candidate_id=node_id, term=current_term, log_length=log_length, last_term=last_term)
            stub = self.get_stub(nodeId)
            response = stub.Vote(request)
            self.channel.close()
            self.collect_vote(response)

        except grpc.RpcError as e:
            print(f"Failed to connect to node {nodeId}")
    
    def collect_vote(self, response):
        global current_role, node_id, current_term, logs, voted_for, current_leader

        print("collect_vote 1:", response.vote_granted)
        if current_role == 'candidate' and response.term == current_term and response.vote_granted:
            self.votes_received.add(response.voter_id)

            if len(self.votes_received) >= (len(self.server_adds) + 1) // 2:
                current_role = 'leader'
                dump_state(f"Node {node_id} became the leader for term {current_term}")
                print(f"collect_vote 2: Node {node_id} became the leader for ", current_role)
                current_leader = node_id
                reset_election_timeout()
                for follower_id in self.server_adds.keys():
                    if follower_id != node_id:
                        self.sent_length[follower_id] = len(logs)
                        self.acked_length[follower_id] = 0
                self.start_heartbeat()

        elif response.term > current_term:
            current_term = response.term
            current_role = 'follower'
            print("collect_vote 3:", current_role)
            voted_for = None
            reset_election_timeout()
    
    def start_heartbeat(self):
        global current_role, node_id

        while current_role == 'leader':
            time.sleep(HEARTBEAT_INTERVAL)
            reset_election_timeout()
            print("start_heartbeat")
            threads = []

            for follower_id in self.server_adds.keys():
                if follower_id != node_id:
                    thread = threading.Thread(target=self.replicate_log, args=(follower_id,))
                    threads.append(thread)
                    thread.start()
            # wait for all threads to complete
            for thread in threads:
                thread.join()
        else:
            dump_state(f"{node_id} Stepping down")
    
    def replicate_log(self, follower_id):
        global logs, node_id, current_term, commit_length

        print("replicate_log:", follower_id)
        prefixLen = self.sent_length[follower_id]
        suffix = logs[prefixLen:]
        prefixTerm = 0

        if self.sent_length[follower_id] > 0:
            prefixTerm = logs[self.sent_length[follower_id] - 1].term

        request = node_pb2.LogRequest(leader_id=node_id, term=current_term, prefixLen=prefixLen,
                                       prefixTerm=prefixTerm, leaderCommit=commit_length,
                                       suffix=suffix, follower_id=follower_id)
        try:
            stub = self.get_stub(follower_id)
            response = stub.Log(request)
            self.channel.close()
            print("replicate_log 2-",follower_id)
            self.process_log_response(follower_id, response.term, response.ack, response.success)

        except grpc.RpcError as e:
            dump_state(f"Error occurred while sending RPC to Node {follower_id}.")
            print(f"Failed to connect to node {follower_id}")

    def process_log_response(self, follower, term, ack, success):
        global current_term, commit_length, current_role, voted_for

        print("process_log_response-",follower)
        if term == current_term and current_role == 'leader':
            if success and ack >= self.acked_length[follower]:
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] -= 1

        elif term > current_term:
            current_term = term
            current_role = 'follower'
            voted_for = None
            reset_election_timeout()
            # apply committed entries to the state machine

    def commit_log_entries(self):
        global logs, commit_length, current_term, node_id

        min_acks = len(self.server_adds) // 2
        print("commit log entries 1")
        ready = []

        for i in range(len(logs)):
            acked_nodes = [n for n in self.server_adds.keys() if n != node_id and self.acked_length[n] >= i]
            if len(acked_nodes) >= min_acks:
                ready.append(i)
        
        if ready and ready[-1] >= commit_length and logs[ready[-1]].term == current_term:
            for i in range(commit_length, ready[-1] + 1):
                print(logs[i], end=' ')
                dump_state(f"Node {node_id} (leader) committed the entry {logs[i].command} {logs[i].key} {logs[i].value} to the state machine.")
            commit_length = ready[-1] + 1
            print("commitlength 2-",commit_length)
            save_state(node_id)


class NodeServer(node_pb2_grpc.NodeServicer):

    def Vote(self, request, context):
        global current_term, current_role, voted_for, logs, node_id
        
        reset_election_timeout()
        if request.term > current_term:
            current_term = request.term
            current_role = 'follower'
            voted_for = None

        last_log_term = 0 if not logs else logs[-1].term
        log_ok = request.last_term > last_log_term or (request.last_term == last_log_term and request.log_length >= len(logs))
        vote_granted = False
        print(f"Vote 1: term: {request.term}, current_term: {current_term}, voted_for: {voted_for}, candidate_id: {request.candidate_id}")
        
        if request.term == current_term and log_ok and (voted_for is None or voted_for == request.candidate_id):
            voted_for = request.candidate_id
            vote_granted = True
        
        dump_state(f"Vote {'granted' if vote_granted else 'denied'} for Node {request.candidate_id} in term {current_term}")
        return node_pb2.VoteResponse(voter_id=node_id, term=current_term, vote_granted=vote_granted)
    
    def append_entries(self, prefix_len, leader_commit, suffix):
        global logs, commit_length, current_leader

        print("append_entries")
        if len(suffix) > 0 and len(logs) > prefix_len:
            index = min(len(logs), prefix_len + len(suffix)) - 1
            if logs[index].term != suffix[index-prefix_len].term:
                logs = logs[:prefix_len]

        if prefix_len + len(suffix) > len(logs):
            logs += suffix
            dump_state(f"Node {node_id} accepted AppendEntries RPC from {current_leader}.")
        
        if leader_commit > commit_length:
            for i in range(commit_length, leader_commit):
                print(logs[i], end = " ")
                dump_state(f"Node {node_id} (leader) committed the entry {logs[i].command} {logs[i].key} {logs[i].value} to the state machine.")
            commit_length = leader_commit
        save_state(node_id)
            
    def Log(self, request, context):
            global current_term, current_role, current_leader, voted_for, logs, commit_length, node_id

            if request.term >= current_term:
                if request.term > current_term:
                    voted_for = None

                current_term = request.term
                current_role = 'follower'
                current_leader = request.leader_id
                reset_election_timeout()

            log_ok = len(logs) >= request.prefixLen and (request.prefixLen == 0 or logs[request.prefixLen - 1].term == request.prefixTerm)
            if request.term == current_term and log_ok:
                self.append_entries(request.prefixLen, request.leaderCommit, request.suffix)
                ack = request.prefixLen + len(request.suffix)
                print(f"In log - success {True} {logs}id-{node_id}, ack - {ack}")
                return node_pb2.LogResponse(node_id = node_id, term = current_term, ack = ack, success = True)
            
            else:
                dump_state(f"Node {node_id} rejected AppendEntries RPC from {current_leader}.")
                return node_pb2.LogResponse(node_id = node_id, term = current_term, ack = 0, success = False)

    def SetVal(self, request, context):
        global logs, current_role, current_leader, current_term, commit_length
        
        if current_role != "leader":
            return node_pb2.SetValResponse(success=False, current_leader = current_leader)
        
        key = request.key
        value = request.value
        newLog = node_pb2.LogRequest.LogItem(command = "SET", key = key, value = value, term = current_term)
        dump_state(f"Node {node_id} (leader) received a SET request.")
        logs.append(newLog)
        index = len(logs)  # Index of the new log entry
        print(logs)
        save_state(node_id)        
        # The actual replication will be handled in the next heartbeat
        print(f"Entry appended to log at index {index}, waiting for next heartbeat to replicate")
        attempt = 1

        while commit_length < index:
            if attempt > 5:
                print("Timeout waiting for log entry to be committed")
                return node_pb2.SetValResponse(success=False, current_leader = current_leader)
            
            if current_role != "leader":  # Check if still the leader
                print("No longer the leader")
                return node_pb2.SetValResponse(success=False, current_leader = current_leader)
            time.sleep(1)
            attempt += 1
        else:
            print(f"Set {key} = {value}")            
            return node_pb2.SetValResponse(success=True, current_leader = current_leader)
    
    def GetVal(self, request, context):
        global node_id, current_role, current_leader, logs
        
        success = False
        if current_role != "leader":
            return node_pb2.GetValResponse(value=None, success=success, current_leader = current_leader)
        
        key = request.key
        value = None
        dump_state(f"Node {node_id} (leader) received a GET request.")

        for entry in reversed(logs):
            if entry.key == key:
                value = entry.value
                success = True
                break

        return node_pb2.GetValResponse(value=value, success=success, current_leader = current_leader)
    

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node_pb2_grpc.add_NodeServicer_to_server(NodeServer(), server)
    server.add_insecure_port('localhost:50051')
    server.start()
    server.wait_for_termination()

def client():        
    server_adds = {
        0: "localhost:50051",
        1: "localhost:50052",
        2: "localhost:50053",
        3: "localhost:50054",
        4: "localhost:50055"
    }
    node = NodeClient(nodeId=0, server_adds=server_adds)

# main function to start client and server side
def main():
    clientThread = threading.Thread(target=client)
    serverThread = threading.Thread(target=server)
    
    clientThread.start()
    serverThread.start()

    clientThread.join()
    serverThread.join()

if __name__ == '__main__':
    main()
