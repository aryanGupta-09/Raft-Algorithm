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
LEASE_DURATION = 4  # Lease duration in seconds

server_stubs = {
    0: node_pb2_grpc.NodeStub(grpc.insecure_channel("localhost:50051")),
    1: node_pb2_grpc.NodeStub(grpc.insecure_channel("localhost:50052")),
    2: node_pb2_grpc.NodeStub(grpc.insecure_channel("localhost:50053")),
    3: node_pb2_grpc.NodeStub(grpc.insecure_channel("localhost:50054")),
    4: node_pb2_grpc.NodeStub(grpc.insecure_channel("localhost:50055"))
}

replicate_log_lock = threading.Lock()

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
sent_length = defaultdict(lambda: 0)
acked_length = defaultdict(lambda: 0)
lease_timeout = 0
lease_timer_event = threading.Event()
lease_timer_thread = None

def generate_election_timeout():
    return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

def reset_election_timeout():
    election_timer_event.set()

lease_flag = None

def start_leader_lease_timer():
    global current_role, lease_flag

    print(f"start_lease_timer: leader_lease_remaining: {lease_timeout}")
    while current_role == 'leader':
        lease_timer_event.wait(lease_timeout)
        
        if lease_flag == "reset":
            lease_timer_event.clear()
            lease_flag = None
            continue
        
        elif lease_flag == "stop":
            lease_flag = None
            break
        else:
            current_role = 'follower'
            break
    
def reset_leader_lease_timeout():
    global lease_timeout, lease_flag, lease_timer_event
    lease_timeout = LEASE_DURATION
    lease_flag = "reset"
    lease_timer_event.set()

def start_lease_timer(flag):
    if flag == "leader":
        start_leader_lease_timer()
    else:
        start_follower_lease_timer()

def start_lease_timer_with_flag(flag):
    global lease_timer_event, lease_timer_thread, lease_flag
    lease_timer_event = threading.Event()
    if lease_timer_thread is not None and lease_timer_thread.is_alive():
        lease_flag = "stop"
        lease_timer_event.set()
    lease_timer_thread = threading.Thread(target=start_lease_timer, args=(flag,))
    lease_timer_thread.start()

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
                if log.command == 'NO-OP':
                    f.write(f'{log.command} {log.term}\n')
                else:
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
            for line in f.readlines():
                line = line.split()

                if line[0] == "NO-OP":
                    logs.append(node_pb2.LogRequest.LogItem(command=line[0], key="", value="", term=int(line[1])))
                else:
                    logs.append(node_pb2.LogRequest.LogItem(command=line[0], key=line[1], value=line[2], term=int(line[3])))
        
        with open(f'{directory}/metadata.txt', 'r') as f:
            metadata = f.read().split(',')
            commit_length = int(metadata[0].split(':')[1].strip())
            current_term = int(metadata[1].split(':')[1].strip())
            voted_for = metadata[2].split(':')[1].strip().lower() == 'True'
    
    except Exception as e:
        print(f"An error occurred while loading the state")

def replicate_log(self, follower_id, acks):
    global logs, node_id, current_term, commit_length

    print("replicateLog1:", follower_id)
    prefixLen = sent_length[follower_id]
    suffix = logs[prefixLen:]
    prefixTerm = 0

    if sent_length[follower_id] > 0:
        prefixTerm = logs[sent_length[follower_id] - 1].term

    request = node_pb2.LogRequest(leader_id=node_id, term=current_term, prefixLen=prefixLen,
                                    prefixTerm=prefixTerm, leaderCommit=commit_length,
                                    suffix=suffix, follower_id=follower_id, leader_lease = lease_timeout)
    try:
        stub = server_stubs[follower_id]
        response = stub.Log(request)
        print("replicateLog2:",follower_id)
        process_log_response(self, follower_id, response.term, response.ack, response.success, acks)

    except grpc.RpcError as e:
        dump_state(f"Error occurred while sending RPC to Node {follower_id}.")
        print(f"Failed connection - {follower_id}")

def process_log_response(self, follower, term, ack, success, acks):
    global current_term, commit_length, current_role, voted_for

    print("process_log_response-",follower)
    if term == current_term and current_role == 'leader':
        if success and ack >= acked_length[follower]:
            sent_length[follower] = ack
            acked_length[follower] = ack
            commit_log_entries(self)
        elif sent_length[follower] > 0:
            sent_length[follower] -= 1
        acks[0] += 1

    elif term > current_term:
        current_term = term
        current_role = 'follower'
        voted_for = None
        reset_election_timeout()
        # apply committed entries to the state machine

def commit_log_entries(self):
    global logs, commit_length, current_term, node_id

    min_acks = len(server_stubs) // 2
    print("commit log entries 1")
    ready = []

    for i in range(len(logs)):
        acked_nodes = [n for n in server_stubs.keys() if n != node_id and acked_length[n] >= i]
        if len(acked_nodes) >= min_acks:
            ready.append(i)
    
    if ready and ready[-1] >= commit_length and logs[ready[-1]].term == current_term:
        for i in range(commit_length, ready[-1] + 1):
            print(logs[i], end=' ')
            dump_state(f"Node {node_id} (leader) committed the entry {logs[i].command} {logs[i].key} {logs[i].value} to the state machine.")
        commit_length = ready[-1] + 1
        print("commitlength 2-",commit_length)
        save_state(node_id)
            

class NodeClient:
    def __init__(self, nodeId):
        global node_id, election_timeout, election_timer_event, election_timer_thread, lease_timer_event, lease_timer_thread

        load_state(nodeId)
        save_state(nodeId)
        node_id = nodeId
        election_timer_event = threading.Event()
        election_timer_thread = threading.Thread(target=self.start_election_timer)
        election_timer_thread.start()

        self.votes_received = set()
            
    def start_election_timer(self):
        while True:
            election_timeout = generate_election_timeout()
            election_timer_event.wait(election_timeout)

            if election_timer_event.is_set():
                election_timer_event.clear()
            else:
                if current_role != 'leader':
                    self.start_election()
        
    def start_election(self):
        global current_leader, current_role, voted_for, current_term, node_id, logs

        dump_state(f"Node {node_id} election timeout-> Starting election.")
        current_term += 1
        save_state(node_id)
        current_role = 'candidate'
        voted_for = node_id
        self.votes_received = {node_id}
        last_term = 0 if not logs else logs[-1].term
        threads = []

        for nodeId in server_stubs.keys():
            print("start_election 1:", current_role)
            if current_role != 'candidate':
                break

            if nodeId != node_id:
                print(f"{current_term} start_election2:{nodeId}")
                thread = threading.Thread(target=self.request_vote, args=(nodeId, current_term, len(logs), last_term))
                threads.append(thread)
                thread.start()
                thread.join()
        # wait for all threads to complete
        # for thread in threads:
        #     thread.join()
    
    def request_vote(self, nodeId, current_term, log_length, last_term):
        try:
            request = node_pb2.VoteRequest(candidate_id=node_id, term=current_term, log_length=log_length, last_term=last_term)
            stub = server_stubs[nodeId]
            response = stub.Vote(request)
            self.collect_vote(response)

        except grpc.RpcError as e:
            print(f"Failed to connect to node {nodeId}")
            dump_state(f"Error occurred while sending RPC to Node {nodeId}.")
    
    def collect_vote(self, response):
        global current_role, node_id, current_term, logs, voted_for, current_leader, lease_timeout

        print(f"{response.voter_id} collectVote1:{response.vote_granted}")
        if current_role == 'candidate' and response.term == current_term and response.vote_granted:
            self.votes_received.add(response.voter_id)
            lease_timeout = max(lease_timeout, response.old_leader_lease_duration) # lease_timeout first becomes equal to the max old leader lease's remaining duration
            print(f"{response.voter_id} collectVote1: recieving {response.old_leader_lease_duration} ")

            if len(self.votes_received) >= len(server_stubs) // 2 + 1:
                current_role = 'leader'
                dump_state(f"Node {node_id} became the leader for term {current_term}")
                print(f"collectVote 2: Node {node_id} became the LEADER for term {current_term}")
                current_leader = node_id
                reset_election_timeout()
                for follower_id in server_stubs.keys():
                    if follower_id != node_id:
                        sent_length[follower_id] = len(logs)
                        acked_length[follower_id] = 0
                
                print(f"Leader Sleeping for {lease_timeout} seconds")
                dump_state(f"New Leader waiting for Old Leader Lease to timeout.")
                time.sleep(lease_timeout)
                lease_timeout = LEASE_DURATION
                
                noopLog = node_pb2.LogRequest.LogItem(command = "NO-OP", key = "", value = "", term = current_term)
                logs.append(noopLog)
                start_lease_timer_with_flag("leader")
                self.start_heartbeat()

        elif response.term > current_term:
            current_term = response.term
            current_role = 'follower'
            print("collect_vote 3:", current_role)
            voted_for = None
            save_state(node_id)
            reset_election_timeout()
    
    def start_heartbeat(self):
        global current_role, node_id, lease_timeout

        start_time = time.time()
        renew_fail = False
        while current_role == 'leader':
            reset_election_timeout()
            print("start_heartbeat")

            if lease_timeout <= 0:
                print(f"Node {node_id}'s leader lease expired.")
                break

            with replicate_log_lock:
                dump_state(f"Leader {node_id} sending heartbeat & Renewing Lease")
                threads = []
                acks = [0]
                elapsed_time = time.time() - start_time
                lease_timeout = max(0, lease_timeout - elapsed_time)
                print(f"start_heartbeat lease_timeout: {lease_timeout}")
               
                for follower_id in server_stubs.keys():
                    if follower_id != node_id:
                        thread = threading.Thread(target=replicate_log, args=(self, follower_id, acks))
                        threads.append(thread)
                        thread.start()
                        thread.join()
                # wait for all threads to complete
                # for thread in threads:
                #     thread.join()
                
                if acks[0] >= len(server_stubs) // 2:
                    print(f"Leader {node_id} lease renewed for {lease_timeout} seconds.")
                    reset_leader_lease_timeout()
                    renew_fail = False
                else:
                    renew_fail = True

                start_time = time.time()
                time.sleep(HEARTBEAT_INTERVAL)
        else:
            if renew_fail:
                dump_state(f"Leader {node_id} lease renewal failed. Stepping Down.")
            else:
                dump_state(f"{node_id} Stepping down")


leader_lease_start_time = None

def start_follower_lease_timer():
    global lease_flag, leader_lease_start_time, current_role
    print(f"start_leader_lease_timer: leader_lease_remaining: {lease_timeout}")
    while current_role == "follower":
        leader_lease_start_time = time.time()
        lease_timer_event.wait(lease_timeout)

        if lease_flag == "reset":
            lease_timer_event.clear()
            lease_flag = None
            continue
                
        elif lease_flag == "stop":
            lease_flag = None
            leader_lease_start_time = None
            break

        current_role = 'follower'
        break

def reset_follower_lease_timeout():
    global lease_flag
    lease_flag = "reset"
    lease_timer_event.set()

class NodeServer(node_pb2_grpc.NodeServicer):

    def Vote(self, request, context):
        global current_term, current_role, voted_for, logs, node_id, leader_lease_start_time, lease_timeout, lease_flag
        
        reset_election_timeout()
        if request.term > current_term:
            current_term = request.term
            current_role = 'follower'
            lease_flag = "stop"
            lease_timer_event.set()
            voted_for = None

        last_log_term = 0 if not logs else logs[-1].term
        log_ok = request.last_term > last_log_term or (request.last_term == last_log_term and request.log_length >= len(logs))
        vote_granted = False
        
        if request.term == current_term and log_ok and (voted_for is None or voted_for == request.candidate_id):
            voted_for = request.candidate_id
            vote_granted = True

        print(f"Vote: term: {request.term}, current_term: {current_term}, voted_for: {voted_for}, candidate_id: {request.candidate_id}, vote_granted: {vote_granted}")
        
        dump_state(f"Vote {'granted' if vote_granted else 'denied'} for Node {request.candidate_id} in term {current_term}")
        if leader_lease_start_time is not None:
            leader_lease_elapsed_time = time.time() - leader_lease_start_time
            lease_timeout = max(0, lease_timeout - leader_lease_elapsed_time)
            print(f"leader_lease_timeout: {lease_timeout}, leader_lease_start_time: {leader_lease_start_time}, leader_lease_elapsed_time: {leader_lease_elapsed_time}")
            
        return node_pb2.VoteResponse(voter_id=node_id, term=current_term, vote_granted=vote_granted, old_leader_lease_duration=lease_timeout)
    
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
                dump_state(f"Node {node_id} (follower) committed the entry {logs[i].command} {logs[i].key} {logs[i].value} to the state machine.")
            commit_length = leader_commit
        save_state(node_id)
            
    def Log(self, request, context):
            global current_term, current_role, current_leader, voted_for, logs, commit_length, node_id, lease_timeout, lease_flag

            if request.term >= current_term:
                if request.term > current_term:
                    voted_for = None

                current_term = request.term
                current_role = 'follower'
                current_leader = request.leader_id
                lease_timeout = request.leader_lease
                reset_follower_lease_timeout()
                reset_election_timeout()
                
            for s in request.suffix:
                if s.command == "NO-OP":
                    start_lease_timer_with_flag("follower")

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
            print("Waiting for log entry to be committed")
            if attempt > 5:
                print("Timeout waiting for log entry to be committed")
                return node_pb2.SetValResponse(success=False, current_leader = current_leader)
            
            if current_role != "leader":  # Check if still the leader
                print("No longer the leader")
                return node_pb2.SetValResponse(success=False, current_leader = current_leader)
            
            reset_election_timeout()

            with replicate_log_lock:
                threads = []
                for follower_id in server_stubs.keys():
                    if follower_id != node_id:
                        thread = threading.Thread(target=replicate_log, args=(self, follower_id, [0]))
                        threads.append(thread)
                        thread.start()
                        thread.join()
                # wait for all threads to complete
                # for thread in threads:
                #     thread.join()
            
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
    node = NodeClient(nodeId=0)

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