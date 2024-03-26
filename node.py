import random
import time
import node_pb2
import node_pb2_grpc
import grpc
import threading

from collections import defaultdict
from concurrent import futures

# Define constants
ELECTION_TIMEOUT_MIN = 5  # Minimum election timeout in seconds
ELECTION_TIMEOUT_MAX = 10  # Maximum election timeout in seconds
HEARTBEAT_INTERVAL = 2.5  # Heartbeat interval in seconds

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


class NodeClient:
    def __init__(self, nodeId, server_adds):
        global node_id, election_timeout, election_timer_event, election_timer_thread
        node_id = nodeId
        election_timer_event = threading.Event()
        election_timer_thread = threading.Thread(target=self.start_election_timer)
        election_timer_thread.start()

        self.votes_received = set()
        self.sent_length = defaultdict(lambda: 0)
        self.acked_length = defaultdict(lambda: 0)
        self.server_adds = server_adds
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
        global current_leader, current_role, voted_for, current_term, node_id, logs, stub

        current_term += 1
        current_role = 'candidate'
        voted_for = node_id
        self.votes_received = {node_id}
        last_term = 0 if not logs else logs[-1]['term']

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
            if not self.collect_vote(response):
                return False
        except grpc.RpcError as e:
            print(f"Failed to connect to node {nodeId}")
    
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
    
    def collect_vote(self, response):
        global current_role, node_id, current_term, logs, voted_for, current_leader
        print("collect_vote 1:", response.vote_granted)
        if current_role == 'candidate' and response.term == current_term and response.vote_granted:
            self.votes_received.add(response.voter_id)
            if len(self.votes_received) >= (len(self.server_adds) + 1) // 2:
                current_role = 'leader'
                print("collect_vote 2:", current_role)
                current_leader = node_id
                reset_election_timeout()
                for follower_id in self.server_adds.keys():
                    if follower_id != node_id:
                        self.sent_length[follower_id] = len(logs)
                        self.acked_length[follower_id] = 0
                        self.replicate_log(follower_id)
                self.start_heartbeat()
                return False
        elif response.term > current_term:
            current_term = response.term
            current_role = 'follower'
            print("collect_vote 3:", current_role)
            voted_for = None
            reset_election_timeout()
            return False
        return True
    
    def replicate_log(self, follower_id):
        global logs, node_id, current_term, commit_length
        print("replicate_log 1:", follower_id)
        prefixLen = self.sent_length[follower_id]
        suffix = logs[self.sent_length[follower_id]:]
        prefixTerm = 0
        if self.sent_length[follower_id] > 0:
            prefixTerm = logs[self.sent_length[follower_id] - 1]['term']
        request = node_pb2.LogRequest(leader_id=node_id, term=current_term, prefixLen=prefixLen,
                                       prefixTerm=prefixTerm, leaderCommit=commit_length,
                                       suffix=suffix, follower_id=follower_id)
        try:
            stub = self.get_stub(follower_id)
            response = stub.Log(request)
            self.channel.close()
            print("replicate_log 2")
            self.process_log_response(follower_id, response.term, response.ack, response.success)
        except grpc.RpcError as e:
            print(f"Failed to connect to node {follower_id}")

    def process_log_response(self, follower, term, ack, success):
        global current_term, commit_length, current_role, voted_for
        print("process_log_response")
        if term == current_term and current_role == 'leader':
            if success and ack >= self.acked_length[follower]:
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] -= 1
                self.replicate_log(follower)
        elif term > current_term:
            current_term = term
            current_role = 'follower'
            voted_for = None
            reset_election_timeout()
            # apply committed entries to the state machine

    def commit_log_entries(self):
        global logs, commit_length, current_term
        print("commit_log_entries")
        min_acks = (len(self.server_adds) + 1) // 2
        ready = [i for i in range(1, len(logs) + 1) if len([n for n in self.server_adds.keys() if self.acked_length[n] >= i]) >= min_acks]
        if ready and max(ready) > commit_length and logs[max(ready) - 1]['term'] == current_term:
            for i in range(commit_length, max(ready)):
                print(logs[i])
            commit_length = max(ready)


class NodeServer(node_pb2_grpc.NodeServicer):

    def Vote(self, request, context):
        global current_term, current_role, voted_for, logs, node_id
        reset_election_timeout()
        if request.term > current_term:
            current_term = request.term
            current_role = 'follower'
            voted_for = None

        last_log_term = 0 if not logs else logs[-1]['term']

        log_ok = request.last_term > last_log_term or (request.last_term == last_log_term and request.log_length >= len(logs))
        
        vote_granted = False
        print("Vote 1: request.term, current_term, log_ok voted_for, request.candidate_id", request.term, current_term, log_ok, voted_for, request.candidate_id)
        
        if request.term == current_term and log_ok and (voted_for is None or voted_for == request.candidate_id):
            voted_for = request.candidate_id
            vote_granted = True
        return node_pb2.VoteResponse(voter_id=node_id, term=current_term, vote_granted=vote_granted)
    
    def __append_entries(self, prefix_len, leader_commit, suffix):
        global logs, commit_length
        print("__append_entries")
        if len(suffix) > 0 and len(logs) > prefix_len:
            index = min(len(logs), prefix_len + len(suffix)) - 1
            if logs[index]['term'] != suffix[index-prefix_len]['term']:
                logs = logs[:prefix_len]

        if prefix_len + len(suffix) > len(logs):
            logs += suffix
        
        if leader_commit > commit_length:
            for i in range(commit_length, leader_commit-1):
                print(logs[i])
            commit_length = leader_commit
            
    def Log(self, request, context):
            global current_term, current_role, current_leader, voted_for, logs, commit_length, node_id
            print("Log")

            if request.term >= current_term:
                if request.term > current_term:
                    voted_for = None

                current_term = request.term
                current_role = 'follower'
                current_leader = request.leader_id
                reset_election_timeout()

            log_ok = len(logs) >= request.prefixLen and (request.prefixLen == 0 or logs[request.prefixLen - 1]['term'] == request.prefixTerm)
            if request.term == current_term and log_ok:
                self.__append_entries(request.prefixLen, request.leaderCommit, request.suffix)
                ack = request.prefixLen + len(request.suffix)
                return node_pb2.LogResponse(node_id = node_id, term = current_term, ack = ack, success = True)
            else:
                return node_pb2.LogResponse(node_id = node_id, term = current_term, ack = 0, success = False)


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
