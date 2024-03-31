import grpc
import node_pb2
import node_pb2_grpc
import sys

class RaftClient:
    def __init__(self):
        self.leader_id = 0
        self.server_adds = {
            0: "localhost:50051",
            1: "localhost:50052",
            2: "localhost:50053",
            3: "localhost:50054",
            4: "localhost:50055"
        }
    
    def set_val(self, key, value):
        try:
            with grpc.insecure_channel(self.server_adds[self.leader_id]) as channel:
                stub = node_pb2_grpc.NodeStub(channel)
                request = node_pb2.SetValRequest(key=key, value=value)
                response = stub.SetVal(request)
                print(f"Set result: {response.current_leader} {response.success}")
                if self.leader_id != response.current_leader:
                    self.leader_id = response.current_leader
                    self.set_val(key, value)

        except grpc.RpcError:
            print("Leader server is unavailable.")
            self.leader_id = (self.leader_id + 1) % len(self.server_adds)
            self.set_val(key, value)

    def get_val(self, key):
        try:
            with grpc.insecure_channel(self.server_adds[self.leader_id]) as channel:
                stub = node_pb2_grpc.NodeStub(channel)
                request = node_pb2.GetValRequest(key=key)
                response = stub.GetVal(request)
                print(f"Get result: {response.value}")
                if self.leader_id != response.current_leader:
                    self.leader_id = response.current_leader
                    self.get_val(key)
                
        except grpc.RpcError:
            print("Leader server is unavailable.")
            self.leader_id = (self.leader_id + 1) % len(self.server_adds)
            self.get_val(key)

def main():
    print("Raft Client started")
    raft_client = RaftClient()

    while True:
        user_input = input().split()
        if user_input[0] == "SET" and len(user_input) == 3:
            key = user_input[1]
            value = user_input[2]
            raft_client.set_val(key, value)
        elif user_input[0] == "GET" and len(user_input) == 2:
            key = user_input[1]
            raft_client.get_val(key)
        elif user_input[0] == "EXIT":
            sys.exit()
        else:
            print("Invalid command")

if __name__ == "__main__":
    main()
