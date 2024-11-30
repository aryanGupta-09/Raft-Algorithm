# Raft-Algorithm

Implemented the Raft Consensus Algorithm with a leader lease mechanism to achieve enhanced fault tolerance and data consistency in distributed systems. It showcases seamless coordination between nodes and state recovery mechanisms to restore system functionality following failures.

The solution was successfully deployed and tested on Google Cloud, achieving 99% uptime.

## Tech Stack

<a href="https://www.python.org/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Languages/Python.svg" width="45" height="45" alt="Python" title="Python" /></a>
<a href="https://cloud.google.com/" target="_blank" rel="noreferrer" title="Google Cloud"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/GoogleCloud.png" height="42" alt="Google Cloud" /></a>&nbsp;
<a href="https://protobuf.dev/" target="_blank" rel="noreferrer" title="Protobuf"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/Protobuf.png" width="64" height="38" alt="Protobuf" /></a>&nbsp;
<a href="https://grpc.io/" target="_blank" rel="noreferrer" title="gRPC"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/gRPC.png" width="75" height="38" alt="gRPC" /></a>

## Implementation Details

* **Election Functionality**
    1. Follower nodes maintain a randomized election timer (5-10 seconds) and listen for heartbeat or vote request events.
    2. If no event is received within the timeout, a follower becomes a candidate, increments its term, and sends `voteRPC` requests to other nodes.
    3. Each node votes for only one candidate per term based on specific conditions.
    4. If a candidate receives a majority of votes in response to its `voteRPC` requests, it transitions to the Leader state, waiting for the old leader's lease timer to expire before starting its own lease.
    5. The new leader appends a NO-OP entry to the log and sends heartbeats to all nodes.

* **Log Replication Functionality**
    1. The leader sends periodic heartbeats (~1 second) to all nodes using the `appendEntriesRPC`, reacquiring its lease at each heartbeat by restarting the lease timer.
    2. The lease duration is a fixed value between 2 and 10 seconds, decided before execution, and remains constant throughout.
    3. If the leader fails to reacquire the lease (i.e., does not receive successful acknowledgments from the majority of followers within the lease duration), it must step down.
    4. Followers monitor and track the duration of the leader's lease during each heartbeat.
    5. When the leader receives a client SET request, it uses `appendEntriesRPC` to replicate the log to all nodes. For GET requests, the value is returned immediately if the lease is acquired.
    6. The leader identifies the latest matching log entry in the follower's log and removes any entries beyond that point before transmitting subsequent leader entries.
    7. Once a majority of nodes have replicated the log, the leader sends a SUCCESS reply to the client; otherwise, it sends a FAIL message.

* **Committing Entries**
    1. The leader commits an entry only after a majority of nodes acknowledge appending it and ensures that the latest entry to be committed belongs to the same term as the leader.
    2. Follower nodes commit entries using the `LeaderCommit` field in the `appendEntriesRPC` received during each heartbeat.

* **Client Interaction**
    1. Followers, candidates, and the leader form a Raft cluster serving Raft clients.
    2. The client stores the IP addresses and ports of all nodes and maintains the current leader ID, which might become outdated.
    3. The client sends a GET/SET request to the leader node and updates its leader ID in case of a failure, resending the request to the updated leader.
    4. If there is no leader, the node returns NULL for the current leader and a failure message.
    5. The client continues sending requests until it receives a SUCCESS reply from any node.
    6. Supported operations for the client are:
        - **SET K V:** Maps the key K to value V (WRITE OPERATION).
        - **GET K:** Returns the latest committed value of key K; if K doesn’t exist, an empty string is returned (READ OPERATION).

* **Print Statements & Log Generation**
    1. The implementation includes print statements to indicate the current state of each node and the operations being performed (e.g., starting elections, sending heartbeats, committing entries).
    2. Running the `nodeX.py` files generates a `logs_node_X` folder for each node (where X is the node ID). This folder contains:
        - **logs.txt:** Stores the data of all the logs and the current term.
        - **metadata.txt:** Contains information such as `commitLength`, `appliedLength`, `current_term`, and `votedFor`.
        - **dump.txt:** Captures all the required print statements and logs the significant events and state transitions that occur during execution, providing valuable insights into the operation of the Raft algorithm.   

## Installation

1. Clone the repo
```bash
  git clone https://github.com/aryanGupta-09/Raft-Algorithm.git
```

2. Go to the project directory
```bash
  cd Raft-Algorithm
```

3. Generate the Python code for gRPC
```bash
  python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. node.proto
```

4. Run the `node.py` files in different instances after updating the node IDs and the IP addresses.

5. Run the `client.py` file in a separate instance to perform SET or GET operations.
