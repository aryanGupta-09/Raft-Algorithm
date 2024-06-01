# Raft-Algorithm
Crafting a robust distributed consensus system on Google Cloud, integrating the Raft algorithm with gRPC communication and leader lease mechanism. This project showcases the efficiency and reliability of distributed systems while ensuring seamless coordination and fault tolerance across nodes.

## Tech Stack

<a href="https://www.python.org/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Languages/Python.svg" width="45" height="45" alt="Python" /></a>
<a href="https://cloud.google.com/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/GoogleCloud.png" height="42" alt="Google Cloud" /></a>&nbsp;
<a href="https://protobuf.dev/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/Protobuf.png" width="64" height="38" alt="Protobuf" /></a>&nbsp;
<a href="https://grpc.io/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/GitHub-Profile-Icons/blob/main/Distributed%20Systems%20and%20Cloud/gRPC.png" width="75" height="38" alt="gRPC" /></a>

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

4. Run the Python files
