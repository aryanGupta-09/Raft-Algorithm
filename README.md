# Raft-Algorithm
Crafting a robust distributed consensus system on Google Cloud, integrating the Raft algorithm with gRPC communication and leader lease mechanism. This project showcases the efficiency and reliability of distributed systems while ensuring seamless coordination and fault tolerance across nodes.

## Tech Stack

<a href="https://www.python.org/" target="_blank" rel="noreferrer"><img src="https://raw.githubusercontent.com/danielcranney/readme-generator/main/public/icons/skills/python-colored.svg" width="45" height="45" alt="Python" /></a>
<a href="https://cloud.google.com/" target="_blank" rel="noreferrer"><img src="https://static-00.iconduck.com/assets.00/google-cloud-icon-1024x823-wiwlyizc.png" height="42" alt="Google Cloud" /></a>&nbsp;
<a href="https://protobuf.dev/" target="_blank" rel="noreferrer"><img src="https://www.techunits.com/wp-content/uploads/2021/07/pb.png" height="40" alt="protobuf" /></a>&nbsp;
<a href="https://grpc.io/" target="_blank" rel="noreferrer"><img src="https://github.com/aryanGupta-09/aryanGupta-09/assets/96881807/310cb125-1346-49b9-a87a-b6a84934a9a6" width="78" height="40" alt="gRPC" /></a>

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
