# PyRaft-KV: A Zero-Dependency Distributed Key-Value Store in Python

A lightweight, zero-dependency implementation of a distributed, fault-tolerant key-value database powered by the Raft Consensus Algorithm. Built strictly using the Python Standard Library, this project demonstrates core distributed systems principles including leader election, state replication, custom TCP network framing, and local disk persistence.

**Title:** PyRaft-KV: Zero-Dependency Distributed Consensus Engine

**Short Description:** A fault-tolerant, single-file distributed key-value store built in pure Python using the Raft Consensus Algorithm, raw TCP socket framing, and thread synchronization.

**Tags:** python, distributed-systems, raft-consensus, key-value-store, networking, sockets, zero-dependencies, concurrency

## Overview

In modern cloud architecture, systems like `etcd`, Consul, and CockroachDB rely on distributed consensus algorithms to keep state identical across multiple physical or virtual machines. `PyRaft-KV` is a ground-up, single-file educational implementation of the **Raft Consensus Protocol** that requires no third-party packages or external frameworks.

By relying solely on Python's `socket`, `threading`, `json`, and `struct` modules, `PyRaft-KV` exposes the low-level mechanical realities of network programming, binary framing, thread safety, and state machine replication.

## Key Features

* **Zero External Dependencies:** Runs out of the box on any standard Python 3.8+ environment.

* **Dynamic Leader Election:** Automatic failure detection via randomized election timeouts (300ms–600ms) to prevent split-vote scenarios.

* **Custom TCP Framing Engine:** Fixed 4-byte big-endian length-prefix binary protocol (`struct.pack('!I', length)`) to overcome raw TCP byte-streaming and packet fragmentation issues.

* **Atomic State Persistence:** Critical state variables (`currentTerm`, `votedFor`, and `log`) are synchronized to local disk storage (`.raft_state_<node_id>.json`) on mutation, allowing nodes to safely recover from power loss or crash faults without violating cluster safety invariants.

* **Thread-Safe Architecture:** Mutex locks (`threading.Lock`) safeguard internal node state, enabling concurrent handling of incoming RPC requests, client queries, and heartbeat execution.

* **Interactive REPL Interface:** Built-in command-line interface for manual inspection, debugging, and data manipulation directly from the terminal.

## Architecture & Design

### 1. Network & Protocol Layer (Custom TCP Framing)

Because TCP is a continuous byte-stream protocol rather than a message-oriented protocol, reading directly from a socket buffer can lead to packet concatenation or fragmentation. `PyRaft-KV` solves this by framing every JSON payload with a 4-byte header containing the payload's integer length:

```
+-------------------+------------------------------------+
| Length (4 Bytes)  | Payload (JSON-encoded UTF-8 string) |
| Big-Endian (!I)   | {"type": "APPEND_ENTRIES", ...}    |
+-------------------+------------------------------------+

```

### 2. State Machine & Persistence Model

Each Raft node tracks two categories of state:

1. **Persistent State (Saved to `.raft_state_<node_id>.json`):**

   * `current_term`: The latest term the server has seen.

   * `voted_for`: The candidate node ID that received this node's vote in the current term.

   * `log`: Sequence of log entries containing client operations and associated term IDs.

2. **Volatile State (In-Memory Only):**

   * `state`: Current role (`Follower`, `Candidate`, or `Leader`).

   * `commit_index`: Index of the highest log entry known to be committed.

   * `kv_store`: The local key-value dictionary representing the actual state machine.

### 3. Consensus Lifecycle

```
[ Follower ] --(Timeout, start election)--> [ Candidate ]
     ^                                           |
     |                                (Receives Majority Votes)
     |                                           v
(Discovers higher term) <------------------- [ Leader ]

```

## Cluster Topology

By default, the cluster is preconfigured to run three nodes on localhost (`127.0.0.1`):

| Node ID | Host Address | Port | State File Path | 
| ----- | ----- | ----- | ----- | 
| **Node 1** | `127.0.0.1` | `5001` | `.raft_state_1.json` | 
| **Node 2** | `127.0.0.1` | `5002` | `.raft_state_2.json` | 
| **Node 3** | `127.0.0.1` | `5003` | `.raft_state_3.json` | 

## Getting Started

### Prerequisites

* Python 3.8 or higher installed on your system.

* Terminal environment capable of running multiple processes concurrently.

### Launching the Cluster

Open three distinct terminal windows/tabs and run each command to spawn the nodes:

**Terminal 1:**

```
python raft_cluster.py 1

```

**Terminal 2:**

```
python raft_cluster.py 2

```

**Terminal 3:**

```
python raft_cluster.py 3

```

Upon launching all three nodes, you will observe the background ticker fire election timeouts. One node will successfully solicit votes, win the majority election, and transition into the `Leader` state.

## Interactive REPL Reference

Once a node is running, you can enter commands into the terminal prompt (`node-<id>>`):

| Command | Usage Example | Description | 
| ----- | ----- | ----- | 
| `set <key> <value>` | `set user_id 4096` | Commits a key-value entry to the cluster. **Must be issued to the current Leader.** | 
| `get <key>` | `get user_id` | Fetches the value for a key from the local node's state machine. | 
| `state` | `state` | Prints the node's current role (`Leader`/`Follower`), term, and internal state. | 

## Testing Fault Tolerance & Failure Scenarios

### Scenario 1: Simulating Leader Crash and Re-Election

1. Run `state` in all terminal windows to identify which node is the `Leader`.

2. Issue a write command to the Leader terminal:

   ```
   node-1> set cluster_status operational
   
   ```

3. Terminate the Leader process manually via `Ctrl + C`.

4. Observe the remaining two terminals. Within 600 milliseconds, one of the Followers will detect missing heartbeats, increment its term, request votes, and assume leadership.

5. In any surviving node terminal, query the key:

   ```
   node-2> get cluster_status
   
   ```

   The data remains accessible across the cluster.

### Scenario 2: Node Recovery with Disk Persistence

1. Restart the node killed in Scenario 1 (`python raft_cluster.py <node_id>`).

2. The node will read its `.raft_state_<node_id>.json` file, restoring its last known term and voted candidate without corrupting the active cluster lifecycle.
