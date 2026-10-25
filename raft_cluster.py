import socket
import threading
import time
import random
import json
import struct
import sys
import os

# Hardcoded cluster map: Node ID -> (Host, Port)
CLUSTER_CONFIG = {
    1: ("127.0.0.1", 5001),
    2: ("127.0.0.1", 5002),
    3: ("127.0.0.1", 5003),
}

class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.host, self.port = CLUSTER_CONFIG[node_id]
        
        # Persistent state on all servers (saved to disk so restarts don't wipe memory)
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Each entry: {"term": int, "index": int, "command": dict}
        
        # Volatile state on all servers
        self.state = "Follower"  # Follower, Candidate, Leader
        self.commit_index = 0
        self.last_applied = 0
        
        # Volatile state on leaders (reinitialized after election)
        self.next_index = {}
        self.match_index = {}
        
        # Actual Key-Value state machine storage
        self.kv_store = {}
        
        # Timing & Concurrency mechanics
        self.lock = threading.Lock()
        self.last_heartbeat_time = time.time()
        # Randomized election timeout between 300ms and 600ms to avoid split votes
        self.election_timeout = random.uniform(0.3, 0.6)
        
        self.load_persistent_state()
        print(f"[Node {self.node_id}] Booted up. State: {self.state} | Term: {self.current_term}")

    # Persistence (Don't lose state on crash)
    def persist(self):
        # Write vital Raft state to a local JSON file
        state_file = f".raft_state_{self.node_id}.json"
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log
        }
        with open(state_file, "w") as f:
            json.dump(data, f)

    def load_persistent_state(self):
        state_file = f".raft_state_{self.node_id}.json"
        if os.path.exists(state_file):
            try:
                with open(state_file, "r") as f:
                    data = json.load(f)
                    self.current_term = data.get("current_term", 0)
                    self.voted_for = data.get("voted_for", None)
                    self.log = data.get("log", [])
                    print(f"[Node {self.node_id}] Recovered state from disk. Term: {self.current_term}")
            except Exception as e:
                print(f"[Node {self.node_id}] Failed to load state file: {e}")

    # Network & TCP Framing Core
    def start(self):
        # Spin up TCP server listener thread
        threading.Thread(target=self._run_server, daemon=True).start()
        # Spin up background ticker for elections and heartbeats
        threading.Thread(target=self._ticker_loop, daemon=True).start()

    def _run_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(10)
        
        while True:
            try:
                sock, _ = server.accept()
                threading.Thread(target=self._handle_connection, args=(sock,), daemon=True).start()
            except Exception as e:
                print(f"[Node {self.node_id}] Socket accept error: {e}")

    def _handle_connection(self, sock):
        try:
            msg = self._recv_msg(sock)
            if msg:
                resp = self._route_message(msg)
                self._send_msg(sock, resp)
        except Exception:
            # Sockets drop, peers crash. Don't panic, just clean up.
            pass
        finally:
            sock.close()

    def _send_msg(self, sock, data):
        payload = json.dumps(data).encode('utf-8')
        sock.sendall(struct.pack('!I', len(payload)) + payload)

    def _recv_msg(self, sock):
        raw_len = sock.recv(4)
        if not raw_len:
            return None
        msg_len = struct.unpack('!I', raw_len)[0]
        data = bytearray()
        while len(data) < msg_len:
            packet = sock.recv(msg_len - len(data))
            if not packet:
                return None
            data.extend(packet)
        return json.loads(data.decode('utf-8'))

    def send_rpc(self, target_id, message, timeout=0.2):
        target_host, target_port = CLUSTER_CONFIG[target_id]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((target_host, target_port))
                self._send_msg(s, message)
                return self._recv_msg(s)
        except Exception:
            # Peer is likely dead or partitioned. Ignore safely.
            return None

    # Raft Protocol Logic & Ticker
    def _ticker_loop(self):
        while True:
            time.sleep(0.05)  # Tick every 50ms
            with self.lock:
                now = time.time()
                
                if self.state == "Leader":
                    # Leaders broadcast heartbeats constantly to assert dominance
                    self._send_heartbeats()
                else:
                    # Followers/Candidates check if their election timeout expired
                    if now - self.last_heartbeat_time > self.election_timeout:
                        print(f"[Node {self.node_id}] Election timeout reached! Starting election...")
                        self._start_election()

    def _start_election(self):
        self.state = "Candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.persist()
        
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(0.3, 0.6)  # Reset randomized timer
        
        term = self.current_term
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]["term"] if last_log_index >= 0 else 0
        
        votes = 1  
        cluster_size = len(CLUSTER_CONFIG)
        
        # Fire off RequestVote RPCs to all peers in background threads
        for peer_id in CLUSTER_CONFIG:
            if peer_id == self.node_id:
                continue
            
            def ask_vote(pid):
                rpc = {
                    "type": "REQUEST_VOTE",
                    "term": term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term
                }
                res = self.send_rpc(pid, rpc)
                if res and res.get("vote_granted"):
                    nonlocal votes
                    with self.lock:
                        if self.state == "Candidate" and res.get("term") == self.current_term:
                            votes += 1
                            if votes > cluster_size // 2:
                                print(f"[Node {self.node_id}] WON ELECTION FOR TERM {self.current_term}! Becoming Leader.")
                                self.state = "Leader"
                                # Initialize leader state structures
                                for p in CLUSTER_CONFIG:
                                    if p != self.node_id:
                                        self.next_index[p] = len(self.log)
                                        self.match_index[p] = 0
                                self._send_heartbeats()

            threading.Thread(target=ask_vote, args=(peer_id,), daemon=True).start()

    def _send_heartbeats(self):
        # Send AppendEntries (heartbeat) to all peers
        for peer_id in CLUSTER_CONFIG:
            if peer_id == self.node_id:
                continue
            
            def heartbeat(pid):
                with self.lock:
                    if self.state != "Leader":
                        return
                    prev_index = self.next_index.get(pid, 1) - 1
                    prev_term = self.log[prev_index]["term"] if prev_index >= 0 and prev_index < len(self.log) else 0
                    entries = self.log[prev_index + 1:]
                    
                    rpc = {
                        "type": "APPEND_ENTRIES",
                        "term": self.current_term,
                        "leader_id": self.node_id,
                        "prev_log_index": prev_index,
                        "prev_log_term": prev_term,
                        "entries": entries,
                        "leader_commit": self.commit_index
                    }
                
                res = self.send_rpc(pid, rpc)
                if res:
                    with self.lock:
                        if res.get("term") > self.current_term:
                            # Step down if we find a higher term out in the wild
                            self.current_term = res.get("term")
                            self.state = "Follower"
                            self.voted_for = None
                            self.persist()
                            return
                        
                        if res.get("success"):
                            self.next_index[pid] = len(self.log)
                            self.match_index[pid] = len(self.log) - 1

            threading.Thread(target=heartbeat, args=(peer_id,), daemon=True).start()

    def _route_message(self, msg):
        with self.lock:
            m_type = msg.get("type")
            m_term = msg.get("term", 0)
            
            # Rule: If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
            if m_term > self.current_term:
                self.current_term = m_term
                self.state = "Follower"
                self.voted_for = None
                self.persist()

            if m_type == "REQUEST_VOTE":
                return self._handle_request_vote(msg)
            elif m_type == "APPEND_ENTRIES":
                return self._handle_append_entries(msg)
            elif m_type == "CLIENT_COMMAND":
                return self._handle_client_command(msg)
            
            return {"status": "error", "message": "unknown message type"}

    def _handle_request_vote(self, msg):
        candidate_id = msg["candidate_id"]
        term = msg["term"]
        
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}
            
        # Grant vote if we haven't voted for anyone else yet in this term
        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self.persist()
            print(f"[Node {self.node_id}] Voted for Node {candidate_id} in term {term}")
            return {"term": self.current_term, "vote_granted": True}
            
        return {"term": self.current_term, "vote_granted": False}

    def _handle_append_entries(self, msg):
        term = msg["term"]
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
            
        # Valid leader found, reset our election ticker timer
        self.last_heartbeat_time = time.time()
        self.state = "Follower"
        
        # Append logs logic simplified for robustness
        entries = msg.get("entries", [])
        if entries:
            self.log.extend(entries)
            self.persist()
            
        # Apply committed entries to our state machine (KV store)
        leader_commit = msg.get("leader_commit", 0)
        while self.commit_index < leader_commit and self.commit_index < len(self.log):
            entry = self.log[self.commit_index]
            cmd = entry["command"]
            if cmd["action"] == "SET":
                self.kv_store[cmd["key"]] = cmd["value"]
                print(f"[Node {self.node_id}] State Machine Applied: SET {cmd['key']} = {cmd['value']}")
            self.commit_index += 1

        return {"term": self.current_term, "success": True}

    def _handle_client_command(self, msg):
        # If we are not leader, reject or redirect client
        if self.state != "Leader":
            return {"status": "not_leader", "leader_port": self.port}
            
        action = msg.get("action")
        if action == "GET":
            val = self.kv_store.get(msg.get("key"))
            return {"status": "success", "value": val}
            
        elif action == "SET":
            entry = {
                "term": self.current_term,
                "index": len(self.log),
                "command": msg
            }
            self.log.append(entry)
            self.persist()
            # For this compact script, immediately apply locally when leader processes it
            self.kv_store[msg["key"]] = msg["value"]
            self.commit_index += 1
            return {"status": "success", "message": "Key set successfully"}

        return {"status": "bad_command"}

    def client_request(self, command_dict):
        # Helper for local REPL client commands
        if self.state == "Leader":
            return self._handle_client_command(command_dict)
        else:
            return {"status": "not_leader"}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python raft_cluster.py <node_id (1, 2, or 3)>")
        sys.exit(1)
        
    nid = int(sys.argv[1])
    node = RaftNode(nid)
    node.start()
    
    print(f"\nRaft Node {nid} Interactive Terminal")
    print("Commands:")
    print("  set <key> <value>")
    print("  get <key>")
    print("  state")
    print("-" * 40)
    
    try:
        while True:
            line = input(f"node-{nid}> ").strip().split()
            if not line:
                continue
            
            cmd = line[0].lower()
            if cmd == "set" and len(line) == 3:
                res = node.client_request({"action": "SET", "key": line[1], "value": line[2]})
                print("Result:", res)
            elif cmd == "get" and len(line) == 2:
                res = node.client_request({"action": "GET", "key": line[1]})
                print("Result:", res)
            elif cmd == "state":
                print(f"State: {node.state} | Term: {node.current_term} | KV Store: {node.kv_store}")
            else:
                print("Invalid command or syntax.")
    except KeyboardInterrupt:
        print("\nShutting down node gracefully...")