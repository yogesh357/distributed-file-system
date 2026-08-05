# Distributed File System Project Flow & Architecture

This document outlines the core architecture, key execution flows, and peer behavior of the Go-based distributed file system.

---

## Core Components

*   **FilerServer**: The main orchestrator managing the peer list, local storage, and server loops.
*   **TCPTransport**: Manages low-level TCP socket connections and coordinates reading/writing control signals.
*   **Store**: Handles file persistence using content-addressable storage (CAS) and encryption.

---

## Key Execution Flows

### 1. Startup & Network Bootstrapping
1.  **Start Listener**: `Start()` is called on the server, launching the TCP listener and starting the infinite accept loop `startAcceptLoop()` to handle incoming connections.
2.  **Bootstrap network**: `bootstrapNetwork()` dials designated bootstrap nodes to establish connections.
3.  **Connection Establishment**:
    *   **Outbound Dial (Node A)**: Node A calls `Dial()` which executes `net.Dial("tcp", addr)` and spins up `handleConn(conn, true)`.
    *   **Inbound Accept (Node B)**: Node B's listener unblocks from `listener.Accept()` and spins up `handleConn(conn, false)`.
4.  **Handshake & Registration**:
    *   Both nodes wrap their active connection in a `TCPPeer` and run `HandshakeFunc`.
    *   Upon success, the transport's `OnPeer()` callback triggers the `OnPeer()` method in `FilerServer` on both ends, registering the peer in their respective `peers` map.
    *   Both nodes then spin up a persistent read loop in `handleConn()` to process incoming messages.

### 2. Storing a File
When a user calls `Store()`:
1.  **Write Local**: The data is encrypted using AES-CTR and stored in the local `Store`.
2.  **Broadcast Metadata**: A `MessageStoreFile` GOB metadata payload (prefixed by `IncomingMessage`) is broadcasted to all connected peers.
3.  **Transport Pause**: The remote peers receive the metadata message and prepare their storage.
4.  **Stream File**: The sender transmits the `IncomingStream` header followed by the raw ciphertext file bytes.
5.  **Write Remote**: Remote peers read the `IncomingStream` byte, pause their transport read loops via a `WaitGroup`, write the incoming file bytes directly to their local disk, and finally resume their transport loops.

### 3. Retrieving a File
When a user calls `Get()`:
1.  **Local Check**: The server checks if the file exists locally. If yes, it reads, decrypts, and returns it.
2.  **Broadcast Query**: If not, it broadcasts a `MessageGetFile` query to all peers.
3.  **Peer Response**: A peer that has the file handles this query in `handleMessageGetFile()` and streams back the `IncomingStream` header, the file size, and the file bytes.
4.  **Network Read**: The requester parses the file size, temporarily pauses its transport read loop, downloads and decrypts the file directly into its local disk, and returns the reader to the user.

---

## Behavior with 4–5 Peers

When scaled to 4–5 peers:

1.  **Mesh Network Establishment**:
    Each peer runs its own transport listener. When a new peer enters the network, it connects to one or more bootstrap peers. Through handshakes and bootstrap address sharing, peers establish TCP connections with each other, forming a mesh network.
2.  **Full File Replication**:
    Whenever a peer calls `Store()`, it replicates the file to **all** peers in its active `peers` map. In a 5-node cluster, the file is encrypted locally and distributed concurrently to the remaining 4 nodes.
3.  **Distributed File Retrieval**:
    If a node (e.g., Peer A) needs to retrieve a file it does not have:
    *   It broadcasts the query to all 4 neighbors.
    *   The neighbors search their local databases.
    *   The first neighbor that responds with the file size and stream will have its data read and stored by Peer A. Subsequent responses from other neighbors are processed or ignored once Peer A has successfully saved the file.
