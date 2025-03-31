# Avengers Distributed File System (ADFS)

## Overview

The **Avengers Distributed File System (ADFS)** is an MPI-based distributed file system designed to securely store and manage data in a resilient, fault-tolerant way. In this project, **Iron Man (Rank 0)** acts as the central metadata server, and **Avengers (Ranks 1 to N-1)** serve as storage nodes. The system implements file partitioning with three-way replication, load balancing, a heartbeat mechanism for node monitoring, and failover/recovery operations.

---

## Features

1. **File Partitioning and Three-Way Replication**  
   - Files are split into fixed-size chunks (32 bytes each).  
   - Each chunk is replicated on three distinct storage nodes to ensure redundancy.  
   - The replication factor is maintained only until a node failure occurs.

2. **Load Balancing**  
   - Chunks are distributed evenly across available storage nodes.  
   - Each node is prevented from holding more than one replica of the same chunk.

3. **Heartbeat Mechanism**  
   - Each storage node sends periodic heartbeat messages (every 1 second) to the metadata server (Iron Man).  
   - If the metadata server does not receive a heartbeat from a node for more than 3 seconds, that node is marked as down.

4. **Failover and Recovery Operations**  
   - **Failover:** Simulates a node failure by stopping the node’s heartbeat and processing of requests.  
   - **Recover:** Restores a failed node’s ability to send heartbeats and process requests, effectively reintroducing it into the system.

5. **File Operations**  
   - **Upload:** Files are partitioned and their chunks are replicated across storage nodes.  
   - **Retrieve (Download):** The file is reassembled from its chunks if at least one replica per chunk is available.  
   - **Search:** Distributed search across file chunks to find exact word matches and report the offsets.  
   - **List File:** Displays the chunk mapping to node ranks for a given file.

6. **Error Handling**  
   - On any operation failure (e.g., file not found, missing chunk replicas, duplicate uploads), the system outputs `-1` as an error signal.

---

## Problem Statement

In a mission to safeguard Earth’s most critical data, the Avengers must securely store and manage their vast amount of information. The task is to design a robust distributed file system (ADFS) where:

- Iron Man (Rank 0) manages file metadata and indexes.
- Other Avengers (Ranks 1 to N-1) serve as storage nodes for file chunks.
- The system must support file uploads, downloads, searches, and dynamic load balancing with replication.
- Failover and recovery options must simulate node failures and restore them accordingly.
- A minimum of 4 processes will be spawned (one metadata server and at least three storage nodes).

The complete feature set and I/O specifications are detailed in the provided problem statement.

---

## Project Structure

The project is organized into the following files:

- **DistributedFileSystem.h**  
  Contains the class declaration for the `DistributedFileSystem` along with constants, data structures, and function prototypes.

- **DistributedFileSystem.cpp**  
  Provides the implementation of all methods declared in the header file, including file operations, heartbeat management, load balancing, and messaging via MPI.

- **main.cpp**  
  Contains the `main()` function which initializes MPI, creates an instance of the distributed file system, and processes commands (upload, retrieve, search, list, failover, recover, exit) from standard input.

---

## Setup and Compilation

### Prerequisites

- **MPI Library:** Ensure you have an MPI implementation (such as MPICH or OpenMPI) installed on your system.
- **C++ Compiler:** A C++ compiler that supports C++11 (or later) and threading (e.g., `g++` or `mpic++`).

### Compilation

Compile the project using the MPI compiler. For example, using `mpic++`:

```bash
mpic++ -std=c++11 -pthread -o adfs main.cpp DistributedFileSystem.cpp
```

This command will create an executable named `adfs`.

---

## Running the Application

Run the application with MPI by specifying the number of processes (at least 4). For example:

```bash
mpirun -np 4 ./adfs
```

### Command Line Interface

The system reads commands from standard input. Supported commands include:

- **Upload a File:**

  ```bash
  upload <file_name> <absolute_file_path>
  ```
  
  - Uploads a file to the DFS, partitions it into 32-byte chunks, replicates each chunk on three storage nodes, and prints `1` on success along with chunk mapping.

- **Retrieve (Download) a File:**

  ```bash
  retrieve <file_name>
  ```
  
  - Reassembles and outputs the file’s content if all chunks have at least one live replica. Outputs `-1` if the file is not found or a chunk is missing.

- **Search for a Word in a File:**

  ```bash
  search <file_name> <word>
  ```
  
  - Searches for exact word matches in the file and outputs the number of occurrences and their offsets. Outputs `-1` on error.

- **List File:**

  ```bash
  list_file <file_name>
  ```
  
  - Lists the storage nodes (ranks) that contain each chunk of the file, ordered by chunk number.

- **Failover a Node:**

  ```bash
  failover <node_rank>
  ```
  
  - Simulates the failure of a storage node by stopping its heartbeat and removing it from chunk mappings. Outputs `1` on success or `-1` on error.

- **Recover a Node:**

  ```bash
  recover <node_rank>
  ```
  
  - Recovers a previously failed node so that it resumes sending heartbeats and processing requests. Outputs `1` on success or `-1` on error.

- **Exit:**

  ```bash
  exit
  ```
  
  - Gracefully terminates the system.

---

## Testing

To test the system:
1. Ensure that your file names end with the `.txt` extension.
2. Use relative paths (relative to the main executable) when specifying file paths.
3. Try uploading, retrieving, searching, listing files, and simulating failover/recovery as described in the problem statement.
4. Note that if any error occurs (e.g., duplicate file upload or missing chunk replica), the system should output `-1`.

---

## Additional Information

- **Error Handling:** The DFS returns `-1` for all error cases, such as invalid commands, non-existent files, or when a required node is down.
- **Heartbeat Monitoring:** The metadata server (Rank 0) uses a separate thread to monitor heartbeats and marks nodes as down if they fail to respond within 3 seconds.
- **Thread Safety:** Mutexes are used to ensure safe concurrent access to metadata and system state.

This project demonstrates distributed systems concepts including redundancy, fault tolerance, and load balancing using MPI and C++ multithreading.
