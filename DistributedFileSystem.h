#ifndef DISTRIBUTED_FILE_SYSTEM_H
#define DISTRIBUTED_FILE_SYSTEM_H

#include <mpi.h>
#include <cstring>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <set>
#include <unistd.h>

const int CHUNK_SIZE = 32;
const int REPLICATION_FACTOR = 3;
const int MAX_FILENAME = 256;
const int MAX_FILE_SIZE = 2 * 1024 * 1024; // 2MB

enum MessageTag {
    HEARTBEAT_TAG = 100,
    UPLOAD_TAG,
    DOWNLOAD_TAG,
    SEARCH_TAG,
    LIST_FILE_TAG,
    FAILOVER_TAG,
    RECOVER_TAG,
    EXIT_TAG
};

class DistributedFileSystem {
private:
    int rank;
    int total_ranks;
    std::mutex system_mutex;
    MPI_Comm mpi_comm;
    std::mutex metadata_mutex; // For thread-safe access to metadata

    // Data structure to track file chunk metadata
    struct FileChunk {
        std::string filename;
        int chunk_number;
        std::vector<int> node_ranks;
    };

    struct FileMetadata {
        std::string filename;
        int total_chunks;
        std::vector<FileChunk> chunks;
    };

    std::unordered_map<std::string, FileMetadata> file_metadata;
    std::unordered_map<int, bool> node_status;
    std::unordered_map<int, bool> node_heartbeat_status;

    std::atomic<bool> is_running;
    std::unordered_map<int, std::chrono::steady_clock::time_point> last_heartbeat;

    const int HEARTBEAT_INTERVAL = 1; // seconds
    const int HEARTBEAT_TIMEOUT = 3;  // seconds
    std::thread heartbeat_thread;
    std::thread monitor_thread;

    // Private methods for heartbeat and node selection
    void start_heartbeat_mechanism();
    void send_heartbeats();
    void monitor_heartbeats();
    void stop_heartbeat_threads();
    std::vector<int> select_storage_nodes(const std::string &filename, int chunk_number);

public:
    DistributedFileSystem(int current_rank, int total_process_count);
    ~DistributedFileSystem();

    void stop();
    bool isRunning() const;
    void exit_system();
    int upload_file(const std::string &filename, const std::string &filepath);
    void list_file(const std::string &filename);
    int failover(int target_rank);
    int recover(int target_rank);
    std::vector<char> retrieve_file(const std::string &filename);
    void search_file(const std::string &filename, const std::string &word);
    void process_incoming_messages();
};

#endif // DISTRIBUTED_FILE_SYSTEM_H
