#include "DistributedFileSystem.h"

// Private: Start heartbeat mechanism based on the node role.
void DistributedFileSystem::start_heartbeat_mechanism() {
    if (rank == 0) {
        // Metadata server monitors heartbeats
        monitor_thread = std::thread(&DistributedFileSystem::monitor_heartbeats, this);
    } else {
        // Storage nodes send heartbeats
        heartbeat_thread = std::thread(&DistributedFileSystem::send_heartbeats, this);
    }
}

// Private: Storage nodes send periodic heartbeats.
void DistributedFileSystem::send_heartbeats() {
    while (is_running && node_heartbeat_status[rank]) {
        if (rank != 0) {
            int heartbeat = 1;
            MPI_Request request;
            MPI_Status status;
            MPI_Isend(&heartbeat, 1, MPI_INT, 0, HEARTBEAT_TAG, mpi_comm, &request);
            MPI_Wait(&request, &status);
        }
        std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_INTERVAL));
    }
}

// Private: Metadata server monitors incoming heartbeats.
void DistributedFileSystem::monitor_heartbeats() {
    std::unordered_map<int, std::chrono::steady_clock::time_point> last_heartbeat_time;
    for (int i = 1; i < total_ranks; ++i) {
        last_heartbeat_time[i] = std::chrono::steady_clock::now();
    }

    while (is_running) {
        MPI_Status status;
        int flag;
        int heartbeat;
        MPI_Iprobe(MPI_ANY_SOURCE, HEARTBEAT_TAG, mpi_comm, &flag, &status);
        if (flag) {
            MPI_Recv(&heartbeat, 1, MPI_INT, status.MPI_SOURCE, HEARTBEAT_TAG, mpi_comm, &status);
            last_heartbeat_time[status.MPI_SOURCE] = std::chrono::steady_clock::now();
            if (!node_status[status.MPI_SOURCE]) {
                std::lock_guard<std::mutex> lock(system_mutex);
                node_status[status.MPI_SOURCE] = true;
                node_heartbeat_status[status.MPI_SOURCE] = true;
            }
        }
        auto current_time = std::chrono::steady_clock::now();
        for (int i = 1; i < total_ranks; ++i) {
            if (node_heartbeat_status[i]) {
                auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                                    current_time - last_heartbeat_time[i]).count();
                if (duration > HEARTBEAT_TIMEOUT) {
                    std::lock_guard<std::mutex> lock(system_mutex);
                    node_status[i] = false;
                    node_heartbeat_status[i] = false;
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

// Private: Stop heartbeat threads gracefully.
void DistributedFileSystem::stop_heartbeat_threads() {
    is_running = false;
    if (heartbeat_thread.joinable())
        heartbeat_thread.join();
    if (monitor_thread.joinable())
        monitor_thread.join();
}

// Private: Select storage nodes for replicating a file chunk.
std::vector<int> DistributedFileSystem::select_storage_nodes(const std::string &filename, int chunk_number) {
    static std::unordered_map<int, int> node_load;
    if (node_load.empty()) {
        for (int node = 1; node < total_ranks; ++node) {
            node_load[node] = 0;
        }
    }
    std::vector<int> available_nodes;
    for (int node = 1; node < total_ranks; ++node) {
        if (node_status[node]) {
            bool already_holds_chunk = false;
            for (const auto &metadata_pair : file_metadata) {
                for (const auto &chunk : metadata_pair.second.chunks) {
                    if (chunk.filename == filename && chunk.chunk_number == chunk_number &&
                        std::find(chunk.node_ranks.begin(), chunk.node_ranks.end(), node) != chunk.node_ranks.end()) {
                        already_holds_chunk = true;
                        break;
                    }
                }
                if (already_holds_chunk)
                    break;
            }
            if (!already_holds_chunk)
                available_nodes.push_back(node);
        }
    }
    auto compare_node_load = [&node_load](int a, int b) {
        return node_load[a] < node_load[b];
    };
    std::sort(available_nodes.begin(), available_nodes.end(), compare_node_load);
    std::vector<int> replica_nodes;
    for (const auto &node : available_nodes) {
        if (replica_nodes.size() < REPLICATION_FACTOR) {
            replica_nodes.push_back(node);
            node_load[node]++;
        } else {
            break;
        }
    }
    return replica_nodes;
}

// Constructor: Initializes status and starts heartbeat mechanism.
DistributedFileSystem::DistributedFileSystem(int current_rank, int total_process_count)
    : rank(current_rank), total_ranks(total_process_count), is_running(true), mpi_comm(MPI_COMM_WORLD)
{
    for (int i = 1; i < total_ranks; ++i) {
        node_status[i] = true;
        node_heartbeat_status[i] = true;
    }
    start_heartbeat_mechanism();
}

// Destructor: Ensures threads are stopped.
DistributedFileSystem::~DistributedFileSystem() {
    stop();
}

// Stop the distributed file system.
void DistributedFileSystem::stop() {
    is_running = false;
    stop_heartbeat_threads();
}

// Exit the system by notifying all nodes.
void DistributedFileSystem::exit_system() {
    stop();
    int stop_signal = 1;
    for (int i = 1; i < total_ranks; ++i) {
        MPI_Send(&stop_signal, 1, MPI_INT, i, EXIT_TAG, mpi_comm);
    }
}

// Upload a file by breaking it into chunks and sending to storage nodes.
int DistributedFileSystem::upload_file(const std::string &filename, const std::string &filepath) {
    if (rank != 0)
        return -1;
    std::ifstream file(filepath, std::ios::binary);
    if (!file)
        return -1;
    std::vector<char> buffer((std::istreambuf_iterator<char>(file)), {});
    file.close();
    if (buffer.size() > MAX_FILE_SIZE)
        return -1;
    
    FileMetadata metadata;
    metadata.filename = filename;
    metadata.total_chunks = (buffer.size() + CHUNK_SIZE - 1) / CHUNK_SIZE;
    for (int chunk_num = 0; chunk_num < metadata.total_chunks; ++chunk_num) {
        FileChunk chunk;
        chunk.filename = filename;
        chunk.chunk_number = chunk_num;
        std::vector<int> replica_nodes = select_storage_nodes(filename, chunk_num);
        int chunk_start = chunk_num * CHUNK_SIZE;
        int chunk_end = std::min(chunk_start + CHUNK_SIZE, static_cast<int>(buffer.size()));
        std::vector<char> chunk_data(buffer.begin() + chunk_start, buffer.begin() + chunk_end);
        for (int replica_node : replica_nodes) {
            MPI_Send(chunk_data.data(), chunk_data.size(), MPI_CHAR, replica_node, UPLOAD_TAG, mpi_comm);
            int file_name_length = filename.size();
            MPI_Send(&file_name_length, 1, MPI_INT, replica_node, UPLOAD_TAG, mpi_comm);
            MPI_Send(filename.c_str(), file_name_length, MPI_CHAR, replica_node, UPLOAD_TAG, mpi_comm);
            MPI_Send(&chunk_num, 1, MPI_INT, replica_node, UPLOAD_TAG, mpi_comm);
            int chunk_size = chunk_data.size();
            MPI_Send(&chunk_size, 1, MPI_INT, replica_node, UPLOAD_TAG, mpi_comm);
            chunk.node_ranks.push_back(replica_node);
        }
        metadata.chunks.push_back(chunk);
    }
    {
        std::lock_guard<std::mutex> lock(system_mutex);
        file_metadata[filename] = metadata;
    }
    return 1;
}

// List file information (only executed by the metadata server).
void DistributedFileSystem::list_file(const std::string &filename) {
    if (rank != 0)
        return;
    for (auto &pair : file_metadata) {
        for (const auto &chunk : pair.second.chunks) {
            std::vector<int> live_ranks;
            for (int node : chunk.node_ranks) {
                if (node_heartbeat_status[node])
                    live_ranks.push_back(node);
            }
            std::cout << chunk.chunk_number << " " << live_ranks.size() << " ";
            for (int node : live_ranks)
                std::cout << node << " ";
            std::cout << "\n";
        }
    }
}

// Failover: Mark a node as failed.
int DistributedFileSystem::failover(int target_rank) {
    if (rank != 0 || target_rank < 1 || target_rank >= total_ranks)
        return -1;
    is_running = false;
    node_status[target_rank] = false;
    node_heartbeat_status[target_rank] = false;
    return 1;
}

// Recover: Restore a previously failed node.
int DistributedFileSystem::recover(int target_rank) {
    if (rank != 0 || target_rank < 1 || target_rank >= total_ranks)
        return -1;
    node_status[target_rank] = true;
    node_heartbeat_status[target_rank] = true;
    last_heartbeat[target_rank] = std::chrono::steady_clock::now();
    is_running = true;
    return 1;
}

// Retrieve a file by gathering its chunks from available nodes.
std::vector<char> DistributedFileSystem::retrieve_file(const std::string &filename) {
    if (rank != 0)
        return std::vector<char>{'-', '1'};
    std::lock_guard<std::mutex> lock(system_mutex);
    auto it = file_metadata.find(filename);
    if (it == file_metadata.end())
        return std::vector<char>{'-', '1'};
    FileMetadata &metadata = it->second;
    std::vector<char> reassembled_data;
    bool file_complete = true;
    for (const auto &chunk : metadata.chunks) {
        bool chunk_retrieved = false;
        for (int node : chunk.node_ranks) {
            if (node_heartbeat_status[node]) {
                int chunk_size;
                MPI_Send(&chunk.chunk_number, 1, MPI_INT, node, DOWNLOAD_TAG, mpi_comm);
                int file_name_length = filename.size();
                MPI_Send(&file_name_length, 1, MPI_INT, node, DOWNLOAD_TAG, mpi_comm);
                MPI_Send(filename.c_str(), file_name_length, MPI_CHAR, node, DOWNLOAD_TAG, mpi_comm);
                MPI_Status status;
                MPI_Recv(&chunk_size, 1, MPI_INT, node, DOWNLOAD_TAG, mpi_comm, &status);
                std::vector<char> chunk_data(chunk_size);
                MPI_Recv(chunk_data.data(), chunk_size, MPI_CHAR, node, DOWNLOAD_TAG, mpi_comm, &status);
                reassembled_data.insert(reassembled_data.end(), chunk_data.begin(), chunk_data.end());
                chunk_retrieved = true;
                break;
            }
        }
        if (!chunk_retrieved) {
            file_complete = false;
            break;
        }
    }
    if (!file_complete)
        return std::vector<char>{'-', '1'};
    return reassembled_data;
}

// Search for a word in a file.
void DistributedFileSystem::search_file(const std::string &filename, const std::string &word) {
    std::vector<char> data = retrieve_file(filename);
    if (data.size() == 2 && data[0] == '-' && data[1] == '1') {
        std::cout << "-1\n";
        return;
    }
    std::vector<char> word_vector(word.begin(), word.end());
    std::vector<int> offsets;
    size_t pos = 0;
    while (true) {
        auto it = std::search(data.begin() + pos, data.end(), word_vector.begin(), word_vector.end());
        if (it == data.end())
            break;
        bool is_word_boundary = true;
        if (it != data.begin() && !isspace(*(it - 1)) && *(it - 1) != '\0')
            is_word_boundary = false;
        if (it + word_vector.size() != data.end() && !isspace(*(it + word_vector.size())) && *(it + word_vector.size()) != '\0')
            is_word_boundary = false;
        if (is_word_boundary)
            offsets.push_back(it - data.begin());
        pos = it - data.begin() + 1;
    }
    if (offsets.empty()) {
        std::cout << -1 << std::endl;
    } else {
        std::cout << offsets.size() << std::endl;
        for (const auto &offset : offsets)
            std::cout << offset << " ";
        std::cout << std::endl;
    }
}

// Process incoming MPI messages for worker nodes.
void DistributedFileSystem::process_incoming_messages() {
    if (rank > 0) { // Only worker nodes handle these messages
        MPI_Status status;
        int message_tag;
        int flag;
        while (true) {
            MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, mpi_comm, &flag, &status);
            if (!is_running && (status.MPI_TAG != EXIT_TAG && status.MPI_TAG != RECOVER_TAG))
                continue;
            if (!flag)
                continue;
            message_tag = status.MPI_TAG;
            switch (message_tag) {
                case UPLOAD_TAG: {
                    std::vector<char> chunk_data(CHUNK_SIZE);
                    MPI_Recv(chunk_data.data(), CHUNK_SIZE, MPI_CHAR, 0, UPLOAD_TAG, mpi_comm, &status);
                    int file_name_length;
                    MPI_Recv(&file_name_length, 1, MPI_INT, 0, UPLOAD_TAG, mpi_comm, &status);
                    std::vector<char> file_name_buffer(file_name_length);
                    MPI_Recv(file_name_buffer.data(), file_name_length, MPI_CHAR, 0, UPLOAD_TAG, mpi_comm, &status);
                    std::string file_name(file_name_buffer.begin(), file_name_buffer.end());
                    int chunk_id;
                    MPI_Recv(&chunk_id, 1, MPI_INT, 0, UPLOAD_TAG, mpi_comm, &status);
                    int chunk_size;
                    MPI_Recv(&chunk_size, 1, MPI_INT, 0, UPLOAD_TAG, mpi_comm, &status);
                    std::string filename = file_name + "_chunk_" + std::to_string(chunk_id) +
                                           "_node_" + std::to_string(rank) + ".bin";
                    std::ofstream out_file(filename, std::ios::binary);
                    if (out_file.is_open()) {
                        out_file.write(chunk_data.data(), chunk_size);
                        out_file.close();
                    } else {
                        std::cerr << "Node " << rank << " failed to save chunk " << chunk_id
                                  << " of file " << file_name << std::endl;
                    }
                    break;
                }
                case DOWNLOAD_TAG: {
                    int chunk_number;
                    MPI_Recv(&chunk_number, 1, MPI_INT, 0, DOWNLOAD_TAG, mpi_comm, &status);
                    int file_name_length;
                    MPI_Recv(&file_name_length, 1, MPI_INT, 0, DOWNLOAD_TAG, mpi_comm, &status);
                    std::vector<char> file_name_buffer(file_name_length);
                    MPI_Recv(file_name_buffer.data(), file_name_length, MPI_CHAR, 0, DOWNLOAD_TAG, mpi_comm, &status);
                    std::string file_name(file_name_buffer.begin(), file_name_buffer.end());
                    std::string chunk_filename = file_name + "_chunk_" + std::to_string(chunk_number) +
                                                 "_node_" + std::to_string(rank) + ".bin";
                    std::ifstream chunk_file(chunk_filename, std::ios::binary);
                    if (!chunk_file.is_open()) {
                        std::cerr << "Error: Could not open chunk file " << chunk_filename << std::endl;
                        break;
                    }
                    chunk_file.seekg(0, std::ios::end);
                    int chunk_size = chunk_file.tellg();
                    chunk_file.seekg(0, std::ios::beg);
                    MPI_Send(&chunk_size, 1, MPI_INT, status.MPI_SOURCE, DOWNLOAD_TAG, mpi_comm);
                    std::vector<char> chunk_data(chunk_size);
                    chunk_file.read(chunk_data.data(), chunk_size);
                    chunk_file.close();
                    if (chunk_file.gcount() != chunk_size) {
                        std::cerr << "Error: Incomplete chunk read for chunk " << chunk_number << std::endl;
                        break;
                    }
                    MPI_Send(chunk_data.data(), chunk_size, MPI_CHAR, status.MPI_SOURCE, DOWNLOAD_TAG, mpi_comm);
                    break;
                }
                case HEARTBEAT_TAG: {
                    int heartbeat_signal;
                    MPI_Recv(&heartbeat_signal, 1, MPI_INT, 0, HEARTBEAT_TAG, mpi_comm, &status);
                    std::cout << "Node " << rank << " received heartbeat from node " << status.MPI_SOURCE << std::endl;
                    last_heartbeat[status.MPI_SOURCE] = std::chrono::steady_clock::now();
                    break;
                }
                case LIST_FILE_TAG: {
                    std::cout << "Node " << rank << " received request for file listing" << std::endl;
                    std::lock_guard<std::mutex> lock(system_mutex);
                    for (const auto &pair : file_metadata) {
                        MPI_Send(pair.first.c_str(), pair.first.size() + 1, MPI_CHAR, status.MPI_SOURCE, LIST_FILE_TAG, mpi_comm);
                    }
                    const char *end_message = "END";
                    MPI_Send(end_message, strlen(end_message) + 1, MPI_CHAR, status.MPI_SOURCE, LIST_FILE_TAG, mpi_comm);
                    break;
                }
                case FAILOVER_TAG: {
                    int failed_rank;
                    MPI_Recv(&failed_rank, 1, MPI_INT, 0, FAILOVER_TAG, mpi_comm, &status);
                    std::cout << "Node " << rank << " received failover for node: " << failed_rank << std::endl;
                    failover(failed_rank);
                    break;
                }
                case RECOVER_TAG: {
                    int recovered_rank;
                    MPI_Recv(&recovered_rank, 1, MPI_INT, 0, RECOVER_TAG, mpi_comm, &status);
                    std::cout << "Node " << rank << " received recovery for node: " << recovered_rank << std::endl;
                    recover(recovered_rank);
                    break;
                }
                case EXIT_TAG: {
                    stop();
                    return;
                }
                default:
                    std::cout << "Node " << rank << " received an unknown message tag: " << message_tag << std::endl;
                    break;
            }
        }
    }
}
