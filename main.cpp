#include "DistributedFileSystem.h"
#include <mpi.h>
#include <sstream>
#include <iostream>
#include <vector>

int main(int argc, char **argv) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank, total_ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &total_ranks);

    DistributedFileSystem dfs(rank, total_ranks);

    if (rank == 0) {
        std::string command;
        while (std::getline(std::cin, command)) {
            std::istringstream iss(command);
            std::string cmd, arg1, arg2;
            iss >> cmd >> arg1 >> arg2;
            if (cmd == "upload" && !arg1.empty() && !arg2.empty()) {
                int result = dfs.upload_file(arg1, arg2);
                std::cout << result << std::endl;
                if (result == 1)
                    dfs.list_file(arg1);
            } else if (cmd == "list_file" && !arg1.empty()) {
                dfs.list_file(arg1);
            } else if (cmd == "failover" && !arg1.empty()) {
                int result = dfs.failover(std::stoi(arg1));
                std::cout << result << std::endl;
            } else if (cmd == "recover" && !arg1.empty()) {
                int result = dfs.recover(std::stoi(arg1));
                std::cout << result << std::endl;
            } else if (cmd == "retrieve" && !arg1.empty()) {
                std::vector<char> ans = dfs.retrieve_file(arg1);
                for (char byte : ans)
                    std::cout << byte;
                std::cout << "\n";
            } else if (cmd == "search" && !arg1.empty() && !arg2.empty()) {
                dfs.search_file(arg1, arg2);
            } else if (cmd == "exit" && arg1.empty()) {
                dfs.exit_system();
                break;
            }
        }
    } else {
        int stop_signal = 0;
        if (stop_signal == 1)
            dfs.stop();
        dfs.process_incoming_messages();
    }
    MPI_Finalize();
    return 0;
}
