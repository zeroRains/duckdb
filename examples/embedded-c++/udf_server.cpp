#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_transform_util.hpp"

#include <iostream>
#include <string>

using namespace duckdb;
using namespace imbridge;

int main(int argc, char **argv) {

	if (argc < 2) {
		std::cout << "[Server] you shoule add a parameter\n";
		return 0;
	}
	std::string channel_name = argv[1];
	imbridge::SharedMemoryManager shm_server(channel_name, imbridge::ProcessKind::SERVER);
	std::cout <<"[Server] start " << channel_name << " server\n";
	while (true) {
		shm_server.sem_server->wait();
		std::cout << "[Server] read shared table \n";
		std::shared_ptr<arrow::Table> parse_table = imbridge::ReadArrowTableFromSharedMemory(shm_server, INPUT_TABLE);
		std::cout << "[Server] write shared table \n";
		imbridge::WriteArrowTableToSharedMemory(parse_table, shm_server, OUTPUT_TABLE);
		std::cout << "[Server] server end\n";
		shm_server.sem_client->post();
	}
	return 0;
}