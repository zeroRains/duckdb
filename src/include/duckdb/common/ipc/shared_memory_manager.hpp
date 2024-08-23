//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/ipc/shared_memory_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

namespace bi = boost::interprocess;

namespace duckdb {
namespace imbridge {

enum class ProcessKind : u_int8_t { CLIENT = 0, SERVER = 1 };

class SharedMemoryManager {
public:
	SharedMemoryManager(const std::string &name, ProcessKind process_kind, const size_t size = 1024 * 1024 * 16);

	template <typename T>
	T *create_shared_memory_object(const std::string &name, const size_t size);

	template <typename T>
	std::pair<T *, size_t> open_shared_memory_object(const std::string &name);

	template <typename T>
	void destroy_shared_memory_object(const std::string &name);

	std::string get_channel_name() {
		return channel_name;
	}

	void distroy_shared_memory() {
		bi::shared_memory_object::remove(channel_name.c_str());
	}

	bool is_alive() {
		return *alive;
	}
	void close_server() {
		if(kind != ProcessKind::CLIENT) {
			throw std::runtime_error("[Shared Memory] close_error! It can close server when process kind is CLIENT");
			return;
		}
		*alive = false;
	}

	bi::interprocess_semaphore *sem_client;
	bi::interprocess_semaphore *sem_server;

private:
	bi::managed_shared_memory segment;
	std::string channel_name; // will be the threads id
	ProcessKind kind;         // client or server
	size_t size;              // size of the shared memory
	bool *alive;
};

} // namespace imbridge
} // namespace duckdb
