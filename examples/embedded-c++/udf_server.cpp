#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_transform_util.hpp"

#include <arrow/python/pyarrow.h>
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
	std::cout << "[Server] start " << channel_name << " server\n";
	if (!Py_IsInitialized()) {
		Py_Initialize();
	}
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();
	if (arrow::py::import_pyarrow()) {
		std::cout << "[Server] import pyarrow error! make sure your default python environment has installed the pyarrow\n";
		exit(0);
	}
	imbridge::SharedMemoryManager shm_server(channel_name, imbridge::ProcessKind::SERVER);
	while (true) {
		shm_server.sem_server->wait();
		std::cout << "[Server] read shared table \n";
		std::shared_ptr<arrow::Table> my_table = imbridge::ReadArrowTableFromSharedMemory(shm_server, INPUT_TABLE);
		std::string python_code = R"(
import pyarrow as pa

def process_table(table):
    col1 = table.column(0).to_pylist()
    col2 = table.column(1).to_pylist()
    result = [a + b for a, b in zip(col1, col2)]
    result_field = pa.field('result', pa.float64())
    result_schema = pa.schema([result_field])
    result_array = pa.array(result, type=pa.float64())
    result_table = pa.Table.from_arrays([result_array], schema=result_schema)
    return result_table

class MyTable:
    def __init__(self, table):
        self.table = table
        self.name = "MyTable"
        self.age = 18


    def process(self, table):
        # print("2333")
        print(self.name)
        print(self.age)
        return process_table(table)
)";
		PyObject *py_table_tmp = arrow::py::wrap_table(std::move(my_table));
		PyObject *py_main = PyImport_AddModule("__main__");
		PyObject *py_dict = PyModule_GetDict(py_main);

		PyRun_SimpleString(python_code.c_str());

		PyObject *main_module = PyImport_AddModule("__main__");
		PyObject *main_dict = PyModule_GetDict(main_module);
		PyObject *MyTable = PyDict_GetItemString(main_dict, "MyTable");

		PyObject *args = PyTuple_Pack(1, py_table_tmp);
		PyObject *my_table_instance = PyObject_CallObject(MyTable, args);
		PyObject *py_result = PyObject_CallMethod(my_table_instance, "process", "O", py_table_tmp);

		if (py_result != NULL){
			my_table = arrow::py::unwrap_table(py_result).ValueOrDie();
		}

		std::cout << "[Server] write shared table \n";
		imbridge::WriteArrowTableToSharedMemory(my_table, shm_server, OUTPUT_TABLE);
		std::cout << "[Server] server end\n";
		shm_server.sem_client->post();
	}
	return 0;
}