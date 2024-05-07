#include "duckdb.hpp"

#include <chrono>
#include <iostream>
#include <random>
#include <string>

using namespace duckdb;

template <typename TYPE, int NUM_INPUT>
static void udf(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<int>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = tmp_data1[i] * tmp_data2[i];
	}
}

int main() {
	vector<pair<int, double>> record;
	DuckDB db("/root/duckdb_test/tpch.db");
    
	Connection con(db);
    bool is_scale = false;
	string sql_str =
	    "Explain analyze select sum(udf(l_extendedprice, l_discount)) as revenue        from lineitem        where "
	    "l_shipdate >= date '1994-01-01'               and l_shipdate < date '1994-01-01' + interval '1' year       "
	    "       and l_discount between  0.06 - 0.01 and  0.06 + 0.01              and l_quantity < 24;";

    // string sql_str =
	//     "Explain analyze select sum(l_extendedprice * l_discount) as revenue        from lineitem        where "
	//     "l_shipdate >= date '1994-01-01'               and l_shipdate < date '1994-01-01' + interval '1' year       "
	//     "       and l_discount between  0.06 - 0.01 and  0.06 + 0.01              and l_quantity < 24;";
    if(is_scale){
	con.CreateVectorizedFunction<double, double, double>("udf", &udf<double, 2>);

	for (int i = 0; i < 3; i++) {
		con.Query(sql_str);
	}
	printf("finished warmup\n");
	for (int i = 1; i <= 32; i++) {
		string thread_sql = "SET threads = " + std::to_string(i) + ";";
		std::cout << thread_sql << std::endl;
		con.Query(thread_sql);
		auto start = std::chrono::high_resolution_clock::now();
		con.Query(sql_str);
		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff = end - start;
		record.push_back({i, diff.count()});
	}
	printf("[");
	for (int i = 0; i < record.size(); i++) {
		printf("[%d, %f]", record[i].first, record[i].second);
		if (i != record.size() - 1) {
			printf(", ");
		}
	}
	printf("]\n");
    }else{
		// con.Query("SET threads = 1;");
        auto start = std::chrono::high_resolution_clock::now();
        con.Query(sql_str);
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - start;
        std::cout<<"time: "<<diff.count()<<std::endl;
    }
	return 0;
}