#include "duckdb.hpp"

#include <iostream>
#include <string>

using namespace duckdb;
using namespace imbridge;

template <typename TYPE, int NUM_INPUT>
static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<TYPE>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	std::cout << input.size() << std::endl;
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = 1 * tmp_data1[i] + 0 * tmp_data2[i];
	}
}


int main() {
	printf("?????");
	DuckDB db("/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_tpcx_ai_sf10.db");
	Connection con(db);
	// con.Query("SELECT * FROM data LIMIT 10")->Print();
	
	con.CreateVectorizedFunction<string_t, int64_t, string_t>("udf", &udf_tmp<double, 2>,
	                                                     LogicalType::INVALID, FunctionKind::COMMON, 4096);
    
	std::string sql = R"(
explain analyze  select store, department, udf(store, department) 
from (select store, department 
from Order_o Join Lineitem on Order_o.o_order_id = Lineitem.li_order_id
Join Product on li_product_id=p_product_id 
group by store,department);
	)";
	clock_t start_time = clock();
	con.Query(sql)->Print();
	clock_t end_time = clock();
	printf("finished execute %lf s!\n", (double)(end_time - start_time) / CLOCKS_PER_SEC);
	// con.Query("SELECT i FROM data WHERE i%2==0")->Print();
	return 0;
}