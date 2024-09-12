
#include "duckdb.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

using namespace duckdb;
using namespace imbridge;

static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	using TYPE = double;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<TYPE>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	memset(result_data, std::numeric_limits<int64_t>::min(), input.size() * sizeof(int64_t));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = (int)tmp_data1[i];
	}
}

int main() {
	DuckDB db("/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_tpcx_ai_sf10.db");
	Connection con(db);
	con.CreateVectorizedFunction<string_t, int64_t, string_t>("udf", &udf_tmp, LogicalType::INVALID,
	                                                          FunctionKind::PREDICTION, 4096);

	string sql = R"(
select store, department, udf(store, department) 
from (select store, department 
from Order_o Join Lineitem on Order_o.o_order_id = Lineitem.li_order_id
Join Product on li_product_id=p_product_id 
group by store,department);
)";
con.Query(sql)->Print();
	return 0;
}