#include "duckdb.hpp"

using namespace duckdb;

template <typename TYPE, int NUM_INPUT>
static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<int>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	auto tmp_data3 = ConstantVector::GetData<TYPE>(input.data[2]);
    auto tmp_data4 = ConstantVector::GetData<TYPE>(input.data[3]);
    auto tmp_data5 = ConstantVector::GetData<TYPE>(input.data[4]);
    auto tmp_data6 = ConstantVector::GetData<TYPE>(input.data[5]);
    auto tmp_data7 = ConstantVector::GetData<TYPE>(input.data[6]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = tmp_data1[i] + tmp_data2[i] + tmp_data3[i] + tmp_data4[i] + tmp_data5[i] + tmp_data6[i] + tmp_data7[i];
	}
}

int main() {
	DuckDB db("/root/duckdb_test/imbridge.db");

	Connection con(db);
    con.CreateVectorizedFunction<int64_t, double, double, double, double, double, double, double>("udf", &udf_tmp<double, 7>);
	auto result = con.Query("select serial_number, udf(smart_5_raw, smart_10_raw, smart_184_raw, smart_187_raw, "
	                        "smart_188_raw, smart_197_raw, smart_198_raw) from Failures;");
	result->Print();
}
