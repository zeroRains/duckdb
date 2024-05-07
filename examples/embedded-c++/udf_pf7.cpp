#include "duckdb.hpp"

#include <LightGBM/c_api.h>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace duckdb;

BoosterHandle handle;

template <typename TYPE, int NUM_INPUT>
static void udf(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<TYPE>(result);
	input.Flatten();
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));

	vector<double *> data(NUM_INPUT);
	for (int i = 0; i < NUM_INPUT; i++) {
		data[i] = ConstantVector::GetData<TYPE>(input.data[i]);
	}
	vector<double> in(input.size() * NUM_INPUT, std::numeric_limits<TYPE>::min());
	for (int i = 0; i < input.size(); i++) {
		for (int j = 0; j < NUM_INPUT; j++) {
			in[i * NUM_INPUT + j] = data[j][i];
		}
	}
	void *in_p = static_cast<void *>(in.data());
	int64_t out_len;
	LGBM_BoosterPredictForMat(handle, in_p, C_API_DTYPE_FLOAT32, input.size(), 29, 1, C_API_PREDICT_NORMAL, 50, 1, "",
	                          &out_len, result_data);
}

int main() {
	int p = 1;
	LGBM_BoosterCreateFromModelfile("/root/duckdb_test/test_raven/Credit_Card/creditcard_lgb_model.txt", &p, &handle);

	DuckDB db("/root/duckdb_test/imbridge2.db");
	Connection con(db);
	vector<pair<int, double>> record;

	bool is_scale = true;
	int warmup_time = 3;

	// con.Query("SELECT * FROM feature JOIN label ON feature.label == label.id WHERE feature.label % 2 == 1")->Print();
	con.CreateVectorizedFunction<int64_t, double, double, double, double, double, double, double, double, double,
	                             double, double, double, double, double, double, double, double, double, double, double,
	                             double, double, double, double, double, double, double, double, double>(
	    "udf", &udf<double, 29>);
	string sql_str =
	    "Explain analyze SELECT Time, Amount, udf(V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, "
	    "V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, Amount) AS Class FROM "
	    "Credit_Card_extension  WHERE V1 > 1 AND V2 < 0.27 AND V3 > 0.3;";

	// warm up
	for (int i = 0; i < warmup_time; i++) {
		con.Query(sql_str);
	}
	if (is_scale) {
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
	} else {
		string thread_str = "SET threads = 32;";
		std::cout << thread_str << std::endl;
		con.Query(thread_str);
		auto start = std::chrono::high_resolution_clock::now();
		con.Query(sql_str);
		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double> diff = end - start;
		std::cout << "time: " << diff.count() << std::endl;
	}
	return 0;
}