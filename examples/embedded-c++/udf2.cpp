#include "duckdb.hpp"

#include <iostream>
#include <random>
#include <string>

using namespace duckdb;

template <typename TYPE, int NUM_INPUT>
static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<int>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	auto tmp_data3 = ConstantVector::GetData<TYPE>(input.data[2]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = static_cast<int>(3 * tmp_data1[i] + 3.5 * tmp_data2[i] + 4 * tmp_data3[i]) % 4;
	}
}

void create_feature_table(Connection con, int n = 100) {
	con.Query("CREATE TABLE feature (i INTEGER, f1 FLOAT, f2 FLOAT, f3 FLOAT, label INT)");

	std::random_device rd;  // 使用随机设备作为种子
	std::mt19937 gen(rd()); // 使用 Mersenne Twister 引擎
	std::gamma_distribution<float> dis(2, 10);

	std::stringstream ss;
	ss << "INSERT INTO feature VALUES (1, 3.4, 5.3, 9.3, 3)";
	for (int i = 2; i <= n; i++) {
		float x1 = dis(gen);
		float x2 = dis(gen);
		float x3 = dis(gen);
		int label = static_cast<int>(3 * x1 + 3.5 * x2 + 4 * x3) % 4;
		ss << ", (";
		ss << i;
		ss << ", ";
		ss << x1;
		ss << ", ";
		ss << x2;
		ss << ", ";
		ss << x3;
		ss << ", ";
		ss << label;
		ss << ")";
	}
	con.Query(ss.str());
	printf("create feature finished!\n");
}

void create_label_table(Connection con) {
	con.Query("CREATE TABLE label (id INTEGER, name VARCHAR(20))");
	std::stringstream ss;
	ss << "INSERT INTO label VALUES (0, 'yellow'), (1, 'red'), (2, 'black'), (3, 'white')";
	con.Query(ss.str());
	printf("create label finished!\n");
}

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("SET threads = 1;");
	create_label_table(con);
	create_feature_table(con, 100);
	// con.Query("SELECT * FROM feature JOIN label ON feature.label == label.id WHERE feature.label % 2 == 1")->Print();
	con.CreateVectorizedFunction<int, float, float, float>("udf_vectorized_int", &udf_tmp<float, 3>);
	con.Query("SELECT i, udf_vectorized_int(f1, f2, f3) as predict, feature.label as label, label.name as class FROM feature JOIN label ON "
	          "udf_vectorized_int(f1, f2, f3) == label.id WHERE udf_vectorized_int(f1, f2, f3)%2==1")
	    ->Print();
	// con.Query("SELECT i FROM data WHERE i%2==0")->Print();
	return 0;
}