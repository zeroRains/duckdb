#include "duckdb.hpp"
#include "duckdb/common/types.hpp"

#include <iostream>

using namespace duckdb;

template <typename TYPE, int NUM_INPUT>
static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	// std::cout << input.size() << std::endl;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<int>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = tmp_data1[i] + tmp_data2[i];
	}
	std::cout << input.size() << std::endl;
}

int main() {
	DuckDB db("/root/db/duckdb_test/imbridge2_scale1.db");

	Connection con(db);
	con.Query("SET threads TO 1;");
	con.CreateVectorizedFunction(
	    "udf",
	    {LogicalType::DOUBLE,  LogicalType::DOUBLE,  LogicalType::DOUBLE,  LogicalType::DOUBLE,  LogicalType::DOUBLE,
	     LogicalType::DOUBLE,  LogicalType::DOUBLE,  LogicalType::DOUBLE,  LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::BIGINT,  LogicalType::BOOLEAN, LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::VARCHAR,
	     LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT,
	     LogicalType::BIGINT,  LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	    LogicalType::BIGINT, &udf_tmp<double, 28>);
	auto result = con.Query(
	    "explain analyze SELECT udf(prop_location_score1, prop_location_score2, prop_log_historical_price, price_usd,"
	    "orig_destination_distance, prop_review_score, avg_bookings_usd, stdev_bookings_usd,"
	    "position, prop_country_id, prop_starrating, prop_brand_bool, count_clicks, count_bookings,"
	    "year, month, weekofyear, time, site_id, visitor_location_country_id, srch_destination_id,"
	    "srch_length_of_stay, srch_booking_window, srch_adults_count, srch_children_count,"
	    "srch_room_count, srch_saturday_night_bool, random_bool) "
	    "FROM Expedia_S_listings_extension JOIN Expedia_R1_hotels ON Expedia_S_listings_extension.prop_id = "
	    "Expedia_R1_hotels.prop_id "
	    "JOIN Expedia_R2_searches ON Expedia_S_listings_extension.srch_id = Expedia_R2_searches.srch_id WHERE "
	    "prop_location_score1 > 1 and prop_location_score2 > 0.1 "
	    "and prop_log_historical_price > 4 and count_bookings > 5 "
	    "and srch_booking_window > 10 and srch_length_of_stay > 1;");
	// auto result = con.Query("select count(serial_number) from Failures;");
	result->Print();
}
