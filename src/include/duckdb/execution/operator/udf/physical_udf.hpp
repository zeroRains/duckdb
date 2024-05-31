//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/udf/physical_udf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class PhysicalUDF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UDF;

public:
	PhysicalUDF(vector<LogicalType> types, idx_t estimated_cardinality);

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string ParamsToString() const override;
};

} // namespace duckdb
