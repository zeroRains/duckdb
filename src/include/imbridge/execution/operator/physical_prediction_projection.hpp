#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

namespace imbridge {

#define DEFAULT_RESERVED_CAPACITY STANDARD_VECTOR_SIZE*4
#define INITIAL_PREDICTION_SIZE DEFAULT_PREDICTION_BATCH_SIZE

class PhysicalPredictionProjection : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PREDICTION_PROJECTION;

public:
	PhysicalPredictionProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                   idx_t estimated_cardinality, idx_t user_defined_size = INITIAL_PREDICTION_SIZE);

	vector<unique_ptr<Expression>> select_list;
	idx_t user_defined_size;
	bool use_adaptive_size;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	bool RequiresFinalExecute() const {
		return true;
	}

	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const final;

	string ParamsToString() const override;
    
};

} // namespace imbridge

}// namespace duckdb