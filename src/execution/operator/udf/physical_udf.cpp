#include "duckdb/execution/operator/udf/physical_udf.hpp"


#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

class UDFState : public OperatorState {
public:
	explicit UDFState(ExecutionContext &context)
	    : executor(context.client) {
	}

	ExpressionExecutor executor;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "udf", 0);
	}
};

PhysicalUDF::PhysicalUDF(vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UDF, std::move(types), estimated_cardinality) {
}

OperatorResultType PhysicalUDF::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<UDFState>();
	// state.executor.Execute(input, chunk); // TODO: 这里有待修改
    chunk.Append(input, true);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalUDF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<UDFState>(context);
}

string PhysicalUDF::ParamsToString() const {
	string extra_info;
	extra_info += "233 \n";
	return extra_info;
}

} // namespace duckdb
