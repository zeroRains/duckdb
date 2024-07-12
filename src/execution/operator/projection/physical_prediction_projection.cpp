#include "imbridge/execution/operator/physical_prediction_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

namespace imbridge {

class PredictionProjectionState : public PredictionState {
public:
	explicit PredictionProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions,
    const vector<LogicalType> &input_types, idx_t prediction_size = INITIAL_PREDICTION_SIZE, idx_t buffer_capacity = DEFAULT_RESERVED_CAPACITY)
	    : PredictionState(context, input_types, prediction_size, buffer_capacity), executor(context.client, expressions){
			output_buffer = make_uniq<DataChunk>();
            vector<LogicalType> output_types;

            for(auto & expr: expressions) {
                output_types.push_back(expr->return_type);
            }
            output_buffer->Initialize(Allocator::Get(context.client), output_types,  buffer_capacity);
		}

	ExpressionExecutor executor;
	unique_ptr<DataChunk> output_buffer;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "prediction_projection", 0);
	}
};

PhysicalPredictionProjection::PhysicalPredictionProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                       idx_t estimated_cardinality, idx_t user_defined_size)
    : PhysicalOperator(PhysicalOperatorType::PREDICTION_PROJECTION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)) {
        if(user_defined_size <= 0) {
            this->user_defined_size = INITIAL_PREDICTION_SIZE;
            use_adaptive_size = true; 
        } else {
            this->user_defined_size = user_defined_size;
            use_adaptive_size = false;
        }
}

OperatorResultType PhysicalPredictionProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<PredictionProjectionState>();
	state.executor.Execute(input, chunk);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalPredictionProjection::GetOperatorState(ExecutionContext &context) const {
    D_ASSERT(children.size() == 1);
	return make_uniq<PredictionProjectionState>(context, select_list, children[0]->GetTypes(), user_defined_size);
}

string PhysicalPredictionProjection::ParamsToString() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
    extra_info += use_adaptive_size? "adaptive": "prediction_size:" + std::to_string(user_defined_size) + "\n";
	return extra_info;
}

} // namespace imbridge

} // namespace duckdb