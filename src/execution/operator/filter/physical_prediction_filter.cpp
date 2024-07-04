#include "duckdb/execution/physical_operator.hpp"
#include "imbridge/execution/operator/physical_prediction_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {
namespace imbridge {

class PredictionFilterState: public PredictionState {
public:
	explicit PredictionFilterState(ExecutionContext &context, Expression &expr,
    const vector<LogicalType> &input_types, idx_t prediction_size = INITIAL_PREDICTION_SIZE, idx_t buffer_capacity = DEFAULT_RESERVED_CAPACITY)
	    : PredictionState(context, input_types, prediction_size, buffer_capacity), executor(context.client, expr), sel(prediction_size) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "prediction_filter", 0);
	}
};

PhysicalPredictionFilter::PhysicalPredictionFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
 idx_t estimated_cardinality, idx_t user_defined_size): PhysicalOperator(PhysicalOperatorType::PREDICTION_FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}

    if(user_defined_size <= 0) {
        this->user_defined_size = INITIAL_PREDICTION_SIZE;
        use_adaptive_size = true; 
    } else {
        this->user_defined_size = user_defined_size;
        use_adaptive_size = false;
    }
}

unique_ptr<OperatorState> PhysicalPredictionFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PredictionFilterState>(context, *expression, children[0]->GetTypes(), user_defined_size);
}

OperatorResultType PhysicalPredictionFilter::Execute(ExecutionContext &context, DataChunk &input,
 DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state) const {
    auto &state_p = state.Cast<PredictionFilterState>();
    idx_t result_count = state_p.executor.SelectExpression(input, state_p.sel);
    if (result_count == input.size()) {
        // nothing was filtered: skip adding any selection vectors
        chunk.Reference(input);
    } else {
        chunk.Slice(input, state_p.sel, result_count);
    }
    return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalPredictionFilter::ParamsToString() const {
    auto result = expression->GetName();
    result += "\n[INFOSEPARATOR]\n";
    result += StringUtil::Format("EC: %llu", estimated_cardinality);
    if(!use_adaptive_size){
        result += StringUtil::Format("\nprediction size: %llu\n", user_defined_size);
    } else {
        result += "\nprediction size: adaptive\n";
    }
    return result;
}

} // namespace imbridge
    

} // namespace duckdb
