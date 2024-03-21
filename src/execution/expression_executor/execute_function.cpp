#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

ExecuteFunctionState::ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root)
    : ExpressionState(expr, root) {
}

ExecuteFunctionState::~ExecuteFunctionState() {
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExecuteFunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	if (expr.function.init_local_state) {
		result->local_state = expr.function.init_local_state(*result, expr, expr.bind_info.get());
	}
	return std::move(result);
}

static void VerifyNullHandling(const BoundFunctionExpression &expr, DataChunk &args, Vector &result) {
#ifdef DEBUG
	if (args.data.empty() || expr.function.null_handling != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return;
	}

	// Combine all the argument validity masks into a flat validity mask
	idx_t count = args.size();
	ValidityMask combined_mask(count);
	for (auto &arg : args.data) {
		UnifiedVectorFormat arg_data;
		arg.ToUnifiedFormat(count, arg_data);

		for (idx_t i = 0; i < count; i++) {
			auto idx = arg_data.sel->get_index(i);
			if (!arg_data.validity.RowIsValid(idx)) {
				combined_mask.SetInvalid(i);
			}
		}
	}

	// Default is that if any of the arguments are NULL, the result is also NULL
	UnifiedVectorFormat result_data;
	result.ToUnifiedFormat(count, result_data);
	for (idx_t i = 0; i < count; i++) {
		if (!combined_mask.RowIsValid(i)) {
			auto idx = result_data.sel->get_index(i);
			D_ASSERT(!result_data.validity.RowIsValid(idx));
		}
	}
#endif
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	if (!state->types.empty()) {
		for (idx_t i = 0; i < expr.children.size(); i++) {
			D_ASSERT(state->types[i] == expr.children[i]->return_type);
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
#ifdef DEBUG
			if (expr.children[i]->return_type.id() == LogicalTypeId::VARCHAR) {
				arguments.data[i].UTFVerify(count);
			}
#endif
		}
		arguments.Verify();
	}
	arguments.SetCardinality(count);
	D_ASSERT(expr.function.function);

	// differetiate UDF and common function like +, -, * and /
	if (expr.function.null_handling == FunctionNullHandling::SPECIAL_HANDLING) {
		idx_t dibs = 300;
		if (!save_chunk.size()) {
			save_chunk.Initialize(*context.get(), arguments.GetTypes());
			save_chunk.Reset();
			current_chunk.Initialize(*context.get(), arguments.GetTypes());
			context.get()->udf_count = std::max(context.get()->udf_count + 1, 1);
		}
		// merge the batch data_chunk in save_chunk
		if (count > 0) {
			save_chunk.Append(arguments, true);
		}
		// splite save_chunk to fit the desirable inference batch size
		nums = save_chunk.size() - index;
		if (nums >= dibs || count == 0) {
			state->profiler.BeginSample();
			nums = dibs > nums && count == 0 ? nums : dibs;
			SelectionVector tmp_sel(index, nums);
			current_chunk.Slice(save_chunk, tmp_sel, nums);
			index += nums;
			expr.function.function(current_chunk, *state, result);
			state->profiler.EndSample(current_chunk.size());
			VerifyNullHandling(expr, current_chunk, result);
			// No data in save_chunk
			if (count == 0 && index == save_chunk.size()) {
				context.get()->udf_count -= 1;
			}
		}
	} else {
		state->profiler.BeginSample();
		expr.function.function(arguments, *state, result);
		state->profiler.EndSample(count);
		VerifyNullHandling(expr, arguments, result);
	}

	D_ASSERT(result.GetType() == expr.return_type);
}

} // namespace duckdb
