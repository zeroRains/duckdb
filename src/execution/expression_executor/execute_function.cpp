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
		const idx_t DIBS = 2400;
		// init current_chunk
		if (!is_init_current_chunk) {
			current_chunk.Initialize(*context.get(), arguments.GetTypes());
			context.get()->udf_count = std::max(context.get()->udf_count + 1, 1);
			is_init_current_chunk = true;
		}
		// merge the batch data_chunk in save_chunk
		if (count > 0 && current_chunk.size() < DIBS) {
			if (current_chunk.size() + arguments.size() > DIBS) {
				chunk_offset = DIBS - current_chunk.size();
				SelectionVector tmp(0, chunk_offset);
				current_chunk.Append(arguments, true, &tmp, chunk_offset);
			} else {
				current_chunk.Append(arguments, true);
			}
		}
		// splite save_chunk to fit the desirable inference batch size
		if (current_chunk.size() == DIBS || count == 0) {
			nums = current_chunk.size();

			state->profiler.BeginSample();
			expr.function.function(current_chunk, *state, result);

			current_chunk.Reset();
			idx_t left = arguments.size() - chunk_offset;

			while (left) {
				if (left >= DIBS) { 
					SelectionVector tmp(chunk_offset, DIBS);
					current_chunk.Append(arguments, true, &tmp, DIBS);
					Vector tmp_res(result.GetType(), DIBS);
					expr.function.function(current_chunk, *state, tmp_res);
					result.Resize(nums, nums + DIBS);
					VectorOperations::Copy(tmp_res, result, DIBS, 0, nums);
					nums += DIBS;
					left -= DIBS;
					chunk_offset = left ? chunk_offset + DIBS : 0;
					current_chunk.Reset();
				} else { 
					idx_t current_count = arguments.size() - chunk_offset;
					SelectionVector tmp(chunk_offset, current_count);
					current_chunk.Append(arguments, true, &tmp, current_count);
					chunk_offset = 0;
					left = 0;
				}
			}
			state->profiler.EndSample(nums);
			VerifyNullHandling(expr, current_chunk, result);
			// No data in current_chunk
			if (count == 0 && current_chunk.size() == 0) {
				context.get()->udf_count -= 1;
			}
		} else {
			nums = 0;
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
