#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

namespace imbridge {

class PredictionFuncChecker {

public:
	explicit PredictionFuncChecker(vector<unique_ptr<Expression>>& expressions): expressions(expressions) {
        user_batch_size_map.resize(expressions.size());
    }

    // Check wheather multiple expressions satisfy the optimization constraints,
    // also collect the prediction function info.
    bool CheckExprs(std::function<bool(idx_t)> constraint);

    vector<idx_t> user_batch_size_map;
    set<idx_t> root_idx_list;
    idx_t total_prediction_func_count;

private:

    vector<unique_ptr<Expression>>& expressions;
	void VisitExpression(unique_ptr<Expression> *expression, idx_t root_idx);

	void VisitExpressionChild(Expression &expression, idx_t root_idx);

};

} // namespace imbridge

} // namespace duckdb