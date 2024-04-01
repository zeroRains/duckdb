#include "duckdb/optimizer/udf_split.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> UDFSplit::Rewrite(unique_ptr<LogicalOperator> op) {
	// find the udf in op exepression
	// replace the UDF as op
	VisitOperatorChildren(*op);
	ExtractInternalUDF(*op);
	return std::move(op);
}

void UDFSplit::VisitOperatorChildren(LogicalOperator &op) {
	switch(op.type){
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_DELIM_JOIN: {

		}
	}
	for (auto &child_op : op.children) {
		VisitOperatorChildren(*child_op);
	}
}

bool UDFSplit::ExpressionHasUDF(unique_ptr<Expression> &expr) {
	switch (expr->expression_class) {
	case ExpressionClass::BOUND_FUNCTION:
		return expr->Cast<BoundFunctionExpression>().function.null_handling == FunctionNullHandling::UDF_HANDLING;
	case ExpressionClass::BOUND_COMPARISON:
		return false;
	}
	return false;
}

void UDFSplit::ExtractInternalUDF(LogicalOperator &op) {
	switch ()
		for (auto &child_op : op.children) {
			ExtractInternalUDF(*child_op);
		}
}

void UDFSplit::AppendOperator(LogicalOperator &src, unique_ptr<LogicalOperator> target) {
	// insert the UDF op between the src and children
	target->children = std::move(src.children);
	vector<unique_ptr<LogicalOperator>> new_children;
	new_children.push_back(std::move(target));
	src.children = std::move(new_children);
}

} // namespace duckdb
