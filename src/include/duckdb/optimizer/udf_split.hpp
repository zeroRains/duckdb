//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/udf_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_udf.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Optimizer;

class UDFSplit{

public:
	explicit UDFSplit() {}

	//! Perform udf split
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

private:
	//! Detect udf
	bool ExpressionHasUDF(unique_ptr<Expression> &expr);
	//! Extract internal udf as a Logical_UDF
	void ExtractInternalUDF(LogicalOperator &op);
	//! Create a new UDFOP for op
	void AppendOperator(LogicalOperator &src, unique_ptr<LogicalOperator> target);
	//! recurve check the child operator
	void VisitOperatorChildren(LogicalOperator &op);
	//! check the expression
	void VisitExpression(unique_ptr<Expression> &op);
	//! recurve check the child expression
	void VisitExpressionChild(unique_ptr<Expression> &op);


	bool is_rewrite = false;
};

} // namespace duckdb
