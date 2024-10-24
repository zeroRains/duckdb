#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>
#include <arrow/status.h>

namespace duckdb {

namespace imbridge {

std::shared_ptr<arrow::Table> ConvertDataChunkToArrowTable(DataChunk &input, const ClientProperties &options) {
	auto types = input.GetTypes();
	vector<string> names;
	names.reserve(types.size());

	for (idx_t i = 0; i < types.size(); i++) {
		names.push_back(StringUtil::Format("c%d", i));
	}

	ArrowSchema schema;
	ArrowConverter::ToArrowSchema(&schema, types, names, options);

	idx_t init_capacity = input.size() > STANDARD_VECTOR_SIZE ? NextPowerOfTwo(input.size()) : STANDARD_VECTOR_SIZE;
	ArrowAppender appender(types, init_capacity, options);
	appender.Append(input, 0, input.size(), input.size());
	ArrowArray array = appender.Finalize();

	std::shared_ptr<arrow::Schema> arrow_schema;
	std::vector<std::shared_ptr<arrow::Array>> arrow_array;

	arrow_schema = arrow::ImportSchema(&schema).ValueOrDie();
	for (int i = 0; i < array.n_children; i++) {
		arrow_array.emplace_back(arrow::ImportArray(array.children[i], schema.children[i]).ValueOrDie());
	}

    std::shared_ptr<arrow::Table> table = arrow::Table::Make(arrow_schema, arrow_array);
    return table;
}

} // namespace imbridge

} // namespace duckdb