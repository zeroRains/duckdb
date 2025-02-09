#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

TypeCatalogEntry::TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info.name), user_type(info.type) {
	this->temporary = info.temporary;
	this->internal = info.internal;
	this->comment = info.comment;
}

unique_ptr<CatalogEntry> TypeCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateTypeInfo>();
	auto result = make_uniq<TypeCatalogEntry>(catalog, schema, cast_info);
	return std::move(result);
}

unique_ptr<CreateInfo> TypeCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTypeInfo>();
	result->catalog = catalog.GetName();
	result->schema = schema.name;
	result->name = name;
	result->type = user_type;
	result->comment = comment;
	return std::move(result);
}

string TypeCatalogEntry::ToSQL() const {
	switch (user_type.id()) {
	case (LogicalTypeId::ENUM): {
		auto create_type_info = GetInfo();
		return create_type_info->ToString();
	}
	default:
		throw InternalException("Logical Type can't be used as a User Defined Type");
	}
}

} // namespace duckdb
