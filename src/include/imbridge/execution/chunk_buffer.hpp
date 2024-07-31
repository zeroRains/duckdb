#pragma once
#include "duckdb/common/vector.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

namespace imbridge {

enum class ChunkBufferState: uint8_t { EMPTY, SLICING, BUFFERRING };
    
class ChunkBuffer {
public:
	ChunkBuffer();
	~ChunkBuffer();
    void Initialize(Allocator &allocator, const vector<LogicalType> &types, idx_t capacity);
    void ResetBuffer();
    void PushChunk(const DataChunk &other);
    void PushChunk(const DataChunk &other, idx_t start_offset, idx_t count);
    DataChunk & NextBatch(idx_t required);
    bool HasNext(idx_t required);
public:
    ChunkBufferState GetState();
    void SetState(ChunkBufferState new_state);
    idx_t GetSize();
public:
    // helper method for external chunk reset
    void ExternalChunkReset(DataChunk &input);
    // helper method for batch adpater, keep the output batch size <= STANDARD_VECTOR_SIZE
    void BatchAdapting(DataChunk &input, DataChunk &output, idx_t start_offset, idx_t size=STANDARD_VECTOR_SIZE);

private:
    void InternalVecShift(Vector &vec, data_ptr_t data_view, idx_t offset);
    void InternalSlicing(DataChunk &source, DataChunk &target, idx_t low, idx_t high);
private:
    DataChunk store;
    DataChunk sliced;
    idx_t base_offset;
    idx_t high_offset;
    ChunkBufferState state;
};

} // namespace imbridge

} // namespace duckdb
