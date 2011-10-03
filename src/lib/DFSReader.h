#ifndef LIB_DFSREADER_H
#define LIB_DFSREADER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Mapper.h"

#define BUFSIZE			67108864

/**
 * To be used as the input stage in the pipeline.
 * - Produces a buffer for consumption
 * 
 */

class DFSReader : public tbb::filter {
public:
	DFSReader(Aggregator* agg, MapInput* _input, const size_t cs = 2097152);
	~DFSReader();
private:
	Aggregator* aggregator;
	size_t chunk_ctr;
	MapInput* input;
	size_t rem_buffer_size;
	char* buffer;
	const size_t chunksize;
	char** chunk; 
	size_t next_chunk;
	ChunkID id;
	void* operator()(void* pao);
};

#endif // LIB_DFSREADER_H
