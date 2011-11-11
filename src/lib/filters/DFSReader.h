#ifndef LIB_DFSREADER_H
#define LIB_DFSREADER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Aggregator.h"
#include "MapInput.h"
#include "Util.h"

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
	ChunkInput* input;
	size_t rem_buffer_size;
	char* buffer;
	uint64_t next_buffer;
	const size_t chunksize;
	char** chunk; 
	FilterInfo** send;
	size_t next_chunk;
	ChunkID id;
	void* operator()(void* pao);
};

#endif // LIB_DFSREADER_H
