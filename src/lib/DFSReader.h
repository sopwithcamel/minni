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

/**
 * To be used as the input stage in the pipeline.
 * - Produces a buffer for consumption
 * 
 */

class DFSReader : public tbb::filter {
public:
	static const size_t n_buffer = NUM_BUFFERS;
	DFSReader(Aggregator* agg, MapInput* _input);
	~DFSReader();
private:
	Aggregator* aggregator;
	size_t chunk_ctr;
	MapInput* input;
	size_t next_buffer;
	char* buf[NUM_BUFFERS];
	char* buffer;
	ChunkID id;
	void* operator()(void* pao);
};

#endif // LIB_DFSREADER_H
