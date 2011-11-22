#ifndef LIB_FILETOKENIZER_H
#define LIB_FILETOKENIZER_H

#include <stdlib.h>
#include <assert.h>
#include <iostream>
#include <libconfig.h++>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "MemCache.h"
#include "PartialAgg.h"
#include "Aggregator.h"
#include "Util.h"

/**
 * To be used after Reader in the pipeline.
 * - Consumes a buffer 
 * - Produces a vector of key, value pairs
 * 
 */

class FileTokenizer : public tbb::filter {
public:
	FileTokenizer(Aggregator* agg, const Config& cfg,
			PartialAgg* (*createPAOFunc)(char** t, size_t* ts),
			const size_t max_keys = DEFAULT_MAX_KEYS_PER_TOKEN);
	~FileTokenizer();
private:
	Aggregator* aggregator;
	MemCache* memCache;
	size_t next_buffer;
	const size_t max_keys_per_token;
	MultiBuffer<char**>* tok_list;
	MultiBuffer<size_t*>* tok_size_list;
	MultiBuffer<FilterInfo>* send;
	PartialAgg* (*createPAO)(char** token, size_t* ts);
	void* operator()(void* input_data);
};

#endif // LIB_FILETOKENIZER_H
