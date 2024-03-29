#ifndef LIB_FILETOKENIZERFILTER_H
#define LIB_FILETOKENIZERFILTER_H

#include <stdlib.h>
#include <assert.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "MemCache.h"
#include "PartialAgg.h"
#include "Aggregator.h"
#include "Util.h"
#include "FileTokenizer.h"

/**
 * To be used after Reader in the pipeline.
 * - Consumes a buffer 
 * - Produces a vector of key, value pairs
 * 
 */

class FileTokenizerFilter : public tbb::filter {
public:
	FileTokenizerFilter(Aggregator* agg, const Config& cfg, MapInput* inp,
			size_t max_keys);
	~FileTokenizerFilter();
private:
	Aggregator* aggregator;
	MemCache* memCache;
	FileTokenizer* file_tokenizer;
	size_t next_buffer;
	const size_t max_keys_per_token;
	MultiBuffer<FilterInfo>* send;
	MultiBuffer<Token*>* tokens;
	MultiBuffer<char*>* contents;
	MultiBuffer<size_t>* content_list_sizes;
	MapInput* input;
	void* operator()(void* input_data);
};

#endif // LIB_FILETOKENIZERFILTER_H
