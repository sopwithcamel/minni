#ifndef LIB_FILECHUNKERFILTER_H
#define LIB_FILECHUNKERFILTER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Aggregator.h"
#include "MapInput.h"
#include "Util.h"

#include "DelimitedTokenizer.h"

/**
 * To be used as the input stage in the pipeline.
 * - Produces an array of names of files in directory
 * single-buffered for now.
 */

class FileChunkerFilter : public tbb::filter {
public:
	FileChunkerFilter(Aggregator* agg, MapInput* _input, const Config& cfg,
            const size_t max_keys);
	~FileChunkerFilter();
private:
	Aggregator* aggregator;
	FileInput* input;
	vector<string> file_list;
	size_t files_sent;
	MultiBuffer<FilterInfo>* send;
	MultiBuffer<Token*>* tokens;
	DelimitedTokenizer* chunk_tokenizer;
	uint64_t next_buffer;
	const size_t max_keys_per_token;
	void* operator()(void*);
};

#endif // LIB_FILEREADERFILTER_H
