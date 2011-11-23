#ifndef LIB_FILEREADERFILTER_H
#define LIB_FILEREADERFILTER_H

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

/**
 * To be used as the input stage in the pipeline.
 * - Produces an array of names of files in directory
 * single-buffered for now.
 */

class FileReaderFilter : public tbb::filter {
public:
	FileReaderFilter(Aggregator* agg, MapInput* _input);
	~FileReaderFilter();
private:
	Aggregator* aggregator;
	FileInput* input;
	vector<string> file_list;
	size_t files_per_call;
	size_t files_sent;
	MultiBuffer<char*>* file_names;
	MultiBuffer<FilterInfo>* send;
	uint64_t next_buffer;
	void* operator()(void* pao);
};

#endif // LIB_FILEREADERFILTER_H
