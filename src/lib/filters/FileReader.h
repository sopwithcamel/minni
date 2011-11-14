#ifndef LIB_FILEREADER_H
#define LIB_FILEREADER_H

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

class FileReader : public tbb::filter {
public:
	FileReader(Aggregator* agg, MapInput* _input);
	~FileReader();
private:
	Aggregator* aggregator;
	FileInput* input;
	vector<string> file_list;
	size_t files_per_call;
	size_t files_sent;
	char*** file_content_list;
	char*** file_name_list;
	FilterInfo** send;
	uint64_t next_buffer;
	void* operator()(void* pao);
};

#endif // LIB_FILEREADER_H
