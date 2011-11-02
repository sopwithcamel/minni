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
#include "Mapper.h"

#define BUFSIZE			67108864

/**
 * To be used as the input stage in the pipeline.
 * - Produces a buffer for consumption
 * 
 */

class FileReader : public tbb::filter {
public:
	FileReader(FILE* _input_file, Aggregator* agg);
	~FileReader();
private:
	Aggregator* aggregator;
	size_t chunk_ctr;
	FILE* input_file;
	size_t next_buffer;
	char** buf;
	char* buffer;
	void* operator()(void* pao);
};

#endif // LIB_FILEREADER_H
