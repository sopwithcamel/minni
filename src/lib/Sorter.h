#ifndef	LIB_SORTER_H
#define LIB_SORTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Mapper.h"

class Sorter : public tbb::filter {
public:
	Sorter(const uint64_t num_part, 
			const char* inp_prefix,
			const char* out_prefix);
	~Sorter();
private:
	const uint64_t num_part;
	char* inputfile_prefix;
	char* outputfile_prefix;
	void* operator()(void*);
};

#endif // LIB_SORTER_H
