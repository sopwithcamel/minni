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
	Sorter(const uint64_t num_buckets, const char* inp_prefix);
	~Sorter();
private:
	uint64_t buckets_processed;
	const uint64_t num_buckets;
	const char* inputfile_prefix;
	void* operator()(void*);
};

#endif // LIB_SORTER_H
