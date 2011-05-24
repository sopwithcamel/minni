#ifndef LIB_MAPPERAGGREGATOR_H
#define LIB_MAPPERAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Defs.h"

class MapperAggregator {
public:
	MapperAggregator(uint64_t _capacity, uint64_t _partid, 
			PartialAgg* (*MapFunc)(const char* t),
			void (*destroyPAOFunc)(PartialAgg* p));
	~MapperAggregator();
	void runPipeline();
	tbb::pipeline pipeline;	
	tbb::task_scheduler_init init;
private:
	const uint64_t partid;	// partition ID
	const uint64_t capacity;	// aggregator capacity
	PartialAgg* (*Map)(const char* token);
	void (*destroyPAO)(PartialAgg* p);
};

#endif // LIB_MAPPERAGGREGATOR_H
