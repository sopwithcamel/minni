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
	MapperAggregator(uint64_t _capacity, uint64_t _partid);
	~MapperAggregator();
	void runPipeline();
	tbb::pipeline pipeline;	
	tbb::task_scheduler_init init;
private:
	const uint64_t partid;	// partition ID
	const uint64_t capacity;	// aggregator capacity
};

MapperAggregator::MapperAggregator(uint64_t _capacity, uint64_t _partid) :
		capacity(_capacity),
		partid(_partid)
{
}

MapperAggregator::~MapperAggregator()
{
}

void MapperAggregator::runPipeline()
{
	init.initialize(NUM_THREADS);
	cout << "\t\t\tRunning pipeline!" << endl;
	printf("Addr2: %p\n", &pipeline);
	pipeline.run(NUM_BUFFERS);
}
#endif // LIB_MAPPERAGGREGATOR_H
