#include "config.h"
#include "MapperAggregator.h"

MapperAggregator::MapperAggregator(uint64_t _capacity, 
					uint64_t _partid,
					PartialAgg* (*MapFunc)(const char* t)) :
		capacity(_capacity),
		partid(_partid),
		Map(MapFunc)
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
