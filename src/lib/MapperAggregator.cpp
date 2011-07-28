#include "config.h"
#include "MapperAggregator.h"

MapperAggregator::MapperAggregator(uint64_t _capacity, 
					uint64_t _partid,
					PartialAgg* (*MapFunc)(const char* t),
					void (*destroyPAOFunc)(PartialAgg* p)) :
		capacity(_capacity),
		partid(_partid),
		Map(MapFunc),
		destroyPAO(destroyPAOFunc)
{
}

MapperAggregator::~MapperAggregator()
{
}

void MapperAggregator::runPipeline()
{
	init.initialize(NUM_THREADS);
	pipeline.run(NUM_BUFFERS);
}
