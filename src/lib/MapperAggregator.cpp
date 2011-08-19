#include "config.h"
#include "MapperAggregator.h"

MapperAggregator::MapperAggregator(uint64_t num_pipelines, 
			uint64_t _capacity, 
			uint64_t _partid,
			PartialAgg* (*MapFunc)(const char* t),
			void (*destroyPAOFunc)(PartialAgg* p)) :
		num_pipelines(num_pipelines),
		capacity(_capacity),
		partid(_partid),
		input_finished(false),
		tot_input_tokens(0),
		Map(MapFunc),
		destroyPAO(destroyPAOFunc)
{
	pipeline_list = new tbb::pipeline[num_pipelines]; 
}

MapperAggregator::~MapperAggregator()
{
	delete[] pipeline_list;
}

void MapperAggregator::runPipeline()
{
	init.initialize(NUM_THREADS);
	for (int i=0; i<num_pipelines; i++) {
		fprintf(stderr, "Running pipeline %d\n", i);
		pipeline_list[i].run(NUM_BUFFERS);
		resetFlags();
	}
}

/**
 * Must be called after the execution of each pipeline if te aggregator
 * consists of multiple pipelines placed serially (the only case we handle
 * right now).
 */
void MapperAggregator::resetFlags()
{
	input_finished = false;
	tot_input_tokens = 0;
}
