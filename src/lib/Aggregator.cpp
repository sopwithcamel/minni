#include "config.h"
#include "Aggregator.h"

Aggregator::Aggregator(Config* cfg,
			uint64_t num_pipelines, 
			uint64_t _partid,
			PartialAgg* (*createPAOFunc)(const char* t),
			void (*destroyPAOFunc)(PartialAgg* p)) :
		num_pipelines(num_pipelines),
		partid(_partid),
		input_finished(false),
		tot_input_tokens(0),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc)
{
	pipeline_list = new tbb::pipeline[num_pipelines]; 
}

Aggregator::~Aggregator()
{
	delete[] pipeline_list;
}

void Aggregator::runPipeline()
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
void Aggregator::resetFlags()
{
	input_finished = false;
	tot_input_tokens = 0;
}
