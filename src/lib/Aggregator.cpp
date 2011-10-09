#include "Aggregator.h"

Aggregator::Aggregator(const Config &cfg,
			AggType where,
			uint64_t num_pipelines, 
			uint64_t num_part,
			PartialAgg* (*createPAOFunc)(const char* t),
			void (*destroyPAOFunc)(PartialAgg* p)) :
		type(where),
		num_pipelines(num_pipelines),
		num_partitions(num_part),
		input_finished(false),
		tot_input_tokens(0),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc)
{
	Setting& c_num_threads = readConfigFile(cfg, "minni.tbb.threads");
	num_threads = c_num_threads;

	Setting& c_num_buffers = readConfigFile(cfg, "minni.tbb.buffers");
	num_buffers = c_num_buffers;

	Setting& c_paos_in_token = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
	paos_in_token = c_paos_in_token;

	init = new tbb::task_scheduler_init(num_threads);
	pipeline_list = new tbb::pipeline[num_pipelines]; 
}

Aggregator::~Aggregator()
{
	delete[] pipeline_list;
	delete init;
}

void Aggregator::runPipeline()
{
	for (int i=0; i<num_pipelines; i++) {
		fprintf(stderr, "Running pipeline %d\n", i);
		pipeline_list[i].run(num_buffers);
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

uint64_t Aggregator::getNumThreads(void) const
{
	return num_threads;
}

uint64_t Aggregator::getNumBuffers(void) const
{
	return num_buffers;
}

uint64_t Aggregator::getNumPartitions(void) const
{
	return num_partitions;
}

uint64_t Aggregator::getPAOsPerToken(void) const
{
	return paos_in_token;
}

AggType Aggregator::getType() const
{
	return type;
}
