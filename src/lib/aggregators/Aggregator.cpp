#include "Aggregator.h"

uint64_t PartialAgg::createCtr = 0;
uint64_t PartialAgg::destCtr = 0;

Aggregator::Aggregator(const Config &cfg,
            JobID jid,
			AggType where,
			uint64_t num_pipelines, 
			uint64_t num_part,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p)) :
        jobid(jid),
		type(where),
		num_pipelines(num_pipelines),
		num_partitions(num_part),
		input_finished(false),
        sendNextToken(true),
		tot_input_tokens(0),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc)
{
	Setting& c_num_threads = readConfigFile(cfg, "minni.tbb.threads");
	num_threads = c_num_threads;

//	Setting& c_num_buffers = readConfigFile(cfg, "minni.tbb.buffers");

	init = new tbb::task_scheduler_init();
	num_buffers = init->default_num_threads();
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
        pipeline_list[i].clear();
		TimeLog::addTimeStamp("Pipeline completed");
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
    sendNextToken = true;
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

AggType Aggregator::getType() const
{
	return type;
}

bool Aggregator::repeatPipeline(uint64_t it)
{
	return false;
}
