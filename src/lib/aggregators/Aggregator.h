#ifndef LIB_AGGREGATOR_H
#define LIB_AGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "util.h"

using namespace libconfig;

enum AggType {Map, Reduce};

class Aggregator 
{
  public:
	Aggregator(const Config &cfg,
			AggType where,
			uint64_t num_pipelines, 
			uint64_t num_part, 
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p));
	~Aggregator();
	void runPipeline();

	tbb::pipeline* pipeline_list;	
	tbb::task_scheduler_init* init;
	bool input_finished;		// indicates if input stage is done
    // set to false by filters that aren't ready to terminate
    bool voteTerminate;
	uint64_t tot_input_tokens;	// measures total input tokens

	void resetFlags();
	uint64_t getNumThreads() const;
	uint64_t getNumBuffers() const;
	uint64_t getNumPartitions() const;
	uint64_t getPAOsPerToken() const;
	AggType getType() const;
	virtual bool repeatPipeline(uint64_t it);
  private:
	AggType type;
	uint64_t num_threads;
	uint64_t num_buffers;
	const uint64_t num_pipelines; 	// number of pipelines in aggregator
	const uint64_t num_partitions;	// number of partitions for map output
	uint64_t paos_in_token;
	size_t (*createPAO)(Token* t, PartialAgg** p);
	void (*destroyPAO)(PartialAgg* p);
};

#endif // LIB_AGGREGATOR_H
