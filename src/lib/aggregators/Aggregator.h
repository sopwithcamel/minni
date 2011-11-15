#ifndef LIB_AGGREGATOR_H
#define LIB_AGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>
#include <libconfig.h++>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Defs.h"
#include "util.h"

using namespace libconfig;

enum AggType {Map, Reduce};

class Aggregator {
public:
	Aggregator(const Config &cfg,
			AggType where,
			uint64_t num_pipelines, 
			uint64_t num_part, 
			PartialAgg* (*createPAOFunc)(char** t, size_t* ts),
			void (*destroyPAOFunc)(PartialAgg* p));
	~Aggregator();
	void runPipeline();

	tbb::pipeline* pipeline_list;	
	tbb::task_scheduler_init* init;
	bool input_finished;		// indicates if input stage is done
	uint64_t tot_input_tokens;	// measures total input tokens

	void resetFlags();
	uint64_t getNumThreads() const;
	uint64_t getNumBuffers() const;
	uint64_t getNumPartitions() const;
	uint64_t getPAOsPerToken() const;
	AggType getType() const;
private:
	AggType type;
	uint64_t num_threads;
	uint64_t num_buffers;
	const uint64_t num_pipelines; 	// number of pipelines in aggregator
	const uint64_t num_partitions;	// number of partitions for map output
	uint64_t paos_in_token;
	PartialAgg* (*createPAO)(char** token, size_t* ts);
	void (*destroyPAO)(PartialAgg* p);
};

#endif // LIB_AGGREGATOR_H
