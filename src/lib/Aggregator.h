#ifndef LIB_AGGREGATOR_H
#define LIB_AGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Defs.h"

#define DFS_CHUNK_INPUT			0
#define LOCAL_PAO_INPUT			1

class Aggregator {
public:
	Aggregator(uint64_t num_pipelines, 
			uint64_t _capacity, uint64_t _partid, 
			PartialAgg* (*MapFunc)(const char* t),
			void (*destroyPAOFunc)(PartialAgg* p));
	~Aggregator();
	void runPipeline();

	tbb::pipeline* pipeline_list;	
	tbb::task_scheduler_init init;
	bool input_finished;		// indicates if input stage is done
	uint64_t tot_input_tokens;	// measures total input tokens

	void resetFlags();
private:
	const uint64_t num_pipelines; 	// number of pipelines in aggregator
	const uint64_t partid;	// partition ID
	const uint64_t capacity;	// aggregator capacity
	PartialAgg* (*Map)(const char* token);
	void (*destroyPAO)(PartialAgg* p);
};

#endif // LIB_AGGREGATOR_H
