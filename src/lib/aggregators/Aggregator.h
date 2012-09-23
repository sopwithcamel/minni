#ifndef LIB_AGGREGATOR_H
#define LIB_AGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "daemon_types.h"
#include "Defs.h"
#include "PartialAgg.h"
#include "util.h"

using namespace workdaemon;
using namespace libconfig;

enum AggType {Map, Reduce};

class Aggregator 
{
  public:
	Aggregator(const Config &cfg,
            JobID jid,
			AggType where,
			uint64_t num_pipelines, 
			uint64_t num_part, 
			Operations* _ops);
	virtual ~Aggregator();
	virtual void runPipeline();
    const Operations* const ops() const;

	tbb::pipeline* pipeline_list;	
	tbb::task_scheduler_init* init;
	bool input_finished;		// indicates if input stage is done
    // set to true by filters once they are ready to terminate
    bool can_exit;
    bool stall_pipeline;
	uint64_t tot_input_tokens;	// measures total input tokens

	void resetFlags();
    JobID getJobID() const;
	uint64_t getNumThreads() const;
	uint64_t getNumBuffers() const;
	uint64_t getNumPartitions() const;
	AggType getType() const;
	virtual bool repeatPipeline(uint64_t it);
  protected:
    JobID jobid;
	AggType type;
	uint64_t num_threads;
	uint64_t num_buffers;
	const uint64_t num_pipelines; 	// number of pipelines in aggregator
	const uint64_t num_partitions;	// number of partitions for map output
    const Operations* const ops_;
};

#endif // LIB_AGGREGATOR_H
