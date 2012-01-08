#ifndef LIB_ACCUMFILTER_H
#define LIB_ACCUMFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Aggregator.h"
#include "Accumulator.h"
#include "PartialAgg.h"
#include "Util.h"

class AccumulatorInserter :
        public tbb::filter 
{
  public:
	AccumulatorInserter(Aggregator* agg,
			Accumulator* acc,
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys) {}
	~AccumulatorInserter() {}
	void* operator()(void* pao_list) {}
private:
	Aggregator* aggregator_;
    Accumulator* accumulator_;
	const size_t max_keys_per_token;
	void (*destroyPAO)(PartialAgg* p);
};

class AccumulatorReader :
        public tbb::filter
{
  public:
	AccumulatorReader(Aggregator* agg,
			Accumulator* acc,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			const size_t max_keys);
	~AccumulatorReader();
	void* operator()(void* pao_list) {}
private:
	Aggregator* aggregator_;
    Accumulator* accumulator_;
	size_t next_buffer;
	const size_t max_keys_per_token;
	MultiBuffer<FilterInfo>* send;
	MultiBuffer<PartialAgg*>* pao_list;
	size_t (*createPAO)(Token* t, PartialAgg** p);
};

class AccumulatorSerializer :
        public tbb::filter {
  public:
	AccumulatorSerializer(Aggregator* agg, 
			Accumulator* acc,
			const char* outfile) {}
	~ExternalHashSerializer() {}
	void* operator()(void*) {}
private:
	Aggregator* aggregator_;
    Accumulator* accumulator_;

	/* Write out fields */
	FILE** fl;
	char* outfile;
	char* buf;

};
#endif // LIB_ACCUMFILTER_H
