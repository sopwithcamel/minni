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
			size_t max_keys);
	virtual ~AccumulatorInserter() {}
	void* operator()(void* pao_list) {}
  protected:
	Aggregator* aggregator_;
    Accumulator* accumulator_;
	const size_t max_keys_per_token;
	uint64_t tokens_processed;
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
	AccumulatorReader(Aggregator* agg,
			Accumulator* acc,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			const char* outfile_prefix);
	~AccumulatorReader();
	void* operator()(void* pao_list) {}
  protected:
	Aggregator* aggregator_;
    Accumulator* accumulator_;

    /* flag that controls whether the PAOs that are read are sent out or
     * written to disk */
    const bool writeToFile_;

    /* fields for sending out read PAOs */
	size_t next_buffer;
	const size_t max_keys_per_token_;
	MultiBuffer<FilterInfo>* send_;
	MultiBuffer<PartialAgg*>* pao_list_;
	size_t (*createPAO_)(Token* t, PartialAgg** p);

	/* Write out fields */
	FILE** fl_;
	char* outfile_;
	char* buf_;
};
#endif // LIB_ACCUMFILTER_H
