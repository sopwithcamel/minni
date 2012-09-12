#ifndef LIB_ITERATIVEAGGREGATOR_H
#define LIB_ITERATIVEAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "AccumulatorFilter.h"
#include "Deserializer.h"
#include "Mapper.h"
#include "PartialAgg.h"
#include "PAOMitosis.h"
#include "Serializer.h"
#include "util.h"

class LocalIterativeAggregator : 
    public Aggregator {
  public:
    LocalIterativeAggregator(const Config &cfg, 
                JobID jid,
                AggType type, 
                const uint64_t num_part,
                MapInput* _map_input,
                const char* infile, 
                Operations* ops,
                const char* outfile);
    ~LocalIterativeAggregator();
    void runPipeline();
  private:
    MapInput* map_input; 
    char* infile_;
    char* outfile_;
    char* tempfile_;
    Deserializer* inp_deserializer_;

    PAOMitosis* pao_splitter_;
	AccumulatorInserter* acc_int_inserter_;

    Serializer* final_serializer_;
    uint32_t iter_;
};

#endif // LIB_BUCKETAGGREGATOR_H
