#ifndef LIB_ITERATIVEAGGREGATOR_H
#define LIB_ITERATIVEAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Deserializer.h"
#include "Mapper.h"
#include "PartialAgg.h"
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
    Deserializer* inp_deserializer_;

    Serializer* final_serializer_;
    char* outfile_;
    uint32_t iter_;
};

#endif // LIB_BUCKETAGGREGATOR_H
