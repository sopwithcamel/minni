#ifndef LIB_BUCKETAGGREGATOR_H
#define LIB_BUCKETAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "AccumulatorFilter.h"
#include "Deserializer.h"
#include "DFSReader.h"
#include "ElasticObject.h"
#include "FileChunkerFilter.h"
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "LocalReader.h"
#include "Mapper.h"
#include "PAOCreator.h"
#include "PartialAgg.h"
#include "Serializer.h"
#include "TokenizerFilter.h"
#include "util.h"

class BucketAggregator : 
    public Aggregator {
  public:
    BucketAggregator(const Config &cfg, 
                JobID jid,
                AggType type, 
                const uint64_t num_part,
                MapInput* _map_input,
                const char* infile, 
                Operations* ops,
                const char* outfile);
    ~BucketAggregator();
  private:
    /* for chunk input from DFS */
    MapInput* map_input; 
    DFSReader* chunkreader;
    FileReaderFilter* filereader;
    FileTokenizerFilter* filetoker;
    TokenizerFilter* toker;
    FileChunkerFilter* filechunker;

    /* for serialized PAOs from local file */
    LocalReader* localreader_;
    const char* infile_;
    Deserializer* inp_deserializer_;

    PAOCreator* creator_;
    Serializer* bucket_serializer_;
    Deserializer* deserializer_;
	AccumulatorInserter* acc_int_inserter_;
	AccumulatorInserter* bucket_inserter_;
    Serializer* final_serializer_;
    uint64_t num_buckets;
    const char* outfile_;
};

#endif // LIB_BUCKETAGGREGATOR_H
