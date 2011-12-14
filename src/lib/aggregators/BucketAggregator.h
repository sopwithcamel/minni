#ifndef LIB_BUCKETAGGREGATOR_H
#define LIB_BUCKETAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Deserializer.h"
#include "DFSReader.h"
#include "ElasticObject.h"
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "Hasher.h"
#include "Hashtable.h"
#include "Mapper.h"
#include "Merger.h"
#include "PAOCreator.h"
#include "PartialAgg.h"
#include "Serializer.h"
#include "TokenizerFilter.h"
#include "UTHashtable.h"
#include "util.h"

class BucketAggregator : 
    public Aggregator {
  public:
    BucketAggregator(const Config &cfg, 
                AggType type, 
                const uint64_t num_part,
                MapInput* _map_input,
                const char* infile, 
                size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
                void (*destroyPAOFunc)(PartialAgg* p), 
                const char* outfile);
    ~BucketAggregator();

    bool increaseMemory();
    bool reduceMemory();
  private:
    uint64_t capacity; // aggregator capacity

    /* data structures */
    Hashtable* hashtable_;

    /* for chunk input from DFS */
    MapInput* map_input; 
    DFSReader* chunkreader;
    FileReaderFilter* filereader;
    FileTokenizerFilter* filetoker;
    TokenizerFilter* toker;

    /* for serialized PAOs from local file */
    const char* infile;
    Deserializer* inp_deserializer;

    PAOCreator* creator;
    Hasher* hasher;
    Merger* merger;
    Serializer* bucket_serializer;
    Deserializer* deserializer;
    Hasher* bucket_hasher;
    Merger* bucket_merger;
    Serializer* final_serializer;
    uint64_t num_buckets;
    const char* outfile;
};

#endif // LIB_BUCKETAGGREGATOR_H
