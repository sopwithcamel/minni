#ifndef LIB_BUCKETAGGREGATOR_H
#define LIB_BUCKETAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Accumulator.h"
#include "AccumulatorFilter.h"
#include "CompressTree.h"
#include "CompressTreeFilter.h"
#include "Deserializer.h"
#include "DFSReader.h"
#include "ElasticObject.h"
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "Hasher.h"
#include "Hashtable.h"
#include "Mapper.h"
#include "PAOCreator.h"
#include "PartialAgg.h"
#include "Serializer.h"
#include "SparseHashBob.h"
#include "SparseHashMurmur.h"
#include "SparseHashFilter.h"
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
                size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
                void (*destroyPAOFunc)(PartialAgg* p), 
                const char* outfile);
    ~BucketAggregator();
  private:
    uint64_t capacity; // aggregator capacity

    /* data structures */
    Hashtable* hashtable_;
    Accumulator* acc_internal_;
    Accumulator* acc_bucket_;

    /* for chunk input from DFS */
    MapInput* map_input; 
    DFSReader* chunkreader;
    FileReaderFilter* filereader;
    FileTokenizerFilter* filetoker;
    TokenizerFilter* toker;

    /* for serialized PAOs from local file */
    const char* infile_;
    Deserializer* inp_deserializer_;

    PAOCreator* creator_;
    Hasher* hasher_;
    Serializer* bucket_serializer_;
    Deserializer* deserializer_;
	AccumulatorInserter* acc_int_inserter_;
	AccumulatorInserter* bucket_inserter_;
    Hasher* bucket_hasher_;
    Serializer* final_serializer_;
    uint64_t num_buckets;
    const char* outfile_;
};

#endif // LIB_BUCKETAGGREGATOR_H
