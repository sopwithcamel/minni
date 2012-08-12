#ifndef LIB_EXTHASHAGGREGATOR_H
#define LIB_EXTHASHAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "AccumulatorFilter.h"
#include "CompressTree.h"
#include "CompressTreeFilter.h"
#include "ConcurrentHashFilter.h"
#include "Deserializer.h"
#include "DFSReader.h"
#include "ElasticObject.h"
#include "ExternalHasher.h"
#include "FileChunkerFilter.h"
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "LocalReader.h"
#include "Mapper.h"
#include "PAOCreator.h"
#include "PartialAgg.h"
#include "Serializer.h"
#include "SparseHashBob.h"
#include "SparseHashMurmur.h"
#include "SparseHashFilter.h"
#include "TokenizerFilter.h"
#include "util.h"

class ExthashAggregator : 
    public Aggregator {
  public:
    ExthashAggregator(const Config &cfg, 
                JobID jid,
                AggType type, 
                const uint64_t num_part,
                MapInput* _map_input,
                const char* infile, 
                size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
                void (*destroyPAOFunc)(PartialAgg* p), 
                const char* outfile);
    ~ExthashAggregator();
  private:
    uint64_t capacity; // aggregator capacity

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
	AccumulatorInserter* acc_int_inserter_;
    ExternalHasher* ext_hasher_;
    Serializer* final_serializer_;
    uint64_t num_buckets;
    const char* outfile_;
};

#endif // LIB_EXTHASHAGGREGATOR_H
