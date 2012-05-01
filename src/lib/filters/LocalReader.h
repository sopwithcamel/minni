#ifndef LIB_LOCALREADER_H
#define LIB_LOCALREADER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Mapper.h"
#include "Util.h"

/**
 * To be used as the input stage in the pipeline. It deserializes a list of 
 * PAOs from a list of files. The location prefix is passed as an input
 * parameter and "bucketN" is appended to the prefix to get the absolute
 * path of the file.
 * - Produces an array of PAOs; the size of the array is determined by an
 *   an input parameter.  
 */

class LocalReader : public tbb::filter {
public:
    LocalReader(Aggregator* agg, 
            const char* inp_prefix,
            size_t max_keys);
    ~LocalReader();
private:
    Aggregator* aggregator;
    char* inputfile_prefix;
	const size_t max_keys_per_token;
	MultiBuffer<char*>* chunk;
	MultiBuffer<Token*>* tokens;
    size_t next_buffer;
    MultiBuffer<FilterInfo>* send;
    void* read_buf;

    FILE *inp;       // file pointer for current bucket
    void* operator()(void*);
    uint64_t appendToList(PartialAgg* p);
};

#endif // LIB_DESERIALIZER_H
