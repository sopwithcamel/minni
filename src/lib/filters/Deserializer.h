#ifndef LIB_DESERIALIZER_H
#define LIB_DESERIALIZER_H

#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <boost/archive/binary_iarchive.hpp>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "BoostPartialAgg.h"
#include "HandSerializedPartialAgg.h"
#include "ProtobufPartialAgg.h"
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

class Deserializer : public tbb::filter {
public:
	Deserializer(Aggregator* agg, 
			const uint64_t num_buckets, 
			const char* inp_prefix,
            size_t max_keys,
            bool ref=false);
	~Deserializer();
private:
	Aggregator* aggregator;
	const size_t max_keys_per_token;
	char* inputfile_prefix;
	const uint64_t num_buckets;	// Number of files to process serially
	uint64_t buckets_processed;
	MultiBuffer<PartialAgg*>* pao_list;
	size_t next_buffer;
    bool reuse_paos;
	MultiBuffer<FilterInfo>* send;
	void* read_buf;

    Operations::SerializationMethod serializationMethod_;
	std::ifstream *cur_bucket;		// file pointer for current bucket
    google::protobuf::io::ZeroCopyInputStream *raw_input;
    google::protobuf::io::CodedInputStream* coded_input;
    boost::archive::binary_iarchive* ia_;
    
	void* operator()(void*);
	uint64_t appendToList(PartialAgg* p);
};

#endif // LIB_DESERIALIZER_H
