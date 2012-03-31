#ifndef LIB_SERIALIZER_H
#define LIB_SERIALIZER_H

#include <boost/archive/binary_oarchive.hpp>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <iostream>
#include <stdlib.h>
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

/**
 * Consumes: array of PAOs
 * Produces: nil (PAOs are serialized to one or more files, but this is the 
 *  last stage of the pipeline.
 */

class Serializer : public tbb::filter {
public:
	Serializer(Aggregator* agg,
			const uint64_t nb,
			const char* outfile_prefix, 
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p));
	~Serializer();
private:
	Aggregator* aggregator;
	AggType type;
	size_t (*createPAO)(Token* t, PartialAgg** p);
	void (*destroyPAO)(PartialAgg* p);
	bool already_partitioned;
	int num_buckets;

    PartialAgg::SerializationMethod serializationMethod_;
	std::vector<std::ofstream*> fl_;
    std::vector<google::protobuf::io::ZeroCopyOutputStream*> raw_output_;
    std::vector<google::protobuf::io::CodedOutputStream*> coded_output_;
    std::vector<boost::archive::binary_oarchive*> oa_;
	char* outfile_prefix;
	size_t tokens_processed;
	void* operator()(void* pao_list);
	int partition(const std::string& key);
};

#endif
