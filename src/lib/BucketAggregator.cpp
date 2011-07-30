#include "config.h"
#include "BucketAggregator.h"

/*
 * Initialize pipeline
 */
BucketAggregator::BucketAggregator(const uint64_t _capacity, 
				const uint64_t _partid, 
				MapInput* _map_input,
				PartialAgg* (*MapFunc)(const char*),
				void (*destroyPAOFunc)(PartialAgg*),
				const uint64_t num_buckets,
				const char* outfile_prefix) :
		MapperAggregator(2, _capacity, _partid, MapFunc, destroyPAOFunc),
		map_input(_map_input),
		num_buckets(num_buckets),
		outfile_prefix(outfile_prefix)
{
	PartialAgg* emptyPAO = MapFunc(EMPTY_KEY);

	reader = new DFSReader(this, map_input);
	pipeline_list[0].add_filter(*reader);

	toker = new Tokenizer(this, emptyPAO, MapFunc);
	pipeline_list[0].add_filter(*toker);

	hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO,
			destroyPAOFunc);
	pipeline_list[0].add_filter(*hasher);

	serializer = new Serializer(this, num_buckets, outfile_prefix);
	pipeline_list[0].add_filter(*serializer);
	
	deserializer = new Deserializer(this, num_buckets, outfile_prefix,
			emptyPAO);
	pipeline_list[1].add_filter(*deserializer);

	bucket_hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO,
			destroyPAOFunc);
	pipeline_list[1].add_filter(*bucket_hasher);
}

BucketAggregator::~BucketAggregator()
{
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
