#include "config.h"
#include "HashsortAggregator.h"

/*
 * Initialize pipeline
 */
HashsortAggregator::HashsortAggregator(const uint64_t _capacity, 
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

	/* Beginning of first pipeline: this pipeline takes the entire
	 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
	 * aggregates/writes to buckets. For this pipeline, a "token" or a
	 * a basic pipeline unit is a chunk read from the DFS */
	reader = new DFSReader(this, map_input);
	pipeline_list[0].add_filter(*reader);

	toker = new Tokenizer(this, emptyPAO, MapFunc);
	pipeline_list[0].add_filter(*toker);

	hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO,
			destroyPAOFunc);
	pipeline_list[0].add_filter(*hasher);

	char* bucket_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(bucket_prefix, outfile_prefix);
	strcat(bucket_prefix, "bucket");

	bucket_serializer = new Serializer(this, emptyPAO, num_buckets, 
			bucket_prefix, destroyPAOFunc);
	pipeline_list[0].add_filter(*bucket_serializer);
	
	/* Second pipeline: In this pipeline, a token is an entire bucket. In
	 * other words, each pipeline stage is called once for each bucket to
	 * be processed. This may not be fine-grained enough, but should have
	 * enough parallelism to keep our wimpy-node busy. 

	 * In this pipeline, each bucket is sorted using nsort */
	sorter = new Sorter(num_buckets, bucket_prefix);
	pipeline_list[1].add_filter(*sorter);

	free(bucket_prefix);
}

HashsortAggregator::~HashsortAggregator()
{
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
