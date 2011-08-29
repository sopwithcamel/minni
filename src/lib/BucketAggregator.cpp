#include "config.h"
#include "BucketAggregator.h"

/*
 * Initialize pipeline
 */
BucketAggregator::BucketAggregator(const uint64_t type,
				const uint64_t _capacity, 
				const uint64_t _partid, 
				MapInput* _map_input,
				const char* inpfile_prefix,
				PartialAgg* (*createPAOFunc)(const char*),
				void (*destroyPAOFunc)(PartialAgg*),
				const uint64_t num_buckets,
				const char* outfile_prefix) :
		Aggregator(2, _capacity, _partid, createPAOFunc, destroyPAOFunc),
		type(DFS_CHUNK_INPUT),
		map_input(_map_input),
		num_buckets(num_buckets),
		input_prefix(inpfile_prefix),
		outfile_prefix(outfile_prefix)
{
	PartialAgg* emptyPAO = createPAOFunc(EMPTY_KEY);

	if (DFS_CHUNK_INPUT == type) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
		 * aggregates/writes to buckets. For this pipeline, a "token" or a
		 * a basic pipeline unit is a chunk read from the DFS */
		reader = new DFSReader(this, map_input);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*toker);
	} else if (LOCAL_PAO_INPUT == type) {
		inp_deserializer = new Deserializer(this, 1/*TODO: how many?*/, input_prefix,
			emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*inp_deserializer);
	}

	hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO,
			destroyPAOFunc);
	if (LOCAL_PAO_INPUT == type)
		hasher->setFlushOnComplete();
	pipeline_list[0].add_filter(*hasher);

	char* bucket_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(bucket_prefix, outfile_prefix);
	strcat(bucket_prefix, "-bucket");

	bucket_serializer = new Serializer(this, emptyPAO, num_buckets, 
			bucket_prefix, destroyPAOFunc);
	pipeline_list[0].add_filter(*bucket_serializer);
	
	/* Second pipeline: In this pipeline, a token is an entire bucket. In
	 * other words, each pipeline stage is called once for each bucket to
	 * be processed. This may not be fine-grained enough, but should have
	 * enough parallelism to keep our wimpy-node busy. 

	 * In this pipeline, a bucket is read back into memory (converted to 
	 * PAOs again), aggregated using a hashtable, and serialized. */
	deserializer = new Deserializer(this, num_buckets, bucket_prefix,
			emptyPAO, createPAOFunc);
	pipeline_list[1].add_filter(*deserializer);

	bucket_hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO,
			destroyPAOFunc);
	bucket_hasher->setFlushOnComplete();
	pipeline_list[1].add_filter(*bucket_hasher);

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, outfile_prefix);
	final_serializer = new Serializer(this, emptyPAO, (uint64_t)1, final_path, 
			destroyPAOFunc); 
	pipeline_list[1].add_filter(*final_serializer);

	free(bucket_prefix);
	free(final_path);
}

BucketAggregator::~BucketAggregator()
{
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
