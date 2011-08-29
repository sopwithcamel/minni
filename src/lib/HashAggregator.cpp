#include "config.h"
#include "HashAggregator.h"

/*
 * Initialize pipeline
 */
HashAggregator::HashAggregator(const uint64_t type, // where to read from
				const uint64_t _capacity, 
				const uint64_t _partid, 
				MapInput* _map_input, // if input is DFS_CHUNK
				const char* input_prefix, // if input is LOCAL_PAO
				PartialAgg* (*createPAOFunc)(const char*),
				void (*destroyPAOFunc)(PartialAgg*),
				const uint64_t num_buckets,
				const char* outfile_prefix) :
		Aggregator(1, _capacity, _partid, createPAOFunc, destroyPAOFunc),
		type(DFS_CHUNK_INPUT),
		map_input(_map_input),
		num_buckets(num_buckets),
		input_prefix(input_prefix),
		outfile_prefix(outfile_prefix)
{
	PartialAgg* emptyPAO = createPAOFunc(EMPTY_KEY);

	if (DFS_CHUNK_INPUT == type) {
		reader = new DFSReader(this, map_input);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*toker);
	} else if (LOCAL_PAO_INPUT == type) {
		deserializer = new Deserializer(this, num_buckets, input_prefix,
			emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*deserializer);
	}

	hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO, 
		destroyPAOFunc);
	if (LOCAL_PAO_INPUT == type)
		hasher->setFlushOnComplete();
	pipeline_list[0].add_filter(*hasher);

	// TODO: Handle output to DFS here
	serializer = new Serializer(this, emptyPAO, num_buckets, outfile_prefix, 
		destroyPAOFunc);
	pipeline_list[0].add_filter(*serializer);
}

HashAggregator::~HashAggregator()
{
	pipeline_list[0].clear();
}
