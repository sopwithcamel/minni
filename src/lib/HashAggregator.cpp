#include "config.h"
#include "HashAggregator.h"

/*
 * Initialize pipeline
 */
HashAggregator::HashAggregator(const uint64_t _capacity, 
				const uint64_t _partid, 
				MapInput* _map_input,
				PartialAgg* (*MapFunc)(const char*),
				void (*destroyPAOFunc)(PartialAgg*),
				const uint64_t num_buckets,
				const char* outfile_prefix) :
		MapperAggregator(_capacity, _partid, MapFunc, destroyPAOFunc),
		map_input(_map_input),
		num_buckets(num_buckets),
		outfile_prefix(outfile_prefix)
{
	PartialAgg* emptyPAO = MapFunc(EMPTY_KEY);

	reader = new DFSReader(map_input);
	pipeline.add_filter(*reader);

	toker = new Tokenizer(emptyPAO, MapFunc);
	pipeline.add_filter(*toker);

	hasher = new Hasher<char*, CharHash, eqstr>(emptyPAO, destroyPAOFunc);
	pipeline.add_filter(*hasher);

	serializer = new Serializer(num_buckets, outfile_prefix);
	pipeline.add_filter(*serializer);
}

HashAggregator::~HashAggregator()
{
	pipeline.clear();
}
