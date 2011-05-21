#include "config.h"
#include "HashAggregator.h"

/*
 * Initialize pipeline
 */
HashAggregator::HashAggregator(const uint64_t _capacity, 
				const uint64_t _partid, 
				MapInput* _map_input,
				PartialAgg* (*MapFunc)(const char*)) :
		MapperAggregator(_capacity, _partid, MapFunc),
		map_input(_map_input)
{
	printf("Addr1: %p\n", &pipeline);
	reader = new DFSReader(map_input);
	pipeline.add_filter(*reader);

	toker = new Tokenizer(MapFunc);
	pipeline.add_filter(*toker);

	hasher = new InternalHasher<char*, PartialAgg*, CharHash, eqstr>();
	pipeline.add_filter(*hasher);
}

HashAggregator::~HashAggregator()
{
	pipeline.clear();
}
