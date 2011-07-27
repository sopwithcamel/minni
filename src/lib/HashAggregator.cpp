#include "config.h"
#include "HashAggregator.h"

/*
 * Initialize pipeline
 */
HashAggregator::HashAggregator(const uint64_t _capacity, 
				const uint64_t _partid, 
				MapInput* _map_input,
				PartialAgg* (*MapFunc)(const char*),
				void (*destroyPAOFunc)(PartialAgg*)) :
		MapperAggregator(_capacity, _partid, MapFunc, destroyPAOFunc),
		map_input(_map_input)
{
	PartialAgg* emptyPAO = MapFunc(EMPTY_KEY);

	reader = new DFSReader(map_input);
	pipeline.add_filter(*reader);

	toker = new Tokenizer(emptyPAO, MapFunc);
	pipeline.add_filter(*toker);

	hasher = new InternalHasher<char*, CharHash, eqstr>(emptyPAO, destroyPAOFunc);
	pipeline.add_filter(*hasher);

	serializer = new Serializer(1, "/localfs/hamur/");
	pipeline.add_filter(*serializer);
}

HashAggregator::~HashAggregator()
{
	pipeline.clear();
}
