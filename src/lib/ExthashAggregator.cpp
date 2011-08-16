#include "config.h"
#include "ExthashAggregator.h"

/*
 * Initialize pipeline
 */
ExthashAggregator::ExthashAggregator(const uint64_t _capacity, 
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

	char* ht_name = (char*)malloc(FILENAME_LENGTH);
	strcpy(ht_name, "/localfs/hamur/hashtable_dump");
	ext_hasher = new ExternalHasher(this, ht_name, emptyPAO, destroyPAOFunc);
	pipeline_list[0].add_filter(*ext_hasher);

	free(ht_name);
}

ExthashAggregator::~ExthashAggregator()
{
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
