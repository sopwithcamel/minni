#include "TesterAggregator.h"

/*
 * Initialize pipeline
 */
TesterAggregator::TesterAggregator(const Config &cfg,
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
				void (*destroyPAOFunc)(PartialAgg* p),
				const char* outfile):
		Aggregator(cfg, type, 2, 1/*num_part*/, createPAOFunc, destroyPAOFunc),
		map_input(_map_input)
{
	Setting& c_test_element = readConfigFile(cfg, 
				"minni.aggregator.tester.element");
	test_element = (const char*)c_test_element;

	Setting& c_max_keys = readConfigFile(cfg, 
				"minni.tbb.max_keys_per_token");
	size_t max_keys = c_max_keys;

	if (!test_element.compare("toker")) {
		reader = new DFSReader(this, map_input);	
		pipeline_list[0].add_filter(*reader);

		toker = new TokenizerFilter(this, cfg);
		pipeline_list[0].add_filter(*toker);
	} else if (!test_element.compare("sorter")) {
		Setting& c_sort_input = readConfigFile(cfg, 
				"minni.aggregator.tester.sorter.inputfile");
		string sort_input = (const char*)c_sort_input;

		Setting& c_sort_output = readConfigFile(cfg, 
				"minni.aggregator.tester.sorter.outputfile");
		string sort_output = (const char*)c_sort_output;

		Setting& c_sort_mem = readConfigFile(cfg, 
				"minni.aggregator.tester.sorter.memory");
		int sort_mem = c_sort_mem;

		sorter = new Sorter(1, sort_input.c_str(), sort_output.c_str());
		pipeline_list[0].add_filter(*sorter);
	} else if (!test_element.compare("store")) {
		reader = new DFSReader(this, map_input);	
		pipeline_list[0].add_filter(*reader);

		toker = new TokenizerFilter(this, cfg);
		pipeline_list[0].add_filter(*toker);

		store = new Store(this, createPAOFunc, destroyPAOFunc, max_keys);
		pipeline_list[0].add_filter(*store->hasher);		
		pipeline_list[0].add_filter(*store->agger);		
		pipeline_list[0].add_filter(*store->writer);
	}
}

TesterAggregator::~TesterAggregator()
{
	if (test_element.compare("toker")) {
		delete(reader);
		delete(toker);
	} else if (test_element.compare("sorter")) {
		delete(sorter);
	}
	pipeline_list[0].clear();
}

bool TesterAggregator::increaseMemory()
{
    return false;
}

bool TesterAggregator::reduceMemory()
{
    return false;
}
