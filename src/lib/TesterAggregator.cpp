#include "TesterAggregator.h"

/*
 * Initialize pipeline
 */
TesterAggregator::TesterAggregator(const Config &cfg,
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, type, 2, num_part, createPAOFunc, destroyPAOFunc),
		map_input(_map_input),
		infile(infile),
		outfile(outfile)
{
	Setting& c_empty_key = readConfigFile(cfg, "minni.common.key.empty");
	string empty_key = (const char*)c_empty_key;
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	Setting& c_test_element = readConfigFile(cfg, 
				"minni.aggregator.tester.element");
	test_element = (const char*)c_test_element;

	if (test_element.compare("toker")) {
		reader = new DFSReader(this, map_input);	
		pipeline_list[0].add_filter(reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(toker);
	} else if (test_element.compare("sorter")) {
		Setting& c_sort_input = readConfigFile(cfg, 
				"minni.aggregator.tester.sorter.inputfile");
		string sort_input = (const char*)c_sort_input;

		Setting& c_sort_output = readConfigFile(cfg, 
				"minni.aggregator.tester.sorter.outputfile");
		string sort_output = (const char*)c_sort_output;

		Setting& c_sort_mem = readConfigFile(cfg, 
				"minni.aggregator.tester.sorter.memory");
		int sort_mem = c_sort_mem;

		sorter = new Sorter(this);
		pipeline_list[0].add_filter(*sorter);
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
