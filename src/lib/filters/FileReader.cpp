#include "FileReader.h"

FileReader::FileReader(const char* _dir_name, Aggregator* agg) :
		aggregator(agg),
		filter(serial_in_order),
		next_buffer(0),
		chunk_ctr(0),
		createPAO(createPAOFunc),
		dir_name(_dir_name),
		dp(NULL)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	file_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers); 
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		pao_list[i] = (PartialAgg**)malloc(sizeof(PartialAgg*) * max_keys_per_token);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}	
}

FileReader::~FileReader()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(pao_list[i]);
		free(send[i]);
	}
	free(pao_list);
	free(send);
}

void* FileReader::operator()(void*)
{
	if (!dp) {
		
	}
}

