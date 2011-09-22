#include "Merger.h"

Merger::Merger(Aggregator* agg, 
			PartialAgg* emptyPAO,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
}

Merger::~Merger()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(send[i]);
	}
	free(send);
}

void* Merger::operator()(void* pao_list)
{
	uint64_t ind = 0;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** merge_list = (PartialAgg**)recv->result1;
	PartialAgg** mergand_list = (PartialAgg**)recv->result2;
	uint64_t merge_list_length = (uint64_t)recv->result3;	

	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

	while (ind < merge_list_length) {
		merge_list[ind]->merge(mergand_list[ind]);
		destroyPAO(mergand_list[ind]);

		ind++;
	}

	tokens_processed++;

	this_send->result = recv->result;
	this_send->length = recv->length;
	return this_send;
}
