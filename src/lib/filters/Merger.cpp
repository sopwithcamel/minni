#include "Merger.h"

Merger::Merger(Aggregator* agg, 
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		destroyPAO(destroyPAOFunc),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

Merger::~Merger()
{
	delete send;
}

void* Merger::operator()(void* recv)
{
	uint64_t ind = 0;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** merge_list = (PartialAgg**)recv_list->result1;
	PartialAgg** mergand_list = (PartialAgg**)recv_list->result2;
	uint64_t merge_list_length = (uint64_t)recv_list->result3;	

	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

	while (ind < merge_list_length) {
		merge_list[ind]->merge(mergand_list[ind]);
//		destroyPAO(mergand_list[ind]);

		ind++;
	}

	tokens_processed++;

	this_send->result = recv_list->result;
	this_send->length = recv_list->length;
	return this_send;
}
