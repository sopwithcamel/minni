#include "Adder.h"

Adder::Adder(Aggregator* agg, 
			PartialAgg* emptyPAO,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	uint64_t list_size = aggregator->getPAOsPerToken();

	agged_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers);
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		agged_list[i] = (PartialAgg**)malloc(sizeof(PartialAgg*) * list_size);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
}

Adder::~Adder()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(agged_list[i]);
		free(send[i]);
	}
	free(agged_list);
	free(send);
}

void* Adder::operator()(void* pao_list)
{
	uint64_t ind = 0;
	PartialAgg *pao, *merge_pao = NULL;
	uint64_t pao_list_ctr = 0;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** merge_list = (PartialAgg**)recv->result;
	uint64_t merge_list_length = (uint64_t)recv->length;

	FilterInfo* this_send = send[next_buffer];
	PartialAgg** this_list = agged_list[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

	while (ind < merge_list_length) {
		pao = merge_list[ind];
		if (!merge_pao) // first pao
			merge_pao = pao;
		else if (!strcmp(merge_pao->key, pao->key)) { // same pao
			merge_pao->merge(pao);
			destroyPAO(pao);
		} else { // different pao
			this_list[pao_list_ctr++] = merge_pao;
			merge_pao = pao;
		}
		ind++;
	}

	tokens_processed++;

	this_send->result = this_list;
	this_send->length = pao_list_ctr;
	return this_send;
}
