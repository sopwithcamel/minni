#include "Adder.h"

Adder::Adder(Aggregator* agg, 
			void (*destroyPAOFunc)(PartialAgg* p),
            size_t max_keys) :
		filter(/*serial=*/true),
		aggregator(agg),
        max_keys_per_token(max_keys),
		destroyPAO(destroyPAOFunc),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();

	agged_list = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

Adder::~Adder()
{
	delete agged_list;
	delete send;
}

void* Adder::operator()(void* recv)
{
	uint64_t ind = 0;
	PartialAgg *pao, *merge_pao = NULL;
	uint64_t pao_list_ctr = 0;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** merge_list = (PartialAgg**)recv_list->result;
	uint64_t merge_list_length = (uint64_t)recv_list->length;

	FilterInfo* this_send = (*send)[next_buffer];
	PartialAgg** this_list = (*agged_list)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();
	tokens_processed++;

	while (ind < merge_list_length) {
		pao = merge_list[ind];
		if (!merge_pao) // first pao
			merge_pao = pao;
		else if (!merge_pao->key().compare(pao->key())) { // same key
			merge_pao->merge(pao);
			destroyPAO(pao);
		} else { // different pao
			this_list[pao_list_ctr++] = merge_pao;
			merge_pao = pao;
		}
		ind++;
	}

    // Not storing any state
    aggregator->can_exit = true;
	this_send->result = this_list;
	this_send->length = pao_list_ctr;
	return this_send;
}
