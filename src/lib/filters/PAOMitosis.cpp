#include "PAOMitosis.h"

PAOMitosis::PAOMitosis(Aggregator* agg,
			const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
//		max_keys_per_token(max_keys),
		max_keys_per_token(10000000),
		next_buffer(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	pao_list = new MultiBuffer<PartialAgg*>(num_buffers, max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

PAOMitosis::~PAOMitosis()
{
	delete pao_list;
	delete send;
}

/**
 * Can't use const for argument because strtok modifies the string. 
 * Not re-entrant!
 */
void* PAOMitosis::operator()(void* recv)
{
	PartialAgg* p;
	size_t* tok_size;
	size_t this_list_ctr = 0;
	size_t ind;
	size_t num_paos_added;

	uint64_t num_buffers = aggregator->getNumBuffers();

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** p_list = (PartialAgg**)(recv_list->result);
	uint64_t recv_length = (uint64_t)recv_list->length;	

	PartialAgg** this_pao_list = (*pao_list)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 

    const Operations* const op = aggregator->ops();
	
	while (ind < recv_length) {
		p = p_list[ind];
		num_paos_added = op->dividePAO(*p, &this_pao_list[this_list_ctr]);
		this_list_ctr += num_paos_added;
		assert(this_list_ctr < max_keys_per_token);

		ind++;
	}

	this_send->result = this_pao_list;
	this_send->length = this_list_ctr;
	this_send->flush_hash = false;
    this_send->destroy_pao = true;

	return this_send;
}
