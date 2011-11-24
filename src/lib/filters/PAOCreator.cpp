#include "PAOCreator.h"

PAOCreator::PAOCreator(Aggregator* agg,
			PartialAgg* (*createPAOFunc)(Token* t),
			const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
		max_keys_per_token(max_keys),
		next_buffer(0),
		createPAO(createPAOFunc)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	pao_list = new MultiBuffer<PartialAgg*>(num_buffers, max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

PAOCreator::~PAOCreator()
{
	delete pao_list;
	delete send;
}

/**
 * Can't use const for argument because strtok modifies the string. 
 * Not re-entrant!
 */
void* PAOCreator::operator()(void* recv)
{
	Token* tok;
	size_t* tok_size;
	size_t this_list_ctr = 0;
	size_t ind;

	uint64_t num_buffers = aggregator->getNumBuffers();
	PartialAgg* new_pao;

	FilterInfo* recv_list = (FilterInfo*)recv;
	Token** tok_list = (Token**)(recv_list->result);
	uint64_t recv_length = (uint64_t)recv_list->length;	

	PartialAgg** this_pao_list = (*pao_list)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	while (ind < recv_length) {
		tok = tok_list[ind];
		// Insert token sizes; TODO
		new_pao = createPAO(tok);
		this_pao_list[this_list_ctr++] = new_pao;
		assert(this_list_ctr < max_keys_per_token);

		ind++;
	}

	this_send->result = this_pao_list;
	this_send->length = this_list_ctr;
	this_send->flush_hash = false;

	return this_send;
}

