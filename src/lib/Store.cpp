#include "Store.h"

StoreHasher::StoreHasher(Aggregator* agg, 
			const size_t max_keys) :
		filter(/*serial=*/true),
		aggregator(agg),
		max_keys_per_token(max_keys),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	offset_list = (size_t**)malloc(sizeof(size_t*) * num_buffers);
	value_list = (char***)malloc(sizeof(char**) * num_buffers);
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		offset_list[i] = (size_t*)malloc(sizeof(size_t) 
			* max_keys_per_token);
		value_list[i] = (char**)malloc(sizeof(char*)
			* max_keys_per_token);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
}

StoreHasher::~StoreHasher()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(offset_list[i]);
		free(value_list[i]);
		free(send[i]);
	}
	free(offset_list)
	free(value_list);
	free(send);
}

void* StoreHasher::operator()(void* pao_list)
{
	char *key, *value;
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	

	size_t* this_offset_list = offset_list[next_buffer];
	char** this_value_list = value_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

	while (ind < recv_length) {
		pao = pao_l[ind];
		// Find bin using hash function
		// Find offset of value in external HT
		// Add offset to list
		// Read value
		// Add value to list
		
		ind++;
	}

	this_send->result = pao_l;
	this_send->length = recv_length;
	this_send->result1 = this_merge_list;
	this_send->result3 = merge_list_ctr;
	return this_send;
}

void* StoreAggregator::operator()(void* pao_list)
{
	char *key, *value;
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	
	size_t* recv_off_list = (size_t*)recv->result1;
	char** recv_val_list = (char**)recv->result3;

	char** this_value_list = value_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

	while (ind < recv_length) {
		pao = pao_l[ind];
		// Aggregate values
		// destroy PAO	
		ind++;
	}

	this_send->result = this_value_list;
	this_send->length = recv_length;
	this_send->result1 = recv_off_list;
	return this_send;
}

void* StoreWriter::operator()(void* pao_list)
{
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	

	while (ind < recv_length) {
		pao = pao_l[ind];
		// Write value at offset
		ind++;
	}
}
