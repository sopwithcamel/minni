#include "Tokenizer.h"

Tokenizer::Tokenizer(Aggregator* agg, 
			PartialAgg* (*createPAOFunc)(const char** t),
			const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
		max_keys_per_token(max_keys),
		next_buffer(0),
		createPAO(createPAOFunc)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	pao_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers); 
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		pao_list[i] = (PartialAgg**)malloc(sizeof(PartialAgg*) * max_keys_per_token);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}	
}

Tokenizer::~Tokenizer()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(pao_list[i]);
		free(send[i]);
	}
	free(pao_list);
	free(send);
}

/**
 * Can't use const for argument because strtok modifies the string. 
 * Not re-entrant!
 */
void* Tokenizer::operator()(void* buffer)
{
	char *spl = NULL ;
	char* tok_buf = (char*) buffer;	 
	int tok_ctr = 0;
	size_t this_list_ctr = 0;
	uint64_t num_buffers = aggregator->getNumBuffers();
	// passes just one token to createPAO
	const char** tokens = (const char**)malloc(sizeof(char*));

	PartialAgg** this_pao_list = pao_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	int tok_flag = 1; // for strtok to behave differently
	PartialAgg* dummyPAO = createPAO(NULL);
	if (tok_buf == NULL) { 
		perror("Buffer sent to Tokenizer is empty!");
		exit(1);
	}
	while (1) {
		tokens = (const char**)dummyPAO->tokenize(tok_buf, &tok_flag);
		if (tokens == NULL) {
			break;
		}
		PartialAgg* new_pao = createPAO(tokens); 
		this_pao_list[this_list_ctr++] = new_pao;
		assert(this_list_ctr < max_keys_per_token);
	}
	this_send->result = this_pao_list;
	this_send->length = this_list_ctr;
	this_send->flush_hash = false;

	free(tokens);
	return this_send;
}

