#include "Tokenizer.h"

Tokenizer::Tokenizer(Aggregator* agg, PartialAgg* emptyPAO, 
			PartialAgg* (*createPAOFunc)(const char* t)) :
		aggregator(agg),
		filter(serial_in_order),
		next_buffer(0),
		emptyPAO(emptyPAO),
		createPAO(createPAOFunc)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	pao_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers); 
}

Tokenizer::~Tokenizer()
{
	free(pao_list);
}

/**
 * Can't use const for argument because strtok modifies the string
 */
void* Tokenizer::operator()(void* buffer)
{
	char *spl = NULL ;
	char* tok_buf = (char*) buffer;	 
	int tok_ctr = 0;
	size_t this_list_ctr = 0;

	uint64_t num_buffers = aggregator->getNumBuffers();

	pao_list[next_buffer] = (PartialAgg**)malloc(sizeof(PartialAgg*) * MAX_KEYS_PER_TOKEN);
	PartialAgg** this_pao_list = pao_list[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	spl = strtok(tok_buf, " .\n\r\'\"?,;:!*()-\uFEFF");
	if (spl == NULL) { 
		perror("Buffer sent to Tokenizer is empty!");
		exit(1);
	}
	while (1) {
	PartialAgg* new_pao = createPAO(spl); 
//		fprintf(stderr, "tok: %d", tok_ctr++);

		this_pao_list[this_list_ctr++] = new_pao;
		assert(this_list_ctr < MAX_KEYS_PER_TOKEN);

		spl = strtok(NULL, " .\n\r\'\"?,;:!*()-\uFEFF");
		if (spl == NULL) {
			break;
		}
	}
	this_pao_list[this_list_ctr++] = emptyPAO;
	return this_pao_list;
}

