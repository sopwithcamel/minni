#include "config.h"
#include "Tokenizer.h"

Tokenizer::Tokenizer(PartialAgg* (*MapFunc)(const char* t)) :
		filter(serial_in_order),
		Map(MapFunc)
{
}

Tokenizer::~Tokenizer()
{
	for (int i=0; i<NUM_BUFFERS; i++)
		pao_vector[i].clear();
}

/**
 * Can't use const for argument because strtok modifies the string
 */
void* Tokenizer::operator()(void* buffer)
{
	char *spl, *new_val;
	char* tok_buf = (char*) buffer;	 
	int tok_ctr = 0;

	/* TODO: don't the next two statements need to executed atomically? */
	paov = &pao_vector[next_buffer];
	next_buffer = (next_buffer + 1) % NUM_BUFFERS;	
	spl = strtok(tok_buf, " \n");
	if (spl == NULL) { 
		perror("Not good!");
		return NULL;
	}
	while (1) {
		PartialAgg* new_pao = Map(spl); 
//		fprintf(stderr, "tok: %d", tok_ctr++);
		paov->push_back(new_pao);

		spl = strtok(NULL, " \n");
		if (spl == NULL) {
			break;
		}
	}
	return paov;
}

