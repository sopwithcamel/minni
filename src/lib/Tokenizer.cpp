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
		kv_vector[i].clear();
}

/**
 * Can't use const for argument because strtok modifies the string
 */
void* Tokenizer::operator()(void* buffer)
{
	char *spl, *new_key, *new_val;
	char* tok_buf = (char*) buffer;	 

	/* TODO: don't the next two statements need to executed atomically? */
	kv = &kv_vector[next_buffer];
	next_buffer = (next_buffer + 1) % NUM_BUFFERS;	
	spl = strtok(tok_buf, " \n");
	if (spl == NULL) { 
		perror("Not good!");
		return NULL;
	}
	while (1) {
		PartialAgg* new_pao = Map(spl); 
		kv->push_back(std::pair<char*, PartialAgg*>(new_key, new_pao));

		spl = strtok(NULL, " \n");
		if (spl == NULL) {
			break;
		}
	}
	return kv;
}

