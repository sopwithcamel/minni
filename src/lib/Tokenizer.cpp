#include "config.h"
#include "Tokenizer.h"
#include "Util.h"

Tokenizer::Tokenizer(Aggregator* agg, PartialAgg* emptyPAO, 
			PartialAgg* (*MapFunc)(const char* t)) :
		aggregator(agg),
		filter(serial_in_order),
		next_buffer(0),
		emptyPAO(emptyPAO),
		Map(MapFunc)
{
	for (int i=0; i<NUM_BUFFERS; i++) {
		pao_list[i] = (PartialAgg**)malloc(sizeof(PartialAgg*));
	}
}

Tokenizer::~Tokenizer()
{
}

/**
 * Can't use const for argument because strtok modifies the string
 */
void* Tokenizer::operator()(void* buffer)
{
	char *spl, *new_val;
	char* tok_buf = (char*) buffer;	 
	int tok_ctr = 0;
	size_t this_list_ctr = 0;
	size_t this_list_size = 1;

	/* TODO: don't the next two statements need to executed atomically? */
	pao_list[next_buffer] = (PartialAgg**)malloc(sizeof(PartialAgg*));
	PartialAgg*** this_pao_list = &pao_list[next_buffer];
	next_buffer = (next_buffer + 1) % NUM_BUFFERS;	
	
	spl = strtok(tok_buf, " .\n\r\'\"?,;:!*()-\uFEFF");
	if (spl == NULL) { 
		perror("Not good!");
		return NULL;
	}
	while (1) {
		PartialAgg* new_pao = Map(spl); 
//		fprintf(stderr, "tok: %d", tok_ctr++);
		if (this_list_ctr >= this_list_size) {
			this_list_size += LIST_SIZE_INCR;
			if (call_realloc(this_pao_list, this_list_size) == NULL) {
				perror("realloc failed");
				return NULL;
			}
			assert(this_list_ctr < this_list_size);
		}
		(*this_pao_list)[this_list_ctr++] = new_pao;

		spl = strtok(NULL, " .\n\r\'\"?,;:!*()-\uFEFF");
		if (spl == NULL) {
			break;
		}
	}
	if (this_list_ctr >= this_list_size) {
		this_list_size += LIST_SIZE_INCR;
		if (call_realloc(this_pao_list, this_list_size) == NULL) {
			perror("realloc failed");
			return NULL;
		}
		assert(this_list_ctr < this_list_size);
	}
	(*this_pao_list)[this_list_ctr++] = emptyPAO;
	return *this_pao_list;
}

