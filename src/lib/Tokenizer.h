#ifndef LIB_TOKENIZER_H
#define LIB_TOKENIZER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"

/**
 * To be used after Reader in the pipeline.
 * - Consumes a buffer 
 * - Produces a vector of key, value pairs
 * 
 */

template <typename KeyType>
class Tokenizer : public tbb::filter {
public:
	Tokenizer();
	~Tokenizer();
private:
	typedef std::vector< std::pair<KeyType, PartialAgg*> > KVVector;
	size_t next_buffer;
	KVVector kv_vector[NUM_BUFFERS];
	KVVector* kv;
	void* operator()(void* pao);
};

template <typename KeyType>
Tokenizer<KeyType>::Tokenizer() :
		filter(serial_in_order)
{
}

template <typename KeyType>
Tokenizer<KeyType>::~Tokenizer()
{
	for (int i=0; i<NUM_BUFFERS; i++)
		kv_vector[i].clear();
}

/**
 * Can't use const for argument because strtok modifies the string
 */
template <typename KeyType>
void* Tokenizer<KeyType>::operator()(void* buffer)
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
		new_key = (char*)malloc(strlen(spl)+1);
		new_val = (char*)malloc(VALUE_SIZE);
		strcpy(new_key, spl);
		strcpy(new_val, "1");
		PartialAgg* new_pao = new PartialAgg(new_key, new_val);
		kv->push_back(std::pair<char*, PartialAgg*>(new_key, new_pao));

		spl = strtok(NULL, " \n");
		if (spl == NULL) {
			break;
		}
	}
	return kv;
}

#endif // LIB_TOKENIZER_H
