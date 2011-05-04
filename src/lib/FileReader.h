#ifndef LIB_FILEREADER_H
#define LIB_FILEREADER_H

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
 * To be used as the input stage in the pipeline.
 * - Produces a buffer for consumption
 * 
 */

class FileReader : public tbb::filter {
public:
	static const size_t n_buffer = NUM_BUFFERS;
	FileReader(FILE* _input_file);
	~FileReader();
private:
	size_t chunk_ctr;
	FILE* input_file;
	size_t next_buffer;
	char* buf[NUM_BUFFERS];
	char* buffer;
	void* operator()(void* pao);
};

FileReader::FileReader(FILE* _input_file) :
		filter(serial_in_order),
		next_buffer(0),
		chunk_ctr(0),
		input_file(_input_file)
{
	for (int i = 0; i < n_buffer; i++)
		buf[i] = (char*)malloc(BUFSIZE);
}

FileReader::~FileReader()
{
	for (int i = 0; i < n_buffer; i++)
		free(buf[i]);
}

void* FileReader::operator()(void*)
{
	size_t ret;
	buffer = buf[next_buffer];
	ret = fread(buffer, sizeof(char), BUFSIZE, input_file);
	chunk_ctr++;
	next_buffer = (next_buffer + 1) % NUM_BUFFERS;
	if (!ret || chunk_ctr == 16) { // EOF
		return NULL;
	} else {
		return buffer;
	}
}

#endif // LIB_FILEREADER_H
