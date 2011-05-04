#ifndef LIB_DFSREADER_H
#define LIB_DFSREADER_H

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

class DFSReader : public tbb::filter {
public:
	static const size_t n_buffer = NUM_BUFFERS;
	DFSReader(MapInput* _input);
	~DFSReader();
private:
	size_t chunk_ctr;
	MapInput* input;
	size_t next_buffer;
	char* buf[NUM_BUFFERS];
	char* buffer;
	ChunkID id;
	void* operator()(void* pao);
};

DFSReader::DFSReader(MapInput* _input) :
		filter(serial_in_order),
		next_buffer(0),
		chunk_ctr(0),
		input(_input)
{
	for (int i = 0; i < n_buffer; i++)
		buf[i] = (char*)malloc(BUFSIZE);
	id = input->chunk_id_start;
}

DFSReader::~DFSReader()
{
	cout << "Destroying DFSReader" << endl;
	for (int i = 0; i < n_buffer; i++)
		free(buf[i]);
}

void* DFSReader::operator()(void*)
{
	size_t ret;
	if (id > input->chunk_id_end) {
		cout << "\t\t\tFinishing?" << endl;
		return NULL;
	}
	buffer = buf[next_buffer];
	next_buffer = (next_buffer + 1) % NUM_BUFFERS;
	cout << "Reading in buffer " << id << endl;
	ret = input->key_value(&buffer, id); 
	id++;
	chunk_ctr++;
	return buffer;
}

#endif // LIB_DFSREADER_H
