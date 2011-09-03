#include "DFSReader.h"
#include "Aggregator.h"

DFSReader::DFSReader(Aggregator* agg, MapInput* _input) :
		aggregator(agg),
		filter(serial_in_order),
		rem_buffer_size(0),
		chunk_ctr(0),
		input(_input)
{
	buffer = (char*)malloc(BUFSIZE);
	chunk = (char*)malloc(CHUNKSIZE + 1);
	id = input->chunk_id_start;
}

DFSReader::~DFSReader()
{
	cout << "Destroying DFSReader" << endl;
	free(buffer);
	free(chunk);
}

void* DFSReader::operator()(void*)
{
	if (id > input->chunk_id_end) {
		aggregator->input_finished = true;
		return NULL;
	}
	if (0 >= rem_buffer_size) {
		cout << "Reading in buffer " << id << endl;
		rem_buffer_size = input->key_value(&buffer, id); 
		id++;
		chunk_ctr = 0;
	}
	memcpy(chunk, buffer + chunk_ctr * CHUNKSIZE, CHUNKSIZE);
	chunk[CHUNKSIZE] = '\0';  // if buffer smaller, there's a terminator anyway
	rem_buffer_size -= CHUNKSIZE;
	chunk_ctr++;

	aggregator->tot_input_tokens++;
	if (id > input->chunk_id_end) {
		aggregator->input_finished = true;
	}
	return chunk;
}
