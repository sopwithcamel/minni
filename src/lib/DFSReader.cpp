#include "DFSReader.h"
#include "Aggregator.h"

DFSReader::DFSReader(Aggregator* agg, MapInput* _input) :
		aggregator(agg),
		filter(serial_in_order),
		rem_buffer_size(0),
		next_chunk(0),
		chunk_ctr(0),
		input(_input)
{
	size_t num_buf = aggregator->getNumBuffers();
	buffer = (char*)malloc(BUFSIZE);
	chunk = (char**)malloc(num_buf * sizeof(char*));
	for (int i=0; i < num_buf; i++) {
		chunk[i] = (char*)malloc(CHUNKSIZE + 1);
	}
	id = input->chunk_id_start;
}

DFSReader::~DFSReader()
{
	cout << "Destroying DFSReader" << endl;
	for (int i=0; i<aggregator->getNumBuffers(); i++) {
		free(chunk[i]);
	}
	free(buffer);
	free(chunk);
}

void* DFSReader::operator()(void*)
{
	size_t bytes_to_copy = CHUNKSIZE;

	char *this_chunk = chunk[next_chunk];
	next_chunk = (next_chunk + 1) % aggregator->getNumBuffers();

	if (aggregator->input_finished) {
		return NULL;
	}
	if (0 >= rem_buffer_size) {
		cout << "Reading in buffer " << id << endl;
		rem_buffer_size = input->key_value(&buffer, id); 
		id++;
		chunk_ctr = 0;
	}
	if (rem_buffer_size < CHUNKSIZE)
		bytes_to_copy = rem_buffer_size;
	memcpy(this_chunk, buffer + chunk_ctr * CHUNKSIZE, bytes_to_copy);
	this_chunk[bytes_to_copy] = '\0';  
	rem_buffer_size -= bytes_to_copy;
	chunk_ctr++;

	aggregator->tot_input_tokens++;
	if (id > input->chunk_id_end && rem_buffer_size <= 0) {
		aggregator->input_finished = true;
	}
	return this_chunk;
}
