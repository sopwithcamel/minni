#include "DFSReader.h"
#include "Aggregator.h"

DFSReader::DFSReader(Aggregator* agg, MapInput* _input) :
		aggregator(agg),
		filter(serial_in_order),
		next_buffer(0),
		chunk_ctr(0),
		input(_input)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	buf = (char**)malloc(sizeof(char*) * num_buffers);
	for (int i = 0; i < num_buffers; i++)
		buf[i] = (char*)malloc(BUFSIZE);
	id = input->chunk_id_start;
}

DFSReader::~DFSReader()
{
	cout << "Destroying DFSReader" << endl;
	for (int i = 0; i < aggregator->getNumBuffers(); i++)
		free(buf[i]);
	free(buf);
}

void* DFSReader::operator()(void*)
{
	size_t ret;
	if (id > input->chunk_id_end) {
		aggregator->input_finished = true;
		return NULL;
	}
	buffer = buf[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers(); 
	cout << "Reading in buffer " << id << endl;
	ret = input->key_value(&buffer, id); 
	id++;
	chunk_ctr++;
	aggregator->tot_input_tokens++;
	if (id > input->chunk_id_end) {
		aggregator->input_finished = true;
	}
	return buffer;
}

