#include "config.h"
#include "DFSReader.h"
#include "Aggregator.h"

DFSReader::DFSReader(Aggregator* agg, MapInput* _input, const size_t cs) :
		aggregator(agg),
		filter(serial_in_order),
		rem_buffer_size(0),
		chunksize(cs),
		next_chunk(0),
		next_buffer(0),
		chunk_ctr(0)
{
	size_t num_buffers = aggregator->getNumBuffers();
	buffer = (char*)malloc(BUFSIZE);
	chunk = new MultiBuffer<char*>(num_buffers, 1);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
	for (int i=0; i < num_buffers; i++) {
		(*chunk)[i][0] = (char*)malloc(chunksize + 1);
	}
	input = (ChunkInput*)_input;
	id = input->chunk_id_start;
}

DFSReader::~DFSReader()
{
	cout << "Destroying DFSReader" << endl;
	for (int i=0; i<aggregator->getNumBuffers(); i++) {
		free((*chunk)[i][0]);
	}
	delete chunk;
	delete send;
	free(buffer);
}

void* DFSReader::operator()(void*)
{
	size_t bytes_to_copy = chunksize;

	char *this_chunk = (*chunk)[next_chunk][0];
	FilterInfo* this_send = (*send)[next_buffer];
	next_chunk = (next_chunk + 1) % aggregator->getNumBuffers();

    if (aggregator->input_finished) {
        if (aggregator->can_exit)
            return NULL;
        else {
            aggregator->can_exit = true;
            this_send->result = NULL;
            this_send->length = 0;
            // still have to count this as a token
            aggregator->tot_input_tokens++;
            return this_send;
        }
    } else
        aggregator->can_exit = false;
	aggregator->tot_input_tokens++;

	if (0 >= rem_buffer_size) {
		cout << "Reading in buffer " << id << endl;
		rem_buffer_size = input->key_value(&buffer, id); 
		id++;
		chunk_ctr = 0;
	}
	if (rem_buffer_size < chunksize)
		bytes_to_copy = rem_buffer_size;
	memcpy(this_chunk, buffer + chunk_ctr * chunksize, bytes_to_copy);
	this_chunk[bytes_to_copy] = '\0';  
	rem_buffer_size -= bytes_to_copy;
	chunk_ctr++;

	if (id > input->chunk_id_end && rem_buffer_size <= 0) {
		aggregator->input_finished = true;
        aggregator->can_exit = true;
	}
	this_send->result = this_chunk;
	this_send->length = 1;
	return this_send;
}
