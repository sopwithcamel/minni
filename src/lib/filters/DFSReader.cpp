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
	chunk = (char**)malloc(num_buffers * sizeof(char*));
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	for (int i=0; i < num_buffers; i++) {
		chunk[i] = (char*)malloc(chunksize + 1);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
	input = (ChunkInput*)_input;
	id = input->chunk_id_start;
}

DFSReader::~DFSReader()
{
	cout << "Destroying DFSReader" << endl;
	for (int i=0; i<aggregator->getNumBuffers(); i++) {
		free(chunk[i]);
		free(send[i]);
	}
	free(send);
	free(buffer);
	free(chunk);
}

void* DFSReader::operator()(void*)
{
	size_t bytes_to_copy = chunksize;

	char *this_chunk = chunk[next_chunk];
	FilterInfo* this_send = send[next_buffer];
	next_chunk = (next_chunk + 1) % aggregator->getNumBuffers();

	if (aggregator->input_finished) {
        if (aggregator->voteTerminate)
            return NULL;
        else {
            aggregator->voteTerminate = true;
            this_send->result = NULL;
            this_send->length = 0;
            return this_send;
        }
	}
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

	aggregator->tot_input_tokens++;
	if (id > input->chunk_id_end && rem_buffer_size <= 0) {
		aggregator->input_finished = true;
        aggregator->voteTerminate = true;
	}
	this_send->result = this_chunk;
	this_send->length = 1;
	return this_send;
}
