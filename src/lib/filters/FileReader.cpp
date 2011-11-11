#include "FileReader.h"

FileReader::FileReader(Aggregator* agg, MapInput* _input) :
		aggregator(agg),
		filter(serial_in_order),
		files_sent(0),
		files_per_call(100)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	input = (FileInput*)_input;
	input->getFileNames(file_list);
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	file_send_list = (char***)malloc(sizeof(char**) * num_buffers);
	for (int i=0; i<num_buffers; i++) {
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
		file_send_list[i] = (char**)malloc(sizeof(char*) * 
				files_per_call);
	}
}

FileReader::~FileReader()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	// Freeing memory allocated by FileInput
	file_list.clear();

	for (int i=0; i<num_buffers; i++) {
		free(file_send_list[i]);	
		free(send[i]);
	}
	free(file_send_list);
	free(send);
}

void* FileReader::operator()(void*)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	size_t fil_lim = files_sent + files_per_call;

	if (aggregator->input_finished) {
		return NULL;
	}

	char** this_file_list = file_send_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 

	if (fil_lim > file_list.size()) {
		fil_lim = file_list.size();
		aggregator->input_finished = true;
	}
	for (int i=files_sent; i<fil_lim; i++) {
		input->readFile(file_list[i], this_file_list[i-files_sent]);
	}
	
	aggregator->tot_input_tokens++;
	
	this_send->result = this_file_list;
	this_send->length = (fil_lim - files_sent);	

	files_sent += (fil_lim - files_sent);
	return this_send;
}
