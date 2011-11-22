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
	file_content_list = (char***)malloc(sizeof(char**) * num_buffers);
	file_name_list = (char***)malloc(sizeof(char**) * num_buffers);
	file_size_list = (size_t**)malloc(sizeof(size_t*) * num_buffers);
	for (int i=0; i<num_buffers; i++) {
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
		file_content_list[i] = (char**)malloc(sizeof(char*) * 
				files_per_call);
		file_name_list[i] = (char**)malloc(sizeof(char*) * 
				files_per_call);
		file_size_list[i] = (size_t*)malloc(sizeof(size_t) * 
				files_per_call);
		for (int j=0; j<files_per_call; j++) {
			file_content_list[i][j] = NULL;
			file_name_list[i][j] = (char*)malloc(FILENAME_LENGTH);
		}			
	}
}

FileReader::~FileReader()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	// Freeing memory allocated by FileInput
	file_list.clear();

	for (int i=0; i<num_buffers; i++) {
		free(file_content_list[i]);	
		free(file_name_list[i]);	
		free(file_size_list[i]);	
		free(send[i]);
		for (int j=0; j<files_per_call; j++)
			free(file_name_list[i][j]);
	}
	free(file_content_list);
	free(file_name_list);
	free(file_size_list);
	free(send);
}

void* FileReader::operator()(void*)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	size_t fil_lim = files_sent + files_per_call;

	if (aggregator->input_finished) {
		return NULL;
	}

	char** this_content_list = file_content_list[next_buffer];
	char** this_name_list = file_name_list[next_buffer];
	size_t* this_size_list = file_size_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 

	if (fil_lim > file_list.size()) {
		fil_lim = file_list.size();
		aggregator->input_finished = true;
	}
	/* Free memory from previous invocations */
	for (int j=0; j<files_per_call; j++) {
		if (NULL != this_content_list[j]) {
			free(this_content_list[j]);
			this_content_list[j] = NULL;
		}
	}
	for (int i=files_sent; i<fil_lim; i++) {
		input->readFile(file_list[i], &this_content_list[i-files_sent],
				this_size_list[i]);
		strcpy(this_name_list[i-files_sent], file_list[i].c_str());
	}
	
	aggregator->tot_input_tokens++;
	
	this_send->result = this_content_list;
	this_send->result1 = this_name_list;
	this_send->result2 = this_size_list;
	this_send->length = (fil_lim - files_sent);	

	files_sent += (fil_lim - files_sent);
	return this_send;
}
