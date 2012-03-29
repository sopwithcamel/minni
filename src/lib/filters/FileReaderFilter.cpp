#include "config.h"
#include "FileReaderFilter.h"

FileReaderFilter::FileReaderFilter(Aggregator* agg, MapInput* _input) :
		aggregator(agg),
		filter(serial_in_order),
		files_sent(0),
		files_per_call(100)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	input = (FileInput*)_input;
	input->getFileNames(file_list);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
	file_names = new MultiBuffer<char*>(num_buffers, files_per_call);
	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<files_per_call; j++) {
			(*file_names)[i][j] = (char*)malloc(FILENAME_LENGTH);
		}			
	}
}

FileReaderFilter::~FileReaderFilter()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	// Freeing memory allocated by FileInput
	file_list.clear();

	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<files_per_call; j++)
			free(file_names[i][j]);
	}
	delete file_names;
	delete send;
}

void* FileReaderFilter::operator()(void*)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	size_t fil_lim = files_sent + files_per_call;

	if (aggregator->input_finished) {
		return NULL;
	}

	char** this_name_list = (*file_names)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 

	if (fil_lim > file_list.size()) {
		fil_lim = file_list.size();
		aggregator->input_finished = true;
	}

	for (int i=files_sent; i<fil_lim; i++) {
		strcpy(this_name_list[i-files_sent], file_list[i].c_str());
	}
	
	aggregator->tot_input_tokens++;
	
	this_send->result = this_name_list;
	this_send->length = (fil_lim - files_sent);	

	files_sent += (fil_lim - files_sent);
	return this_send;
}
