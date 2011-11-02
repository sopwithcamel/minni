#include "FileReader.h"

FileReader::FileReader(FILE* _input_file, Aggregator* agg) :
		aggregator(agg),
		filter(serial_in_order),
		next_buffer(0),
		chunk_ctr(0),
		input_file(_input_file)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	buf = (char**)malloc(sizeof(char*) * num_buffers);
	for (int i = 0; i < num_buffers; i++)
		buf[i] = (char*)malloc(BUFSIZE);
}

FileReader::~FileReader()
{
	for (int i = 0; i < aggregator->getNumBuffers(); i++)
		free(buf[i]);
	free(buf);
}

void* FileReader::operator()(void*)
{
	size_t ret;
	buffer = buf[next_buffer];
	ret = fread(buffer, sizeof(char), BUFSIZE, input_file);
	chunk_ctr++;
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();
	if (!ret || chunk_ctr == 16) { // EOF
		return NULL;
	} else {
		return buffer;
	}
}

