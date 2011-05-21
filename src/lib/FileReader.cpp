#include "config.h"
#include "FileReader.h"

FileReader::FileReader(FILE* _input_file) :
		filter(serial_in_order),
		next_buffer(0),
		chunk_ctr(0),
		input_file(_input_file)
{
	for (int i = 0; i < n_buffer; i++)
		buf[i] = (char*)malloc(BUFSIZE);
}

FileReader::~FileReader()
{
	for (int i = 0; i < n_buffer; i++)
		free(buf[i]);
}

void* FileReader::operator()(void*)
{
	size_t ret;
	buffer = buf[next_buffer];
	ret = fread(buffer, sizeof(char), BUFSIZE, input_file);
	chunk_ctr++;
	next_buffer = (next_buffer + 1) % NUM_BUFFERS;
	if (!ret || chunk_ctr == 16) { // EOF
		return NULL;
	} else {
		return buffer;
	}
}

