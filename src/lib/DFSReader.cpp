#include "config.h"
#include "DFSReader.h"

DFSReader::DFSReader(MapInput* _input) :
		filter(serial_in_order),
		next_buffer(0),
		chunk_ctr(0),
		input(_input)
{
	for (int i = 0; i < n_buffer; i++)
		buf[i] = (char*)malloc(BUFSIZE);
	id = input->chunk_id_start;
}

DFSReader::~DFSReader()
{
	cout << "Destroying DFSReader" << endl;
	for (int i = 0; i < n_buffer; i++)
		free(buf[i]);
}

void* DFSReader::operator()(void*)
{
	size_t ret;
	if (id > input->chunk_id_end) {
		cout << "\t\t\tFinishing?" << endl;
		return NULL;
	}
	buffer = buf[next_buffer];
	next_buffer = (next_buffer + 1) % NUM_BUFFERS;
	cout << "Reading in buffer " << id << endl;
	ret = input->key_value(&buffer, id); 
	id++;
	chunk_ctr++;
	return buffer;
}

