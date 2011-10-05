#include "Sorter.h"

Sorter::Sorter(const uint64_t num_part,
			const char* inp_prefix,
			const char* out_prefix,
			const int nsort_mem) :
		filter(serial_in_order),
		num_part(num_part),
		nsort_memory(nsort_mem)
{
	inputfile_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(inputfile_prefix, inp_prefix);
	outputfile_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(outputfile_prefix, out_prefix);
}

Sorter::~Sorter()
{
	free(inputfile_prefix);
	free(outputfile_prefix);
}

void* Sorter::operator()(void*)
{
	char* input_file = (char*)malloc(FILENAME_LENGTH);
	char* out_file = (char*)malloc(FILENAME_LENGTH);
	char* bnum = (char*)malloc(10);
	char* nsort_command = (char*)malloc(512);

	for (int i=0; i<num_part; i++) {
		sprintf(bnum, "%llu", i);

		strcpy(input_file, inputfile_prefix);
		strcat(input_file, bnum);

		strcpy(out_file, outputfile_prefix);
		strcat(out_file, bnum);

		strcpy(nsort_command, "nsort ");
		if (nsort_memory > 0) {
			char* mem_str = (char*)malloc(10);
			sprintf(mem_str, "%d", nsort_memory);
			strcat(nsort_command, "-memory=");
			strcat(nsort_command, mem_str);
			strcat(nsort_command, "M ");
			free(mem_str);
		}
		strcat(nsort_command, input_file);
		strcat(nsort_command, " -o ");
		strcat(nsort_command, out_file);
		
		system(nsort_command);
	}
	free(nsort_command);
	free(input_file);
	free(out_file);
	free(bnum);
	return NULL;
}
