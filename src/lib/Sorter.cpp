#include "Sorter.h"

Sorter::Sorter(const uint64_t num_part,
			const char* inp_prefix,
			const char* out_prefix) :
		filter(serial_in_order),
		num_part(num_part)
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

		strcpy(nsort_command, "nsort -memory=400M ");
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
