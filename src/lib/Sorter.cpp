#include "Sorter.h"

Sorter::Sorter(const uint64_t num_buckets,
			const char* inp_prefix) :
		filter(serial_in_order),
		num_buckets(num_buckets),
		inputfile_prefix(inp_prefix),
		buckets_processed(0)
{
}

Sorter::~Sorter()
{
}

void* Sorter::operator()(void*)
{
	if (buckets_processed == num_buckets)
		return NULL;
	buckets_processed++;
	
	char* input_file = (char*)malloc(FILENAME_LENGTH);
	char* out_file = (char*)malloc(FILENAME_LENGTH);
	char* bnum = (char*)malloc(10);
	char* spl;
	
	sprintf(bnum, "%llu", buckets_processed++);

	strcpy(input_file, inputfile_prefix);
	strcat(input_file, "bucket");
	strcat(input_file, bnum);

	strcpy(out_file, inputfile_prefix);
	strcat(out_file, "sortedbucket");
	strcat(out_file, bnum);

	char* nsort_command = (char*)malloc(512);
	strcpy(nsort_command, "nsort ");
	strcat(nsort_command, input_file);
	strcat(nsort_command, " ");
	strcat(nsort_command, out_file);
	assert(system(nsort_command));

	free(nsort_command);
	free(input_file);
	free(out_file);
	free(bnum);
}
