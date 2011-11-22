#include "FileTokenizer.h"

FileTokenizer::FileTokenizer(Aggregator* agg, const Config& cfg,
			PartialAgg* (*createPAOFunc)(char** t, size_t* tok_siz),
			const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
		max_keys_per_token(max_keys),
		next_buffer(0),
		createPAO(createPAOFunc)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	tok_list = new MultiBuffer<char**>(num_buffers,
		 	max_keys_per_token); 
	tok_size_list = new MultiBuffer<size_t*>(num_buffers,
		 	max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);

	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
			(*tok_list)[i][j] = (char**)malloc(sizeof(char*) * 4);
			(*tok_size_list)[i][j] = (size_t*)malloc(
					sizeof(size_t) * 4);
		}
	}

	Setting& c_query = readConfigFile(cfg, "minni.query");
	int query = c_query;

	if (1 == query) {
		Setting& c_query_file = readConfigFile(cfg, 
				"minni.query_file");
		string query_file = (const char*)c_query_file;
		memCache = new MemCache(query_file.c_str(), FIL); 
	} else {
		memCache = NULL;
	}
}

FileTokenizer::~FileTokenizer()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
			free((*tok_list)[i][j]);
			free((*tok_size_list)[i][j]);
		}
	}
	delete tok_list;
	delete tok_size_list;
	delete send;
}

/**
 * Can't use const for argument because strtok modifies the string. 
 * Not re-entrant!
 */
void* FileTokenizer::operator()(void* input_data)
{
	char *spl = NULL ;
	int tok_ctr = 0;
	size_t this_list_ctr = 0;
	size_t mc_size;
	uint64_t num_buffers = aggregator->getNumBuffers();
	char** tokens;
	size_t* token_sizes;

	FilterInfo* recv = (FilterInfo*)input_data;
	void* file_cont_buf = recv->result;
	void* file_name_buf = recv->result1;
	void* file_size_buf = recv->result2;
	uint64_t recv_length = (uint64_t)recv->length;	

	char*** this_tok_list = (*tok_list)[next_buffer];
	size_t** this_tok_size_list = (*tok_size_list)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	uint64_t num_tokens_proc = 0;
	if (file_cont_buf == NULL) {
		perror("Buffer sent to FileTokenizer is empty!");
		exit(1);
	}

	if (memCache)
		mc_size = memCache->size();
	for (int j=0; j<recv_length; j++) {
		tokens = this_tok_list[j];
		token_sizes = this_tok_size_list[j];
		if (memCache) {
			tokens[2] = ((char**)file_name_buf)[j];
			token_sizes[2] = FILENAME_LENGTH;
			tokens[3] = ((char**)file_cont_buf)[j];
			token_sizes[3] = ((size_t*)file_size_buf)[j];
			for (int i=0; i<mc_size; i++) {
				tokens[0] = memCache->getItem(i);
				token_sizes[0] = FILENAME_LENGTH;
				tokens[1] = memCache->getFileContents(i);
				token_sizes[1] = memCache->getFileSize(i);
				assert(++this_list_ctr < max_keys_per_token);
			}
		} else {
			tokens[0] = ((char**)file_name_buf)[j];
			token_sizes[0] = FILENAME_LENGTH;
			tokens[1] = ((char**)file_cont_buf)[j];
			token_sizes[1] = ((size_t*)file_size_buf)[j];
			assert(++this_list_ctr < max_keys_per_token);
		}
	}
	this_send->result = this_tok_list;
	this_send->result1 = this_tok_size_list;
	this_send->length = this_list_ctr;
	this_send->flush_hash = false;

	return this_send;
}

