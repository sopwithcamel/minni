#include "FileTokenizer.h"

FileTokenizer::FileTokenizer(Aggregator* agg, const Config& cfg,
			PartialAgg* (*createPAOFunc)(const char** t),
			const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
		max_keys_per_token(max_keys),
		next_buffer(0),
		createPAO(createPAOFunc)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	pao_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers); 
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		pao_list[i] = (PartialAgg**)malloc(sizeof(PartialAgg*) 
				* max_keys_per_token);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
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
		free(pao_list[i]);
		free(send[i]);
	}
	free(pao_list);
	free(send);
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
	// passes just one token to createPAO
	char** tokens = (char**)malloc(sizeof(char*) * 4);
	PartialAgg* new_pao;

	FilterInfo* recv = (FilterInfo*)input_data;
	void* file_cont_buf = recv->result;
	void* file_name_buf = recv->result1;
	uint64_t recv_length = (uint64_t)recv->length;	

	PartialAgg** this_pao_list = pao_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	uint64_t num_tokens_proc = 0;
	if (file_cont_buf == NULL) {
		perror("Buffer sent to FileTokenizer is empty!");
		exit(1);
	}
	if (memCache)
		mc_size = memCache->size();
	for (int j=0; j<recv_length; j++) {
		if (memCache) {
			tokens[2] = ((char**)file_name_buf)[j];
			tokens[3] = ((char**)file_cont_buf)[j];
			for (int i=0; i<mc_size; i++) {
				tokens[0] = memCache->getItem(i);
				tokens[1] = memCache->getFileContents(i);

				new_pao = createPAO((const char**)tokens);
				this_pao_list[this_list_ctr++] = new_pao;
				assert(this_list_ctr < max_keys_per_token);
			}
		} else {
			tokens[0] = ((char**)file_name_buf)[j];
			tokens[1] = ((char**)file_cont_buf)[j];

			new_pao = createPAO((const char**)tokens);
			this_pao_list[this_list_ctr++] = new_pao;
			assert(this_list_ctr < max_keys_per_token);
		}
	}
	this_send->result = this_pao_list;
	this_send->length = this_list_ctr;
	this_send->flush_hash = false;

	free(tokens);
	return this_send;
}

