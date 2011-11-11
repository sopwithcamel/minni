#include "Tokenizer.h"

Tokenizer::Tokenizer(Aggregator* agg, const Config& cfg,
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
		Setting& c_query_type = readConfigFile(cfg, 
				"minni.query_type");
		string query_type = (const char*)c_query_type;
		memCache = new MemCache(query_file.c_str(), query_type.c_str());
	} else {
		memCache = NULL;
	}
}

Tokenizer::~Tokenizer()
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
void* Tokenizer::operator()(void* buffer)
{
	char *spl = NULL ;
	char* tok_buf = (char*) buffer;	 
	int tok_ctr = 0;
	size_t this_list_ctr = 0;
	size_t mc_size;
	uint64_t num_buffers = aggregator->getNumBuffers();
	// passes just one token to createPAO
	char** tokens = (char**)malloc(sizeof(char*) * 2);
	PartialAgg* new_pao;

	PartialAgg** this_pao_list = pao_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	int tok_flag = 1; // for strtok to behave differently
	PartialAgg* dummyPAO = createPAO(NULL);
	if (tok_buf == NULL) { 
		perror("Buffer sent to Tokenizer is empty!");
		exit(1);
	}
	if (memCache)
		mc_size = memCache->size();
	while (1) {
		if (!dummyPAO->tokenize(tok_buf, &tok_flag, tokens))
			break;
		if (memCache) {
			for (int i=0; i<mc_size; i++) {
				tokens[1] = (*memCache)[i];
				new_pao = createPAO((const char**)tokens); 
				this_pao_list[this_list_ctr++] = new_pao;
				assert(this_list_ctr < max_keys_per_token);
			}
		} else {
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

