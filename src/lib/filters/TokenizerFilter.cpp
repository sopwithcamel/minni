#include "TokenizerFilter.h"

TokenizerFilter::TokenizerFilter(Aggregator* agg, const Config& cfg,
			PartialAgg* (*createPAOFunc)(Token* t),
			const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
		max_keys_per_token(max_keys),
		next_buffer(0),
		createPAO(createPAOFunc)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);

	Setting& c_query = readConfigFile(cfg, "minni.query");
	int query = c_query;

	if (1 == query) {
		Setting& c_query_file = readConfigFile(cfg, 
				"minni.query_file");
		string query_file = (const char*)c_query_file;
		memCache = new MemCache(query_file.c_str(), WORD);
	} else {
		memCache = NULL;
	}

	chunk_tokenizer = new DelimitedTokenizer(" .\n\r\'\"?,;:!*()-\uFEFF");
}

TokenizerFilter::~TokenizerFilter()
{
	delete chunk_tokenizer;
	delete send;
}

/**
 * Can't use const for argument because strtok modifies the string. 
 * Not re-entrant!
 */
void* TokenizerFilter::operator()(void* input_data)
{
	char *spl = NULL ;
	size_t tok_ctr = 0;
	size_t num_tokens = 0;
	uint64_t num_buffers = aggregator->getNumBuffers();
	Token* new_token;

	FilterInfo* recv = (FilterInfo*)input_data;
	void* tok_buf = recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	

	// fetch all tokens from chunk
	Token** chunk_tokens;
	num_tokens = chunk_tokenizer->getTokens(tok_buf, max_keys_per_token,
			chunk_tokens);

	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	if (!memCache)
		goto pass_through;
	for (int j=0; j<num_tokens; j++) {
		for (int i=0; i<memCache->size(); i++) {
			// do a shallow copy of the file token
			// for the first iteration, use the existing
			// token
			size_t mi_size = memCache->getItemSize(i);
			void* mem_it = malloc(mi_size);
			memcpy(mem_it, memCache->getItem(i), mi_size);
			if (i == 0) {
				chunk_tokens[j]->tokens.push_back(mem_it);
				chunk_tokens[j]->token_sizes.push_back(
						mi_size);
			} else {
				*new_token = Token(*chunk_tokens[j]);
				new_token->tokens.push_back(mem_it);
				new_token->token_sizes.push_back(
						mi_size);
				chunk_tokens[num_tokens] = new_token;
				assert(++num_tokens < max_keys_per_token);
			}
		}
	}
pass_through:
	this_send->result = chunk_tokens;
	this_send->length = num_tokens;
	this_send->flush_hash = false;

	return this_send;
}

