#include "TokenizerFilter.h"

TokenizerFilter::TokenizerFilter(Aggregator* agg, const Config& cfg,
			const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
		max_keys_per_token(max_keys),
		next_buffer(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
	tokens = new MultiBuffer<Token*>(num_buffers, max_keys_per_token);

	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
			(*tokens)[i][j] = new Token();
		}
	}

	Setting& c_query = readConfigFile(cfg, "minni.query");
	int query = c_query;

	Setting& c_delimf = readConfigFile(cfg, "minni.delimiter");
	string delimf = (const char*)c_delimf;

	string delims;
	if (configExists(cfg, "minni.delimiter_second")) {
		Setting& c_delims = readConfigFile(cfg, "minni.delimiter_second");
		string delims = (const char*)c_delims;
		chunk_tokenizer = new DelimitedTokenizer(delimf.c_str(),
				delims.c_str());
	} else
		chunk_tokenizer = new DelimitedTokenizer(delimf.c_str());

	if (1 == query) {
		Setting& c_query_file = readConfigFile(cfg, 
				"minni.query_file");
		string query_file = (const char*)c_query_file;
		memCache = new MemCache(query_file.c_str(), WORD);
	} else {
		memCache = NULL;
	}

}

TokenizerFilter::~TokenizerFilter()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
            delete (*tokens)[i][j];
		}
	}

	delete tokens; 
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

	Token** this_token_list = (*tokens)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 

    if (recv_length == 0)
        goto pass_through;

	for (int i=0; i < max_keys_per_token; i++)
        this_token_list[i]->clear();
	// fetch all tokens from chunk
	num_tokens = chunk_tokenizer->getTokens(tok_buf, max_keys_per_token,
			this_token_list);
	
	if (!memCache)
		goto pass_through;
	for (int i=0; i<memCache->size(); i++) {
		for (int j=0; j<num_tokens; j++) {
			// do a shallow copy of the file token
			// for the first iteration, use the existing
			// token
			size_t mi_size = memCache->getItemSize(i);
			void* mem_it = memCache->getItem(i);

			if (i == 0) {
				this_token_list[j]->tokens.push_back(mem_it);
				this_token_list[j]->token_sizes.push_back(
						mi_size);
			} else {
				new_token = this_token_list[num_tokens++];
				// initialize new_token
				*new_token = Token(*this_token_list[j]);
				new_token->tokens.push_back(mem_it);
				new_token->token_sizes.push_back(
						mi_size);
				assert(num_tokens < max_keys_per_token);
			}
		}
	}
pass_through:
	this_send->result = this_token_list;
	this_send->length = num_tokens;
	this_send->flush_hash = false;

	return this_send;
}

