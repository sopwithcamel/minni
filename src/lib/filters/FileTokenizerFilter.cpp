#include "FileTokenizerFilter.h"

FileTokenizerFilter::FileTokenizerFilter(Aggregator* agg, const Config& cfg,
			MapInput* inp,
			const size_t max_keys) :
		aggregator(agg),
		input(inp),
		filter(serial_in_order),
		max_keys_per_token(max_keys),
		next_buffer(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);

	Setting& c_query = readConfigFile(cfg, "minni.query");
	int query = c_query;

	file_tokenizer = new FileTokenizer(input);

	if (1 == query) {
		Setting& c_query_file = readConfigFile(cfg, 
				"minni.query_file");
		string query_file = (const char*)c_query_file;
		memCache = new MemCache(query_file.c_str(), FIL); 
	} else {
		memCache = NULL;
	}
}

FileTokenizerFilter::~FileTokenizerFilter()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	delete send;
	delete file_tokenizer;
}

/**
 * Can't use const for argument because strtok modifies the string. 
 * Not re-entrant!
 */
void* FileTokenizerFilter::operator()(void* input_data)
{
	size_t mc_size;
	uint64_t num_buffers = aggregator->getNumBuffers();
	Token* new_token;

	FilterInfo* recv = (FilterInfo*)input_data;
	void* file_name_buf = recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	
	uint64_t this_list_ctr = recv_length;

	// Fetch all tokens for the current list of files
	Token** file_tokens;
	file_tokenizer->getTokens(file_name_buf, recv_length,
			file_tokens);
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 
	
	if (memCache)
		mc_size = memCache->size();
	for (int j=0; j<recv_length; j++) {
		if (memCache) {
			for (int i=0; i<mc_size; i++) {
				// do a shallow copy of the file token
				// for the first iteration, use the existing
				// token
				if (i == 0) 
					new_token = file_tokens[j];
				else
					*new_token = Token(*file_tokens[j]);
				new_token->tokens.push_back(
						memCache->getItem(i));
				new_token->tokens.push_back(
						memCache->getFileContents(i));
				new_token->token_sizes.push_back(
						FILENAME_LENGTH);
				new_token->token_sizes.push_back(
						memCache->getFileSize(i));
				assert(++this_list_ctr < max_keys_per_token);
			}
		} 
	}
	this_send->result = file_tokens;
	this_send->length = this_list_ctr;
	this_send->flush_hash = false;

	return this_send;
}
