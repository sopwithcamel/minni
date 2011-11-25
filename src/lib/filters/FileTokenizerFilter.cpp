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
	tokens = new MultiBuffer<Token*>(num_buffers, max_keys_per_token);
	contents = new MultiBuffer<char*>(num_buffers, max_keys_per_token);
	content_list_sizes = new MultiBuffer<size_t>(num_buffers, 1);

	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
			(*tokens)[i][j] = new Token();
			(*contents)[i][j] = NULL;
		}
	}

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
	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
			delete (*tokens)[i][j];
			if ((*contents)[i][j] != NULL)
				free((*contents)[i][j]);
		}
	}

	delete content_list_sizes;
	delete contents;
	delete tokens; 
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

	Token** this_token_list = (*tokens)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	char** this_content_list = (*contents)[next_buffer];
	size_t* this_content_size_list = (*content_list_sizes)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 

	// Free contents from previous invocation
	for (int i=0; i<*this_content_size_list; i++) {
		if (this_content_list[i] != NULL) {
			free(this_content_list[i]);
			this_content_list[i] = NULL;
		}
	}

	// Fetch all tokens for the current list of files
	file_tokenizer->getTokens(file_name_buf, recv_length,
			this_token_list);

	*this_content_size_list = recv_length;
	for (int i=0; i<recv_length; i++) {
		this_content_list[i] = (char*)this_token_list[i]->tokens[1];
		fprintf(stderr, "%s\n", this_token_list[i]->tokens[1]);
	}
	
	if (!memCache)
		goto pass_through;
	for (int i=0; i<memCache->size(); i++) {
		for (int j=0; j<recv_length; j++) {
			// Do a shallow copy of the file token.
			// For the first iteration, use the existing
			// token
			size_t mi_size = memCache->getItemSize(i);
			void* mem_it = memCache->getItem(i);
			if (i == 0) {
				// File data
				this_token_list[j]->tokens.push_back(mem_it);
				this_token_list[j]->token_sizes.push_back(
						FILENAME_LENGTH);
				// File contents
				this_token_list[j]->tokens.push_back((void*)(
						memCache->getFileContents(i)));
				this_token_list[j]->token_sizes.push_back(
						mi_size);
			} else {
				*new_token = Token(*this_token_list[j]);
				new_token->tokens.push_back(mem_it);				
				new_token->token_sizes.push_back(
						FILENAME_LENGTH);
				new_token->tokens.push_back((void*)(
						memCache->getFileContents(i)));
				new_token->token_sizes.push_back(
						mi_size);
				this_token_list[this_list_ctr] = new_token;
				assert(++this_list_ctr < max_keys_per_token);
			}
		}
	}
pass_through:
	this_send->result = this_token_list;
	this_send->length = this_list_ctr;
	this_send->flush_hash = false;

	return this_send;
}
