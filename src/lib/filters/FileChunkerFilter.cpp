#include "config.h"
#include "FileChunkerFilter.h"

FileChunkerFilter::FileChunkerFilter(Aggregator* agg, MapInput* _input,
            const Config& cfg, const size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
        max_keys_per_token(max_keys),
		files_sent(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	input = (FileInput*)_input;
    // fetch list of file names
	input->getFileNames(file_list);
    for (uint64_t i=0; i<file_list.size(); i++)
        fprintf(stderr, "File: %s\n", file_list[i].c_str());

	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
	tokens = new MultiBuffer<Token*>(num_buffers, max_keys_per_token);
	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
			(*tokens)[i][j] = new Token();
		}
	}

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
}

FileChunkerFilter::~FileChunkerFilter()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	// Freeing memory allocated by FileInput
	file_list.clear();

	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
            delete (*tokens)[i][j];
		}
	}
	delete send;
    delete chunk_tokenizer;
}

void* FileChunkerFilter::operator()(void*)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	Token** this_token_list = (*tokens)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers; 

    if (aggregator->input_finished) {
        if (aggregator->can_exit)
            return NULL;
        else {
            this_send->result = NULL;
            this_send->length = 0;
            // still have to count this as a token
            aggregator->tot_input_tokens++;
            return this_send;
        }
    }
	aggregator->tot_input_tokens++;

    // read file
    uint64_t file_size;
    char* file_data;
    input->readFile(file_list[files_sent], file_data, file_size);

	for (int i=0; i < max_keys_per_token; i++)
        this_token_list[i]->clear();

	// fetch all tokens from file
    void* contents = (void*)file_data;
	size_t num_tokens = chunk_tokenizer->getTokens(contents,
            max_keys_per_token, this_token_list);
    fprintf(stderr, "Num tokens read:%d\n", num_tokens);	

    
	for (int i=0; i < max_keys_per_token; i++)
        this_token_list[i]->tokens.push_back((void*)(file_list[files_sent].c_str()));

	if (++files_sent == file_list.size()) {
		aggregator->input_finished = true;
	}

	this_send->result = this_token_list;
	this_send->length = num_tokens;

	return this_send;
}
