#include "LocalReader.h"
#include <sstream>

using namespace std;
#define BUF_SIZE    65535

LocalReader::LocalReader(Aggregator* agg,
        const char* inp_prefix, 
        size_t max_keys) :
    aggregator(agg),
    filter(serial_in_order),
    max_keys_per_token(max_keys),
    inp(NULL),
    next_buffer(0)
{
    size_t num_buffers = aggregator->getNumBuffers();

    inputfile_prefix = (char*)malloc(FILENAME_LENGTH);
    strcpy(inputfile_prefix, inp_prefix);

	tokens = new MultiBuffer<Token*>(num_buffers, max_keys_per_token);
	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
			(*tokens)[i][j] = new Token();
		}
	}

	chunk = new MultiBuffer<char*>(num_buffers, 1);
	for (int i=0; i < num_buffers; i++) {
		(*chunk)[i][0] = (char*)malloc(max_keys_per_token * 80);
	}
    send = new MultiBuffer<FilterInfo>(num_buffers, 1);
    read_buf = malloc(BUF_SIZE + 1);
}

LocalReader::~LocalReader()
{
    size_t num_buffers = aggregator->getNumBuffers();
    free(inputfile_prefix);
	for (int i=0; i<num_buffers; i++) {
		for (int j=0; j<max_keys_per_token; j++) {
            delete (*tokens)[i][j];
		}
	}
	delete tokens; 
	for (int i=0; i<aggregator->getNumBuffers(); i++) {
		free((*chunk)[i][0]);
	}
	delete chunk;
    delete send;
    free(read_buf);
}


void* LocalReader::operator()(void*)
{
    size_t pao_list_ctr = 0;
    size_t num_buffers = aggregator->getNumBuffers();
    PartialAgg* new_pao;

	Token** this_token_list = (*tokens)[next_buffer];
	char *this_chunk = (*chunk)[next_buffer][0];
    this_chunk[0] = '\0';
    FilterInfo* this_send = (*send)[next_buffer];
    next_buffer = (next_buffer + 1) % num_buffers;

    if (aggregator->input_finished) {
        if (aggregator->can_exit)
            return NULL;
        else {
            aggregator->can_exit = true;
            aggregator->stall_pipeline = false;

            this_send->result = NULL;
            this_send->length = 0;
            this_send->flush_hash = true;
            this_send->destroy_pao = false;
            // still have to count this as a token
            aggregator->tot_input_tokens++;
            return this_send;
        }
    }
    if (aggregator->stall_pipeline) {
        aggregator->stall_pipeline = false;

        this_send->result = NULL;
        this_send->length = 0;
        this_send->flush_hash = true;
        this_send->destroy_pao = false;
        // still have to count this as a token
        aggregator->tot_input_tokens++;
        return this_send;
    } else {
        aggregator->can_exit = false;
        aggregator->stall_pipeline = false;
    }
    aggregator->tot_input_tokens++;

	if (!inp) { // new bucket has to be opened
        stringstream ss;
        ss << aggregator->getJobID();
        string file_name = inputfile_prefix;
        file_name = file_name + "input" + ss.str();
        inp = fopen(file_name.c_str(), "rb");
        fprintf(stderr, "opening file %s\n", file_name.c_str());
    }

	for (int i=0; i < max_keys_per_token; i++)
        this_token_list[i]->clear();

    uint64_t tok_ctr = 0;
    char* buf = this_chunk;
    char* ptr;
    while (tok_ctr < max_keys_per_token) {
        if (fgets(buf, BUF_SIZE, inp) == NULL)
            break;
        int offset = strlen(buf);
        char* spl = strtok_r(buf, " \n\r\t", &ptr);
        if (spl == NULL)
            break;
        Token* tok = this_token_list[tok_ctr++];
        do {
            tok->tokens.push_back(spl);
            spl = strtok_r(NULL, " \n\r\t", &ptr);
        } while (spl);
        if (tok_ctr >= max_keys_per_token-20) {
            fprintf(stderr, "Sending %d elements\n", tok_ctr);
            break;
        }
        buf = buf + offset + 1;
    }

    if (feof(inp)) {
        fclose(inp);
        inp = NULL;
        // ask hashtable to flush itself afterwards
        this_send->flush_hash = true;
        aggregator->input_finished = true;
    } else
        this_send->flush_hash = false;
    //  fprintf(stderr, "list size: %d\n", pao_list_ctr);
    this_send->result = this_token_list;
    this_send->length = tok_ctr;
    return this_send;
}
