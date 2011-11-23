#include "FileTokenizer.h"

FileTokenizer::FileTokenizer(MapInput* inp)
{
	input = (FileInput*)inp;
}

FileTokenizer::~FileTokenizer()
{
}

/**
 * @param[in] data_fragments	array of char-arrays with file names
 * @param[in] num_tokens	number of files in list
 * @param[out] tokens		list of tokens. NULL if none. Memory
 *   for each Token as well as the array of Tokens will be allocated
 *   and must be freed by caller. 
 */
uint64_t FileTokenizer::getTokens(void*& data_fragments, uint64_t num_tokens, 
		Token**& tokens)
{
	char* file_contents;
	size_t file_size;
	char** file_list = (char**)data_fragments;
	tokens = (Token**)malloc(sizeof(Token*) * num_tokens);
	for (int i=0; i<num_tokens; i++) {
		tokens[i] = new Token();
		char* file_name = (char*)malloc(FILENAME_LENGTH);	
		strcpy(file_name, file_list[i]);
		tokens[i]->tokens.push_back((void*)file_name);
		tokens[i]->token_sizes.push_back(FILENAME_LENGTH);

		input->readFile(file_list[i], file_contents, file_size);
		tokens[i]->tokens.push_back((void*)file_contents);
		tokens[i]->token_sizes.push_back(file_size);
	}
}
