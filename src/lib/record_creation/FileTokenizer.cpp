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
void FileTokenizer::getTokens(void*& data_fragments, uint64_t num_tokens, 
		Token**& tokens)
{
	Token** ft_list;
	char* file_contents;
	size_t file_size;
	char** file_list = (char**)data_fragments;
	ft_list = (Token**)malloc(sizeof(Token*) * num_tokens);
	for (int i=0; i<num_tokens; i++) {
		ft_list[i] = new Token();
		char* file_name = (char*)malloc(FILENAME_LENGTH);	
		strcpy(file_name, file_list[i]);
		ft_list[i]->tokens.push_back(file_name);
		ft_list[i]->token_sizes.push_back(FILENAME_LENGTH);

		input->readFile(file_list[i], file_contents, file_size);
		ft_list[i]->tokens.push_back(file_contents);
		ft_list[i]->token_sizes.push_back(file_size);
	}
	tokens = (Token**)ft_list;
}
