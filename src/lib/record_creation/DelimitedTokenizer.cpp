#include "DelimitedTokenizer.h"

DelimitedTokenizer::DelimitedTokenizer(const char* delim)
{
	delimiters = (char*)malloc(100);
	strcpy(delimiters, delim);
}

DelimitedTokenizer::~DelimitedTokenizer()
{
	free(delimiters);
}

/**
 * @param[in] data_fragments	mutable buffer that needs to be tokenized
 * @param[in] num_tokens	ignored
 * @param[out] tokens		list of tokens. NULL if none. Memory
 *   for each Token as well as the array of Tokens will be allocated
 *   and must be freed by caller. 
 */
uint64_t DelimitedTokenizer::getTokens(void*& data_fragments, 
		uint64_t num_tokens, Token**& tokens)
{
	char *spl = NULL ;
	size_t tok_ctr = 0;
	char* buf = (char*)data_fragments;

	tokens = (Token**)malloc(sizeof(Token*) * num_tokens);

	spl = strtok(buf, delimiters);
	if (spl == NULL)
		return tok_ctr;
	tokens[tok_ctr++]->tokens.push_back((void*)spl);
	while (1) {
		spl = strtok(NULL, delimiters);
		if (spl == NULL)
			return tok_ctr;
		tokens[tok_ctr++]->tokens.push_back((void*)spl);
	}
}

