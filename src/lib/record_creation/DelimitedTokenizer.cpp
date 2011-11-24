#include "DelimitedTokenizer.h"

DelimitedTokenizer::DelimitedTokenizer(const char* delim)
{
	delimiters = (char**)malloc(sizeof(char*) * 2);
	delimiters[0] = (char*)malloc(100);
	strcpy(delimiters[0], delim);
	delimiters[1] = NULL;
}

DelimitedTokenizer::DelimitedTokenizer(const char* delim_first, const char* delim_sec)
{
	delimiters = (char**)malloc(sizeof(char*) * 2);
	delimiters[0] = (char*)malloc(100);
	strcpy(delimiters[0], delim_first);
	delimiters[1] = (char*)malloc(100);
	strcpy(delimiters[1], delim_sec);
}

DelimitedTokenizer::~DelimitedTokenizer()
{
	free(delimiters[0]);
	free(delimiters[1]);
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
	char* spl;
	char* spl_sec; 
	size_t tok_ctr = 0;
	char* buf = (char*)data_fragments;
	char *saveptr1, *saveptr2;
	char *str1, *str2;
	char* new_spl;
	Token* tok;
	tokens = (Token**)malloc(sizeof(Token*) * num_tokens);

	for (str1=buf;; str1=NULL) {
		// split token
		spl = strtok_r(str1, delimiters[0], &saveptr1);
		if (spl == NULL)
			return tok_ctr;
		// create a new Token
		tok = new Token();
		tokens[tok_ctr++] = tok;

		if (delimiters[1]) { // sub-tokenize
			for (str2=spl;; str2=NULL) {
				spl_sec = strtok_r(str2, delimiters[1],
						&saveptr2);
				if (spl_sec == NULL)
					break;
				new_spl = (char*)malloc(strlen(spl_sec) + 1);
				strcpy(new_spl, spl_sec);
				tok->tokens.push_back((void*)new_spl);
			}
		} else {
			new_spl = (char*)malloc(strlen(spl) + 1);
			strcpy(new_spl, spl);
			tok->tokens.push_back((void*)new_spl);
		}
	}
}
