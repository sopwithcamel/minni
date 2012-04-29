#include "DelimitedTokenizer.h"

DelimitedTokenizer::DelimitedTokenizer(const char* delim)
{
	delimiters = (char**)malloc(sizeof(char*) * 2);
	delimiters[0] = (char*)malloc(100);
	strcpy(delimiters[0], delim);
	delimiters[1] = NULL;
}

DelimitedTokenizer::DelimitedTokenizer(const char* delim_first,
		const char* delim_sec)
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
	Token* tok;

/*
	for (str1=buf;; str1=NULL) {
		// split token
		spl = strtok_r(str1, delimiters[0], &saveptr1);
		if (spl == NULL)
			return tok_ctr;
		tok = tokens[tok_ctr++];
        if (tok_ctr == num_tokens-1)
            break;

		if (delimiters[1]) { // sub-tokenize
			for (str2=spl;; str2=NULL) {
				spl_sec = strtok_r(str2, delimiters[1],
						&saveptr2);
				if (spl_sec == NULL)
					break;
				tok->tokens.push_back((void*)spl_sec);
			}
		} else {
			tok->tokens.push_back((void*)spl);
		}
	}
*/
    char* last = NULL;
	for (str1=buf;; str1=NULL) {
		// split token
		spl = strtok_r(str1, delimiters[0], &saveptr1);
		if (spl == NULL)
			return tok_ctr;
        if (last == NULL) {
            last = spl;
            continue;
        }
        if (tok_ctr == num_tokens-1)
            return tok_ctr;
		tok = tokens[tok_ctr++];
        tok->tokens.push_back((void*)last);
        tok->tokens.push_back((void*)spl);
        last = spl;
	}
}
