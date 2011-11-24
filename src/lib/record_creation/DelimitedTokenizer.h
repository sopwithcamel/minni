#ifndef LIB_DELIMITEDTOKENIZER_H
#define LIB_DELIMITEDTOKENIZER_H
#include "Tokenizer.h"
#include "MapInput.h"
#include <vector>
#include "Defs.h"

class DelimitedTokenizer : public Tokenizer {
public:
	DelimitedTokenizer(const char* delim);
	DelimitedTokenizer(const char* delim_first, const char* delim_sec);
	~DelimitedTokenizer();
	uint64_t getTokens(void*& data_fragments, uint64_t num_tokens,
			Token**& tokens);
private:
	char** delimiters;
	size_t num_tokens_sent;
	vector<string> file_list;
};

#endif // LIB_DELIMITEDTOKENIZER_H
