#ifndef LIB_TOKENIZER_H
#define LIB_TOKENIZER_H
#include <stdint.h>
#include <vector>

class Token {
  public:
	Token()
	{
		tokens.reserve(4);
		token_sizes.reserve(4);
	}
	~Token() {}
	std::vector<char*> tokens;
	std::vector<size_t> token_sizes;
};

class Tokenizer {
  public:
	Tokenizer() {}
	~Tokenizer() {}
	/**
	 * @param[in] data_fragments	uninterpreted data
	 * @param[in] num_tokens	requested number of tokens
	 * @param[out] tokens		list of tokens. NULL if none. Memory
	 *   for each Token as well as the array of Tokens will be allocated
	 *   and must be freed by caller. 
	 */
	virtual void getTokens(void*& data_fragments, uint64_t num_tokens,
			Token**& tokens) = 0;
};

#endif
