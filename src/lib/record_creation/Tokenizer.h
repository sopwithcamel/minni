#ifndef LIB_TOKENIZER_H
#define LIB_TOKENIZER_H
#include <stdint.h>
#include <stdlib.h>
#include "PartialAgg.h"
#include <vector>

class PartialAgg;
class Token {
  public:
	Token();
	~Token();
	void init(const Token&);
	void clear();
	std::vector<void*> tokens;
	std::vector<size_t> token_sizes;
//	std::vector<PartialAgg*> objs;
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
	virtual uint64_t getTokens(void*& data_fragments, uint64_t num_tokens,
			Token**& tokens) = 0;
};

#endif
