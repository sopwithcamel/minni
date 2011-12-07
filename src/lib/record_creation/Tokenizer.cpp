#include "Tokenizer.h"

Token::Token()
{
	tokens.reserve(4);
	token_sizes.reserve(4);
}

Token::Token(const Token& rhs)
{
	int i;
	void* buf;
	token_sizes.reserve(rhs.token_sizes.size());
	tokens.reserve(rhs.tokens.size());
	objs.reserve(rhs.objs.size());
	for (i=0; i<rhs.tokens.size(); i++) {
		buf = malloc(rhs.token_sizes[i] + 1);
		memcpy(buf, rhs.tokens[i], rhs.token_sizes[i]);
		tokens.push_back(buf);
		token_sizes.push_back(rhs.token_sizes[i]);
	}
}

Token::~Token()
{
	int i;
	for (i=0; i<tokens.size(); i++)
		free(tokens[i]);
	for (i=0; i<objs.size(); i++)
		delete objs[i];
}

void Token::clear()
{
	tokens.clear();
	token_sizes.clear();
	objs.clear();
}
