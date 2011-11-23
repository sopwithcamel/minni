#include "Tokenizer.h"

Token::Token()
{
	tokens.reserve(4);
	token_sizes.reserve(4);
}

Token::Token(const Token& rhs)
{
	int i;
	token_sizes.reserve(rhs.token_sizes.size());
	tokens.reserve(rhs.tokens.size());
	objs.reserve(rhs.objs.size());
	for (i=0; i<rhs.tokens.size(); i++) {
		tokens[i] = malloc(rhs.token_sizes[i] + 1);
		memcpy(tokens[i], rhs.tokens[i], rhs.token_sizes[i]);
		token_sizes[i] = rhs.token_sizes[i];
	}
}

Token::~Token()
{
	int i;
	fprintf(stderr, "%s:%d\n", tokens[0], tokens.size());
//	for (i=0; i<tokens.size(); i++)
	for (i=0; i<2; i++)
		free(tokens[i]);
	for (i=0; i<objs.size(); i++)
		delete objs[i];
}
