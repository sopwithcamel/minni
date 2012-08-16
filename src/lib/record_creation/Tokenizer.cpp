#include "Tokenizer.h"

Token::Token()
{
}

void Token::init(const Token& rhs)
{
	int i;
	void* buf;
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
/* TODO: Fix
	for (i=0; i<tokens.size(); i++)
		free(tokens[i]);
*/
/*
	for (i=0; i<objs.size(); i++)
		delete objs[i];
*/
}

void Token::clear()
{
	tokens.clear();
	token_sizes.clear();
//	objs.clear();
}
