#include "wordcountpao.h"

WordCountPartialAgg::WordCountPartialAgg(const char* const token)
{
	size_t eok = strlen(token);
	key = (char*)malloc(eok + VALUE_SIZE + 1);
	strcpy(key, token);
	value = key + eok + 1;
	value[0] = '1';
	value[1] = '\0';
}

WordCountPartialAgg::~WordCountPartialAgg()
{
	free(key);
}

void WordCountPartialAgg::add (const char* v)
{
	int val = atoi(v);
	int curr_val = atoi(value);
	curr_val += val;
	sprintf(value, "%d", curr_val);
}

void WordCountPartialAgg::merge (PartialAgg* add_agg)
{
	int val = atoi(add_agg->value);
	int curr_val = atoi(value);
	curr_val += val;
	sprintf(value, "%d", curr_val);
}

void WordCountPartialAgg::serialize(FILE* f)
{
	char buf[BUF_SIZE + 1];
	strcpy(buf, key);
	strcat(buf, " ");
	strcat(buf, value);
	strcat(buf, "\n");
	assert(NULL != f);
	size_t l = strlen(buf);
	assert(fwrite(buf, sizeof(char), l, f) == l);
}

REGISTER_PAO(WordCountPartialAgg);
