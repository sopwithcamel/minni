#include "wordcountpao.h"

WordCountPartialAgg::WordCountPartialAgg(const char* token)
{
	key = (char*)malloc(strlen(token)+1);
	value = (char*)malloc(VALUE_SIZE);
	strcpy(key, token);
	strcpy(value, "1");
}

WordCountPartialAgg::~WordCountPartialAgg()
{
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

REGISTER_PAO(WordCountPartialAgg);
