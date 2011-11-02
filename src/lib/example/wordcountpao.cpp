#include "wordcountpao.h"

WordCountPartialAgg::WordCountPartialAgg(const char* const token)
{
	size_t eok = strlen(token);
	key = (char*)malloc(eok + VALUE_SIZE + 1);
	strcpy(key, token);
	char* c_value = key + eok + 1;
	c_value[0] = '1';
	c_value[1] = '\0';
	value = c_value;
}

WordCountPartialAgg::~WordCountPartialAgg()
{
	free(key);
}

void WordCountPartialAgg::add(const char* v)
{
	char* c_value = (char*)value;
	int val = atoi(v);
	int curr_val = atoi(c_value);
	curr_val += val;
	sprintf(c_value, "%d", curr_val);
}

void WordCountPartialAgg::merge(PartialAgg* add_agg)
{
	char* c_value = (char*)value;
	int val = atoi((char*)(add_agg->value));
	int curr_val = atoi(c_value);
	curr_val += val;
	sprintf(c_value, "%d", curr_val);
}

void WordCountPartialAgg::serialize(FILE* f)
{
	char* c_value = (char*)value;
	char buf[BUF_SIZE + 1];
	strcpy(buf, key);
	strcat(buf, " ");
	strcat(buf, c_value);
	strcat(buf, "\n");
	assert(NULL != f);
	size_t l = strlen(buf);
	assert(fwrite(buf, sizeof(char), l, f) == l);
}

bool WordCountPartialAgg::deserialize(FILE* f, void *buf)
{
	char *spl;
	char *read_buf = (char*)buf;
	if (feof(f)) {
		return false;
	}
	if (fgets(read_buf, BUF_SIZE, f) == NULL)
		return false;
	spl = strtok(read_buf, " \n\r");
	if (spl == NULL)
		return false;
	strcpy(key, spl);
	spl = strtok(NULL, " \n\r");
	if (spl == NULL)
		return false;
	value = key + strlen(key) + 1;
	strcpy((char*)value, spl);
	return true;
}

REGISTER_PAO(WordCountPartialAgg);
