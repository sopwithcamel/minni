#include "wordcountpao.h"

WordCountPartialAgg::WordCountPartialAgg(const char** const tokens)
{
	size_t eok = strlen(tokens[0]);
	key = (char*)malloc(eok + VALUE_SIZE + 1);
	strcpy(key, tokens[0]);
	char* c_value = key + eok + 1;
	c_value[0] = '1';
	c_value[1] = '\0';
	value = c_value;
}

WordCountPartialAgg::~WordCountPartialAgg()
{
	free(key);
}

void WordCountPartialAgg::add(void* v)
{
	char* c_value = (char*)value;
	int val = atoi((char*)v);
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

void WordCountPartialAgg::serialize(FILE* f, void* buf, size_t buf_size)
{
	char* c_value = (char*)value;
	char* wr_buf = (char*)buf;
	strcpy(wr_buf, key);
	strcat(wr_buf, " ");
	strcat(wr_buf, c_value);
	strcat(wr_buf, "\n");
	assert(NULL != f);
	size_t l = strlen(wr_buf);
	assert(fwrite(wr_buf, sizeof(char), l, f) == l);
}

bool WordCountPartialAgg::deserialize(FILE* f, void *buf, size_t buf_size)
{
	char *spl;
	char *read_buf = (char*)buf;
	if (feof(f)) {
		return false;
	}
	if (fgets(read_buf, buf_size, f) == NULL)
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

bool WordCountPartialAgg::deserialize(void *buf)
{
	char* spl;
	char *read_buf = (char*)buf;
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

char* tokenize(void* buf)
{
	return NULL;
}

REGISTER_PAO(WordCountPartialAgg);
