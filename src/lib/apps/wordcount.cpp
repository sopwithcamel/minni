#include "wordcount.h"

#define KEY_SIZE        10

WordCountPartialAgg::WordCountPartialAgg(char* wrd)
{
	if (wrd) {
		key.assign(wrd);
        count = 1;
	} else
        count = 0;
}

WordCountPartialAgg::~WordCountPartialAgg()
{
    PartialAgg::destCtr++;
}

size_t WordCountPartialAgg::create(Token* t, PartialAgg** p)
{
	WordCountPartialAgg* new_pao;
	if (t == NULL)
		new_pao = new WordCountPartialAgg(NULL);
	else	
		new_pao = new WordCountPartialAgg((char*)t->tokens[0]);
	p[0] = new_pao;	
    PartialAgg::createCtr++;
	return 1;
}

void WordCountPartialAgg::add(void* v)
{
	int val = atoi((char*)v);
	count += val;
}

void WordCountPartialAgg::merge(PartialAgg* add_agg)
{
	WordCountPartialAgg* wp = (WordCountPartialAgg*)add_agg;
	count += wp->count;
}

void WordCountPartialAgg::serialize(FILE* f, void* buf, size_t buf_size)
{
	serialize(buf);
	char* wr_buf = (char*)buf;
	assert(NULL != f);
	size_t l = strlen(wr_buf);
	size_t w = fwrite(wr_buf, sizeof(char), l, f);
	if (w != l) {
        fprintf(stderr, "buf size: %d, written: %d\n", l, w);
    }
}

void WordCountPartialAgg::serialize(void* buf)
{
	char* wr_buf = (char*)buf;
	strcpy(wr_buf, key.c_str());
	strcat(wr_buf, " ");
	sprintf(wr_buf + strlen(wr_buf), "%lu", count);
	strcat(wr_buf, "\n");
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
	return deserialize(read_buf);
}

bool WordCountPartialAgg::deserialize(void *buf)
{
	char* spl;
	char *read_buf = (char*)buf;
	spl = strtok(read_buf, " \n\r");
	if (spl == NULL)
		return false;
	key.assign(spl);

	spl = strtok(NULL, " \n\r");
	if (spl == NULL)
		return false;
	count = atoi(spl);
	return true;
}

REGISTER_PAO(WordCountPartialAgg);
