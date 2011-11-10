#include "MemCache.h"

MemCache::MemCache(const char* query_file, const char* query_type)
{
	queryFile = (char*)malloc(FILENAME_LENGTH);
	strcpy(queryFile, query_file);
	if (!strcmp(query_type, "image")) {
		queryType = IMAGE;
		loadImageCache();
	} else if (!strcmp(query_type, "word")) {
		queryType = WORD;
		loadWordCache();
	}
}

MemCache::~MemCache()
{
	for (int i=0; i<queryList.size(); i++)
		free(queryList[i]);
}

size_t MemCache::size()
{
	return queryList.size();	
}

char* MemCache::operator[](size_t ind)
{	
	if (ind >= queryList.size())
		return NULL;
	return queryList[ind];
}

void MemCache::loadWordCache()
{
	FILE* f = fopen(queryFile, "r");
	assert(f != NULL);
	fprintf(stderr, "File name: %s\n", queryFile);
	char* buf = (char*)malloc(FILENAME_LENGTH);
	while(!feof(f)) {
		if (fgets(buf, FILENAME_LENGTH, f) == NULL)
			break;
		int len = strlen(buf);
		buf[len-1] = buf[len]; // remove trailing \n
		queryList.push_back(buf);
	}
	fclose(f);
}

void MemCache::loadImageCache()
{
}
