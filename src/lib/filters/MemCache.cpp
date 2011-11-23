#include "MemCache.h"

MemCache::MemCache(const char* query_file, QueryType typ):
		queryType(typ)
{
	queryFile = (char*)malloc(FILENAME_LENGTH);
	strcpy(queryFile, query_file);
	if (queryType == FIL) {
		loadFileCache();
	} else if (queryType == WORD) {
		loadWordCache();
	}
}

MemCache::~MemCache()
{
	for (int i=0; i<queryList.size(); i++) {
		free(queryList[i]);
		if (queryType == FIL)
			free(queryContents[i]);
	}
}

size_t MemCache::size()
{
	return queryList.size();	
}

char* MemCache::getItem(uint64_t ind)
{	
	if (ind >= queryList.size())
		return NULL;
	return queryList[ind];
}

size_t MemCache::getItemSize(uint64_t ind)
{	
	if (ind >= queryFileSizes.size())
		return NULL;
	if (queryType == WORD)
		return strlen(queryList[ind]);
	else
		return queryFileSizes[ind];
}

char* MemCache::getFileContents(uint64_t ind)
{	
	if (ind >= queryContents.size())
		return NULL;
	if (queryType == WORD)
		return NULL;
	return queryContents[ind];
}

void MemCache::loadWordCache()
{
	FILE* f = fopen(queryFile, "r");
	assert(f != NULL);
	char* buf = (char*)malloc(FILENAME_LENGTH);
	while(!feof(f)) {
		if (fgets(buf, FILENAME_LENGTH, f) == NULL)
			break;
		int len = strlen(buf);
		buf[len-1] = '\0'; // remove trailing \n
		if (strlen(buf) > 0)
			queryList.push_back(buf);
	}
	fclose(f);
}

void MemCache::loadFileCache()
{
	// First load file names into queryList
	loadWordCache();
	// then load contents
	readFileContents();
}

void MemCache::readFileContents()
{
	FILE *f;
	char* buf;
	uint64_t sz;
	for(int i=0; i<queryList.size(); i++) {
		f = fopen(queryList[i], "r");
		assert(f);
		fseek(f, 0, SEEK_END);
		sz = ftell(f);
		buf = (char*)malloc(sz + 1);		
		fseek(f, 0, SEEK_SET);
		assert(fread(buf, sizeof(char), sz, f) == sz);
		buf[sz] = '\0';
		queryContents.push_back(buf);
		queryFileSizes.push_back(sz);
		fclose(f);
	}
}
