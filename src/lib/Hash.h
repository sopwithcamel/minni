#ifndef LIB_HASH_H
#define LIB_HASH_H

#include "Hasher.h"
#include "hashutil.h"

typedef struct 
{
	size_t operator()(const char* str) const
	{ 
		return MurmurHash(str, strlen(str), 42);
	}
} MurmurHasher;

struct CStringCompare
{
	bool operator()(const char* s1, const char* s2) const
	{
		return strcmp(s1, s2) == 0;	
	}
};

typedef Hasher<char*, MurmurHasher, CStringCompare> Hashtable;
#endif // LIB_HASH_H

