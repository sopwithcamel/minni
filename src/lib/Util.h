#ifndef LIB_UTIL_H
#define LIB_UTIL_H

extern void* call_realloc(PartialAgg*** list, size_t siz);

typedef struct {
	void* result;
	uint64_t length;
} FilterInfo;

#endif
