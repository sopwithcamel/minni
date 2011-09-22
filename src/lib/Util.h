#ifndef LIB_UTIL_H
#define LIB_UTIL_H

extern void* call_realloc(PartialAgg*** list, size_t siz);

class FilterInfo {
public:
	FilterInfo(): flush_hash(false) {}
	void* result;
	void* result1;
	void* result2;
	uint64_t result3;
	uint64_t result4;
	uint64_t length;
	bool flush_hash;
};

#endif
