#ifndef LIB_UTIL_H
#define LIB_UTIL_H

extern void* call_realloc(PartialAgg*** list, size_t siz);

class FilterInfo {
public:
	FilterInfo(): flush_hash(false) {}
	void* result;
	uint64_t length;
	bool flush_hash;
};

#endif
