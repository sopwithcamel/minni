#ifndef LIB_UTIL_H
#define LIB_UTIL_H

#include <assert.h>

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

template <class T>
class MultiBuffer {
  public:
	MultiBuffer(uint64_t num_buffers, uint64_t tok_per_buf):
			num_bufs(num_buffers)
	{
		bufs = (T**)malloc(sizeof(T*) * num_buffers);
		for (int i=0; i<num_buffers; i++) {
			bufs[i] = (T*)malloc(sizeof(T) * tok_per_buf);
		}
	}
	~MultiBuffer()
	{
		for (int i=0; i<num_bufs; i++) {
			free(bufs[i]);
		}
		free(bufs);
	}
	T* operator[] (const uint64_t ind)
	{
		assert(ind < num_bufs);
		return bufs[ind];
	}
	T** bufs;
  private:
	uint64_t num_bufs;
};

#endif
