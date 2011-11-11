#include <stdlib.h>
#include "PartialAgg.h"

#define KEYSIZE		10000

class ImagePAO : public PartialAgg {
  public:
	ImagePAO(const char** tokens);
	~ImagePAO();
	/* add new neighbor if it is k-nearest */ 
	void add(void* neighbor_key);
	/* merge two lists */
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
	bool tokenize(void*, void*, char**);
};
