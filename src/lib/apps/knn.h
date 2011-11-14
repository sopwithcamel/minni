#include "Mapper.h"
#include "PartialAgg.h"
#include "Neighbor.h"

#define KEYSIZE		10000
#define K		5

class kNNPAO : public PartialAgg {
  public:
	kNNPAO(const char** tokens);
	~kNNPAO();
	/* add new neighbor if it is k-nearest */ 
	void add(void* neighbor_key);
	/* merge two lists */
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
	bool tokenize(void*, void*, void*, char**);
  private:
	/* calculate distance of argument key from object key */
	float calculate_distance(const char* key);
	/* insert key if it is among the k-nearest */
	void check_insert(const char* key, float distance);
};
