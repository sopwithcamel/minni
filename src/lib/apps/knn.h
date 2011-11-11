#include "Mapper.h"
#include "PartialAgg.h"

#define KEYSIZE		10000
#define K		5

class kNNPAO : public PartialAgg {
  public:
	class Neighbor {
	  public:
		char* key;
		float distance;
		Neighbor()
		{
			key = NULL;
			distance = INT_MAX;
		}
		Neighbor(const char* k)
		{
			key = (char*)malloc(KEYSIZE);
			strcpy(key, k);
			distance = INT_MAX;
		}
		Neighbor(const char* k, float d)
		{
			key = (char*)malloc(KEYSIZE);
			strcpy(key, k);
			distance = d;
		}
		~Neighbor()
		{
			free(key);
		}
		static bool comp(Neighbor n1, Neighbor n2)
		{
			return (n1.distance < n2.distance);
		}
	};
	kNNPAO(const char** tokens);
	~kNNPAO();
	/* add new neighbor if it is k-nearest */ 
	void add(void* neighbor_key);
	/* merge two lists */
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
	bool tokenize(void*, void*, char**);
  private:
	/* calculate distance of argument key from object key */
	float calculate_distance(const char* key);
	/* insert key if it is among the k-nearest */
	void check_insert(const char* key, float distance);
};
