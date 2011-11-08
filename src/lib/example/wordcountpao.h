#include "Mapper.h"
#include "PartialAgg.h"

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(const char** tokens);
	~WordCountPartialAgg();
	void add(void* value);
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
	char** tokenize(void*, void*);
};

