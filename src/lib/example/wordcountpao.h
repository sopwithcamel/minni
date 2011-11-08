#include "Mapper.h"
#include "PartialAgg.h"

#define BUF_SIZE	65535

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(const char** tokens);
	~WordCountPartialAgg();
	void add(void* value);
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf);
	bool deserialize(FILE* f, void* buf);
	bool deserialize(void* buf);
};

