#include "Mapper.h"
#include "PartialAgg.h"

#define BUF_SIZE	65535

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(const char* value);
	~WordCountPartialAgg();
	void add(const char* value);
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f);
	bool deserialize(FILE* f, void* buf);
};

