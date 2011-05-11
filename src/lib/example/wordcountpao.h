#include "Mapper.h"
#include "PartialAgg.h"

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(const char* value);
	~WordCountPartialAgg();
	void add(const char* value);
	void merge(PartialAgg* add_agg);
};

