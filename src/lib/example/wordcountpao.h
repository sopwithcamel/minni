#include "Mapper.h"
#include "PartialAgg.h"

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(string value) : PartialAgg(value) {};
	void add(string value);
	void merge(PartialAgg* add_agg);
};

