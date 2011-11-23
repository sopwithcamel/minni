#include "Mapper.h"
#include "PartialAgg.h"
#include "Tokenizer.h"

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(Token* token);
	~WordCountPartialAgg();
	void add(void* value);
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	void serialize(void* buf);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
};

