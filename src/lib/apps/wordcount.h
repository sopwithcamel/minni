#include "Mapper.h"
#include "PartialAgg.h"
#include "Tokenizer.h"

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(char* wrd);
	~WordCountPartialAgg();
	static size_t create(Token* t, PartialAgg** p);
	void add(void* value);
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	void serialize(void* buf);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
  private:
	uint64_t count;
};
