#include <stdlib.h>
#include "PartialAgg.h"
#include <assert.h>
#include "Tokenizer.h"

class PageRankPAO : public PartialAgg {
  public:
	PageRankPAO(char* name, double pr);
	~PageRankPAO();
	static size_t create(Token* t, PartialAgg** p);
	void add(void* neighbor_key);
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	void serialize(void* buf);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
  private:
	double pagerank;
};
