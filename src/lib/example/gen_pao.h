#include "Minnie.h"

class TestPaoClass : public PartialAgg {
  public:
	TestPaoClass (string value) : PartialAgg(value) {};
	void add(string value);
	void merge(TestPaoClass* add_agg);
 private:
	int i;
};

