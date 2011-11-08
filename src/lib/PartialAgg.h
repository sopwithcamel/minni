#ifndef PartialAgg_H
#define PartialAgg_H
#include <string>
#include <sstream>
#include <map>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "uthash.h"
#define REGISTER_PAO(x) extern "C" PartialAgg* __libminni_pao_create(const char** val) {return new x(val);} \
	                extern "C" void __libminni_pao_destroy(x* pao) {delete pao;}


using namespace std;

class PartialAgg {
  public:
	PartialAgg() {}
	~PartialAgg() {}
	virtual void add(void* val);
	virtual void merge(PartialAgg* add_agg);
	virtual void serialize(FILE *f, void* buf);
	/* Deserialize from file; use buf if buffer is required */
	virtual bool deserialize(FILE* f, void* buf);
	/* Deserialize from buffer */
	virtual bool deserialize(void* buf);

	char* key;
	void* value;
	UT_hash_handle hh;
};

#endif


