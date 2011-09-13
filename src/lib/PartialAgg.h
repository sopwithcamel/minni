#ifndef PartialAgg_H
#define PartialAgg_H
#include <string>
#include <sstream>
#include <map>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "uthash.h"
#define REGISTER_PAO(x) extern "C" PartialAgg* __libminni_pao_create(char* val) {return new x(val);} \
	                extern "C" void __libminni_pao_destroy(x* pao) {delete pao;}


using namespace std;

class PartialAgg {
  public:
	PartialAgg() {}
	~PartialAgg() {}
	virtual void add (const char* val);
	virtual void merge (PartialAgg* add_agg);
	char* get_value();
	void set_val(char* v);

	char* key;
	char* value;
	UT_hash_handle hh;
};

#endif


