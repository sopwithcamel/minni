#ifndef PartialAgg_H
#define PartialAgg_H
#include <string>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "uthash.h"
#include "Tokenizer.h"

#define REGISTER_PAO(x) extern "C"\
	PartialAgg* __libminni_pao_create(Token*\
			 tok)\
	{\
		return new x(tok);\
	}\
	extern "C" void __libminni_pao_destroy(x* pao)\
	{\
		delete pao;\
	}


class PartialAgg {
  public:
	PartialAgg() {}
	~PartialAgg() {}
	virtual void add(void* val) = 0;
	virtual void merge(PartialAgg* add_agg) = 0;
	/* Serialize into file; use buf if buffer is required */
	virtual void serialize(FILE *f, void* buf, size_t buf_size) = 0;
	/* Serialize into buffer */
	virtual void serialize(void* buf) = 0;
	/* Deserialize from file; use buf if buffer is required */
	virtual bool deserialize(FILE* f, void* buf, size_t buf_size) = 0;
	/* Deserialize from buffer */
	virtual bool deserialize(void* buf) = 0;
	virtual bool tokenize(void* inp, void* progress, void* tot, 
			char** result) = 0;

	char* key;
	void* value;
	UT_hash_handle hh;
};

#endif
