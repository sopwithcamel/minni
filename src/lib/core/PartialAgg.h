#ifndef PartialAgg_H
#define PartialAgg_H
#include <string>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "uthash.h"
#include "Tokenizer.h"

//#define UTHASH

#define REGISTER_PAO(x) extern "C"\
	size_t __libminni_pao_create(Token* tok, PartialAgg** p_list)\
	{\
		return x::create(tok, p_list);\
	}\
	extern "C" void __libminni_pao_destroy(x* pao)\
	{\
		delete pao;\
	}

class Token;
class PartialAgg {
  public:
    static uint64_t createCtr;
    static uint64_t destCtr;
  public:
	PartialAgg() {}
	virtual ~PartialAgg() {}
    /* Get key */
    virtual const std::string& key() const = 0;
//    virtual void add(void* val) = 0;
	virtual void merge(PartialAgg* add_agg) = 0;
	/* Serialize into file */
	virtual void serialize(std::ostream* output) const = 0;
	/* Serialize into string */
	virtual void serialize(std::string* output) const = 0;
	/* Deserialize from file */
	virtual bool deserialize(std::istream* input) = 0;
	/* Deserialize from buffer */
	virtual bool deserialize(const std::string& input) = 0;
	static size_t create(Token*& t, PartialAgg** p_list) { return 0; }
#ifdef UTHASH
	UT_hash_handle hh;
#endif
};

#endif
