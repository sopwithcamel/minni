#include "wordcount.h"

#define KEY_SIZE        10

WordCountPartialAgg::WordCountPartialAgg(char* wrd)
{
}

WordCountPartialAgg::~WordCountPartialAgg()
{
}

size_t WordCountOperations::createPAO(Token* t, PartialAgg** p) const
{
	WordCountPartialAgg* new_pao;
	if (t == NULL)
		new_pao = new WordCountPartialAgg(NULL);
	else	
		new_pao = new WordCountPartialAgg((char*)t->tokens[0]);
	p[0] = new_pao;	
	return 1;
}

bool WordCountOperations::destroyPAO(PartialAgg* p) const
{
	return true;
}

inline uint32_t WordCountOperations::getSerializedSize(PartialAgg* p) const
{
    // update for Boost!
    return 0;
}

void WordCountOperations::merge(Value* v, Value* mg) const
{
	((WordCountValue*)v)->count += ((WordCountValue*)mg)->count;
}

void WordCountOperations::serialize(PartialAgg* p,
        boost::archive::binary_oarchive* output) const
{
/*
    (*output) << p->key;
    WordCountValue* vptr = (WordCountValue*)(p->value);
    (*output) << vptr->count;
*/
}

bool WordCountOperations::deserialize(PartialAgg* p,
        boost::archive::binary_iarchive* input) const
{
/*
    try {
        (*input) >> p->key;
        WordCountValue* vptr = (WordCountValue*)(p->value);
        (*input) >> vptr->count;
        return true;
    } catch (...) {
        return false;
    }
*/
}

inline bool WordCountOperations::serialize(PartialAgg* p,
        std::string* output) const
{
}

inline bool WordCountOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
}

inline bool WordCountOperations::deserialize(PartialAgg* p,
        const std::string& input) const
{
}

inline bool WordCountOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
}

REGISTER(WordCountOperations);
