#include "wc_boost.h"

#define KEY_SIZE        10

WCBoostPAO::WCBoostPAO(char* wrd)
{
}

WCBoostPAO::~WCBoostPAO()
{
}

const char* WCBoostOperations::getKey(PartialAgg* p) const
{
    return ((WCBoostPAO*)p)->key;
}

bool WCBoostOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    return (!strcmp(((WCBoostPAO*)p1)->key, ((WCBoostPAO*)p2)->key));
}

size_t WCBoostOperations::createPAO(Token* t, PartialAgg** p) const
{
	WCBoostPAO* new_pao;
	if (t == NULL)
		new_pao = new WCBoostPAO(NULL);
	else	
		new_pao = new WCBoostPAO((char*)t->tokens[0]);
	p[0] = new_pao;	
	return 1;
}

bool WCBoostOperations::destroyPAO(PartialAgg* p) const
{
	return true;
}

inline uint32_t WCBoostOperations::getSerializedSize(PartialAgg* p) const
{
    // update for Boost!
    return 0;
}

bool WCBoostOperations::merge(PartialAgg* v, PartialAgg* mg) const
{
	((WCBoostPAO*)v)->count += ((WCBoostPAO*)mg)->count;
}

bool WCBoostOperations::serialize(PartialAgg* p,
        boost::archive::binary_oarchive* output) const
{
/*
    (*output) << p->key;
    WordCountValue* vptr = (WordCountValue*)(p->value);
    (*output) << vptr->count;
*/
}

bool WCBoostOperations::deserialize(PartialAgg* p,
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

inline bool WCBoostOperations::serialize(PartialAgg* p,
        std::string* output) const
{
}

inline bool WCBoostOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
}

inline bool WCBoostOperations::deserialize(PartialAgg* p,
        const std::string& input) const
{
}

inline bool WCBoostOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
}

REGISTER(WCBoostOperations);
