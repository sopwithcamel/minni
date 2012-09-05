#include "wc_boost.h"
#include <boost/serialization/binary_object.hpp> 

#define KEY_SIZE        10

WCBoostPAO::WCBoostPAO(char* wrd)
{
    if (wrd) {
        key = (char*)malloc(strlen(wrd)+1);
        strcpy(key, wrd);
        count = 1;
    } else {
        key = NULL;
        count = 0;
    }    
}

WCBoostPAO::~WCBoostPAO()
{
    if (key) {
        free(key);
        key = NULL;
    }
}

const char* WCBoostOperations::getKey(PartialAgg* p) const
{
    return ((WCBoostPAO*)p)->key;
}

bool WCBoostOperations::setKey(PartialAgg* p, char* k) const
{
	WCBoostPAO* wp = (WCBoostPAO*)p;
    // free the current key and set new key
    free(wp->key);
    wp->key = k;
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
    WCBoostPAO* wp = (WCBoostPAO*)p;
	delete wp;
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
    WCBoostPAO* wp = (WCBoostPAO*)p;
    uint32_t len = strlen(wp->key) + 1;
    (*output) << len;
    (*output) << boost::serialization::make_binary_object(wp->key, len);
    (*output) << wp->count;
}

bool WCBoostOperations::deserialize(PartialAgg* p,
        boost::archive::binary_iarchive* input) const
{
    WCBoostPAO* wp = (WCBoostPAO*)p;
    try {
        uint32_t len;
        (*input) >> len;
        wp->key = (char*)malloc(len + 1);
        (*input) >> boost::serialization::make_binary_object(wp->key, len);
        (*input) >> wp->count;
        return true;
    } catch (...) {
        return false;
    }
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
