#include "wctrig_boost.h"
#include <boost/serialization/binary_object.hpp> 

WCTrigBoostPAO::WCTrigBoostPAO(char* one, char* two, char* three)
{
    if (one) {
        key = (char*)malloc(strlen(one)+strlen(two)+strlen(three)+3);
        strcpy(key, one);
        strcat(key, "-");
        strcat(key, two);
        strcat(key, "-");
        strcat(key, three);
        count = 1;
    } else {
        key = NULL;
        count = 0;
    }
}

WCTrigBoostPAO::~WCTrigBoostPAO()
{
    if (key) {
        free(key);
        key = NULL;
    }
}

const char* WCBoostOperations::getKey(PartialAgg* p) const
{
    return ((WCTrigBoostPAO*)p)->key;
}

bool WCBoostOperations::setKey(PartialAgg* p, char* k) const
{
	WCTrigBoostPAO* wp = (WCTrigBoostPAO*)p;
    // free the current key and set new key
    free(wp->key);
    wp->key = k;
}

bool WCBoostOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    return (!strcmp(((WCTrigBoostPAO*)p1)->key, ((WCTrigBoostPAO*)p2)->key));
}

size_t WCBoostOperations::createPAO(Token* t, PartialAgg** p) const
{
	WCTrigBoostPAO* new_pao;
	if (t == NULL) {
		new_pao = new WCTrigBoostPAO(NULL, NULL, NULL);
	} else {
		new_pao = new WCTrigBoostPAO((char*)t->tokens[0],
                (char*)t->tokens[1], (char*)t->tokens[2]);
    }
	p[0] = new_pao;	
	return 1;
}

bool WCBoostOperations::destroyPAO(PartialAgg* p) const
{
    WCTrigBoostPAO* wp = (WCTrigBoostPAO*)p;
	delete wp;
}

inline uint32_t WCBoostOperations::getSerializedSize(PartialAgg* p) const
{
    // update for Boost!
    return 0;
}

bool WCBoostOperations::merge(PartialAgg* v, PartialAgg* mg) const
{
	((WCTrigBoostPAO*)v)->count += ((WCTrigBoostPAO*)mg)->count;
}

bool WCBoostOperations::serialize(PartialAgg* p,
        boost::archive::binary_oarchive* output) const
{
    WCTrigBoostPAO* wp = (WCTrigBoostPAO*)p;
    uint32_t len = strlen(wp->key) + 1;
    (*output) << len;
    (*output) << boost::serialization::make_binary_object(wp->key, len);
    (*output) << wp->count;
}

bool WCBoostOperations::deserialize(PartialAgg* p,
        boost::archive::binary_iarchive* input) const
{
    WCTrigBoostPAO* wp = (WCTrigBoostPAO*)p;
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
