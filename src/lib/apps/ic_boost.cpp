#include "ic_boost.h"
#include <boost/serialization/binary_object.hpp> 

using namespace std;

ICBoostPAO::ICBoostPAO(char* img, const char* ph)
{
    if (img) {
        key = (char*)malloc(strlen(ph)+1);
        strcpy(key, ph);
        char* n = (char*)malloc(strlen(ph)+1);
        strcpy(n, ph);
        neigh.push_back(n);
    } else {
        key = NULL;
    }    
}

ICBoostPAO::~ICBoostPAO()
{
    if (key) {
        free(key);
        key = NULL;
        for (uint32_t i=0; i<neigh.size(); i++) {
            free(neigh[i]);
            neigh[i] = NULL;
        }
    }
}

const char* ICBoostOperations::getKey(PartialAgg* p) const
{
    return ((ICBoostPAO*)p)->key;
}

bool ICBoostOperations::setKey(PartialAgg* p, char* k) const
{
	ICBoostPAO* wp = (ICBoostPAO*)p;
    // free the current key and set new key
    free(wp->key);
    wp->key = k;
}

bool ICBoostOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    return (!strcmp(((ICBoostPAO*)p1)->key, ((ICBoostPAO*)p2)->key));
}

size_t ICBoostOperations::createPAO(Token* t, PartialAgg** p) const
{
	ICBoostPAO* new_pao;
    string ph((char*)(t->tokens[1]));
    int l = ph.size();
    int num_rotations = 4;
    int prefix_len = (int)(l * 0.5);
    int step = l / num_rotations;
	if (t == NULL)
		new_pao = new ICBoostPAO(NULL, NULL);
	else {
        for (int i=0; i<num_rotations; i++) {
            rotate(ph.begin(), ph.begin()+step, ph.end());
            new_pao = new ICBoostPAO((char*)(t->tokens[0]),
                    ph.substr(0, prefix_len).c_str());
            p[i] = new_pao;	
        }
    }
	return num_rotations;
}

bool ICBoostOperations::destroyPAO(PartialAgg* p) const
{
    ICBoostPAO* wp = (ICBoostPAO*)p;
	delete wp;
}

inline uint32_t ICBoostOperations::getSerializedSize(PartialAgg* p) const
{
    // update for Boost!
    return 0;
}

bool ICBoostOperations::merge(PartialAgg* v, PartialAgg* mg) const
{
    vector<char*> tmp;
    ICBoostPAO* iv = (ICBoostPAO*)v;
    ICBoostPAO* miv = (ICBoostPAO*)mg;
    tmp.reserve(iv->neigh.size() + miv->neigh.size());
    std::merge(iv->neigh.begin(), iv->neigh.end(), miv->neigh.begin(),
            miv->neigh.end(), std::back_inserter(tmp));
    iv->neigh.swap(tmp);
}

bool ICBoostOperations::serialize(PartialAgg* p,
        boost::archive::binary_oarchive* output) const
{
    ICBoostPAO* wp = (ICBoostPAO*)p;
    string temp(wp->key);
    (*output) << temp;
    int l = wp->neigh.size();
    (*output) << l;
    for (uint32_t i=0; i<l; i++) {
        temp = string(wp->neigh[i]);
        (*output) << temp;
    }
}

bool ICBoostOperations::deserialize(PartialAgg* p,
        boost::archive::binary_iarchive* input) const
{
    ICBoostPAO* wp = (ICBoostPAO*)p;
    try {
        string temp;
        (*input) >> temp;
        wp->key = (char*)malloc(temp.size() + 1);
        strcpy(wp->key, temp.c_str());
        uint32_t num_neigh;
        (*input) >> num_neigh;
        for (uint32_t i=0; i<num_neigh; i++) {
            (*input) >> temp;
            char* n = (char*)malloc(temp.size() + 1);
            strcpy(n, temp.c_str());
            wp->neigh.push_back(n);
        }
        return true;
    } catch (...) {
        return false;
    }
}

inline bool ICBoostOperations::serialize(PartialAgg* p,
        std::string* output) const
{
}

inline bool ICBoostOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
}

inline bool ICBoostOperations::deserialize(PartialAgg* p,
        const std::string& input) const
{
}

inline bool ICBoostOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
}

REGISTER(ICBoostOperations);
