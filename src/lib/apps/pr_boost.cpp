#include "pr_boost.h"
#include <boost/serialization/binary_object.hpp> 

#define KEY_SIZE        10

PageRankBoostPAO::PageRankBoostPAO(char* k, float pr)
{
    if (k) {
        key = (char*)malloc(strlen(k)+1);
        strcpy(key, k);
        rank = pr;
    } else {
        key = NULL;
        rank = 0;
    }
    num_links = 0;
    links = NULL;
}

PageRankBoostPAO::~PageRankBoostPAO()
{
    if (key) {
        free(key);
        key = NULL;
    }
    if (links) {
        free(links);
        links = NULL;
    }
}

const char* PageRankBoostOperations::getKey(PartialAgg* p) const
{
    return ((PageRankBoostPAO*)p)->key;
}

bool PageRankBoostOperations::setKey(PartialAgg* p, char* k) const
{
	PageRankBoostPAO* wp = (PageRankBoostPAO*)p;
    // free the current key and set new key
    free(wp->key);
    wp->key = k;
}

bool PageRankBoostOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    return (!strcmp(((PageRankBoostPAO*)p1)->key, ((PageRankBoostPAO*)p2)->key));
}

size_t PageRankBoostOperations::createPAO(Token* t, PartialAgg** p) const
{
	PageRankBoostPAO* new_pao;
    // t has to be NULL for pagerank
    new_pao = new PageRankBoostPAO(NULL, 0);
	p[0] = new_pao;	
	return 1;
}

size_t PageRankBoostOperations::dividePAO(const PartialAgg& p,
        PartialAgg** p_list) const
{
    PageRankBoostPAO* new_pao;
	const PageRankBoostPAO* wp = static_cast<const PageRankBoostPAO*>(&p);

    // copy the input PAO as the first output PAO
    new_pao = new PageRankBoostPAO(wp->key, 0);
    new_pao->num_links = wp->num_links;
    if (wp->num_links > 0) {
        new_pao->links = (char*)malloc(strlen(wp->links) + 1);
        strcpy(new_pao->links, wp->links);
    }
    p_list[0] = new_pao;

    if (wp->num_links > 0) {
        float pr_given = wp->rank / wp->num_links;

        string temp(wp->links);
        stringstream ss(temp);
        string l;
        for (int i=1; i<=wp->num_links; i++) {
            getline(ss, l, ' ');
            new_pao = new PageRankBoostPAO((char*)l.c_str(), pr_given);
            p_list[i] = new_pao;
        }
    }
    return wp->num_links+1;
}

bool PageRankBoostOperations::destroyPAO(PartialAgg* p) const
{
    PageRankBoostPAO* wp = (PageRankBoostPAO*)p;
	delete wp;
}

inline uint32_t PageRankBoostOperations::getSerializedSize(PartialAgg* p) const
{
    // update for Boost!
    return 0;
}

bool PageRankBoostOperations::merge(PartialAgg* v, PartialAgg* mg) const
{
	PageRankBoostPAO* wv = (PageRankBoostPAO*)v;
    PageRankBoostPAO* wmv = (PageRankBoostPAO*)mg;
    wv->rank += wmv->rank;
    if (wv->num_links == 0 && wmv->num_links > 0) {
        wv->num_links = wmv->num_links;
        wv->links = (char*)malloc(strlen(wmv->links) + 1);
        strcpy(wv->links, wmv->links);
    }
}

bool PageRankBoostOperations::serialize(PartialAgg* p,
        boost::archive::binary_oarchive* output) const
{
    PageRankBoostPAO* wp = (PageRankBoostPAO*)p;
    uint32_t len = strlen(wp->key);
    (*output) << len;
    string temp(wp->key);
    (*output) << temp;
    (*output) << wp->rank;
    (*output) << wp->num_links;
    if (wp->num_links > 0) {
        len = strlen(wp->links) + 1;
        (*output) << len;
        temp = wp->links;
        (*output) << temp;
    }
}

bool PageRankBoostOperations::deserialize(PartialAgg* p,
        boost::archive::binary_iarchive* input) const
{
    PageRankBoostPAO* wp = (PageRankBoostPAO*)p;
    try {
        uint32_t len;
        (*input) >> len;
        wp->key = (char*)malloc(len + 1);
        string key_temp;
        (*input) >> key_temp;
        strcpy(wp->key, key_temp.c_str());
        (*input) >> wp->rank;
        (*input) >> wp->num_links;
        if (wp->num_links > 0) {
            (*input) >> len;
            wp->links = (char*)malloc(len + 1);
            string links_temp;
            (*input) >> links_temp;
            strcpy(wp->links, links_temp.c_str());
        }
        return true;
    } catch (...) {
        return false;
    }
}

inline bool PageRankBoostOperations::serialize(PartialAgg* p,
        std::string* output) const
{
}

inline bool PageRankBoostOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
}

inline bool PageRankBoostOperations::deserialize(PartialAgg* p,
        const std::string& input) const
{
}

inline bool PageRankBoostOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
}

REGISTER(PageRankBoostOperations);
