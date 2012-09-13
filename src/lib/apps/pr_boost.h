#include "Mapper.h"
#include "BoostPartialAgg.h"
#include "Tokenizer.h"

class PageRankBoostOperations : public BoostOperations {
  public:
    const char* getKey(PartialAgg* p) const;
    bool setKey(PartialAgg* p, char* k) const;
    bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
	size_t createPAO(Token* t, PartialAgg** p) const;
    size_t dividePAO(const PartialAgg& p,
            PartialAgg** p_list) const;
    bool destroyPAO(PartialAgg* p) const;
	bool merge(PartialAgg* p, PartialAgg* mg) const;
    inline uint32_t getSerializedSize(PartialAgg* p) const;
	inline bool serialize(PartialAgg* p,
            boost::archive::binary_oarchive* output) const;
	inline bool deserialize(PartialAgg* p,
            boost::archive::binary_iarchive* input) const;
    inline bool serialize(PartialAgg* p,
            std::string* output) const;
    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const;
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const;
    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const;
  private:
};

class PageRankBoostPAO : public PartialAgg
{
    friend class PageRankBoostOperations;
  public:
	PageRankBoostPAO(char* k, float pr);
	~PageRankBoostPAO();
  private:
    char* key;
    float rank;
    uint32_t num_links;
    char* links;
};
