#include "Mapper.h"
#include "BoostPartialAgg.h"
#include "Tokenizer.h"

#include <vector>

class ICBoostOperations : public BoostOperations {
  public:
    const char* getKey(PartialAgg* p) const;
    bool setKey(PartialAgg* p, char* k) const;
    bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
	size_t createPAO(Token* t, PartialAgg** p) const;
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
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};

class ICBoostPAO : public PartialAgg
{
    friend class ICBoostOperations;
  public:
	ICBoostPAO(char* img, const char* ph);
	~ICBoostPAO();
  private:
    char* key;
    std::vector<char*> neigh;
};
