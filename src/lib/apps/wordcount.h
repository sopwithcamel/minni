#include "Mapper.h"
#include "BoostPartialAgg.h"
#include "Tokenizer.h"

class WordCountOperations : public BoostOperations {
  public:
	size_t createPAO(Token* t, PartialAgg** p) const;
    bool destroyPAO(PartialAgg* p) const;
	void merge(Value* v, Value* mg) const;
    inline uint32_t getSerializedSize(PartialAgg* p) const;
	inline void serialize(PartialAgg* p,
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
};

class WordCountValue : public Value {
  public:
    uint32_t count;
};

class WordCountPartialAgg : public PartialAgg
{
  public:
	WordCountPartialAgg(char* wrd);
	~WordCountPartialAgg();
};
