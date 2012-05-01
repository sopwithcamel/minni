#include "Mapper.h"
#include "BoostPartialAgg.h"
#include "Tokenizer.h"

class DigPlain : public BoostPartialAgg {
  public:
    DigPlain(char* wrd, char* next, char* last);
    ~DigPlain();
    inline const std::string& key() const;
    inline const uint64_t count() const;
    static size_t create(Token* t, PartialAgg** p);
    void merge(PartialAgg* add_agg);
	inline void serialize(boost::archive::binary_oarchive* output) const;
	inline bool deserialize(boost::archive::binary_iarchive* input);
  private:
    std::string key_;
    uint64_t count_;
};
