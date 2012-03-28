#include "Mapper.h"
#include "HandSerializedPartialAgg.h"
#include "Tokenizer.h"

class WordCountPlain : public HandSerializedPartialAgg {
  public:
    WordCountPlain(char* wrd);
    ~WordCountPlain();
    inline const std::string& key() const;
    inline const uint64_t count() const;
    static size_t create(Token* t, PartialAgg** p);
    void merge(PartialAgg* add_agg);
	inline void serialize(std::ofstream* output) const;
	inline bool deserialize(std::ifstream* input);
  private:
    std::string key_;
    uint64_t count_;
};
