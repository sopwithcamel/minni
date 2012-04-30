#include "Mapper.h"
#include "BoostPartialAgg.h"
#include <boost/serialization/vector.hpp>
#include "Tokenizer.h"
#include <string.h>
#include <wn.h>

class SemanticPlain : public BoostPartialAgg {
  public:
	SemanticPlain(char* wrd, char* book);
	~SemanticPlain();
    inline const std::string& key() const;
    inline const uint64_t count() const;
	static size_t create(Token* t, PartialAgg** p);
	void merge(PartialAgg* add_agg);
	inline void serialize(boost::archive::binary_oarchive* output) const;
	inline bool deserialize(boost::archive::binary_iarchive* input);
    static bool compstr(std::string& n1, std::string& n2)
    {
        return n1.compare(n2);
    }
    static bool eqstr(std::string& n1, std::string& n2)
    {
        return !n1.compare(n2);
    }

  private:
    std::string key_;
    std::vector<std::string> books;
};


inline const std::string& SemanticPlain::key() const
{
    return key_;
}
