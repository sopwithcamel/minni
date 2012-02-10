#include "Mapper.h"
#include "PartialAgg.h"
#include "Tokenizer.h"
#include "wordcountpao.pb.h"

class WordCountPartialAgg : public PartialAgg {
  public:
	WordCountPartialAgg(char* wrd);
	~WordCountPartialAgg();
    inline const std::string& key() const;
    inline const uint64_t count() const;
	static size_t create(Token* t, PartialAgg** p);
//	void add(void* value);
	void merge(PartialAgg* add_agg);
	inline void serialize(std::ostream* output) const;
	inline void serialize(std::string* output) const;
	inline bool deserialize(std::istream* input);
	inline bool deserialize(const std::string& input);
  private:
    wordcount::pao pb;
};


inline const std::string& WordCountPartialAgg::key() const
{
    return pb.key();
}

inline const uint64_t WordCountPartialAgg::count() const
{
    return pb.count();
}
