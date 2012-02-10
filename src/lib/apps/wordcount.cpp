#include "wordcount.h"

#define KEY_SIZE        10

WordCountPartialAgg::WordCountPartialAgg(char* wrd)
{
	if (wrd) {
		pb.set_key(wrd);
        pb.set_count(1);
	} else
        pb.set_count(0);
}

WordCountPartialAgg::~WordCountPartialAgg()
{
    PartialAgg::destCtr++;
}


size_t WordCountPartialAgg::create(Token* t, PartialAgg** p)
{
	WordCountPartialAgg* new_pao;
	if (t == NULL)
		new_pao = new WordCountPartialAgg(NULL);
	else	
		new_pao = new WordCountPartialAgg((char*)t->tokens[0]);
	p[0] = new_pao;	
    PartialAgg::createCtr++;
	return 1;
}

/*
void WordCountPartialAgg::add(void* v)
{
	int val = atoi((char*)v);
	count += val;
}
*/

void WordCountPartialAgg::merge(PartialAgg* add_agg)
{
	WordCountPartialAgg* wp = (WordCountPartialAgg*)add_agg;
	pb.set_count(count() + wp->count());
}

inline void WordCountPartialAgg::serialize(std::ostream* output) const
{
    pb.SerializeToOstream(output);
}

inline void WordCountPartialAgg::serialize(std::string* output) const
{
    pb.SerializeToString(output);
}

inline bool WordCountPartialAgg::deserialize(std::istream* input)
{
	return pb.ParseFromIstream(input);
}

inline bool WordCountPartialAgg::deserialize(const std::string& input)
{
	return pb.ParseFromString(input);
}

REGISTER_PAO(WordCountPartialAgg);
