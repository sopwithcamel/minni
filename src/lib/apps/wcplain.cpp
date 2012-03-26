#include "wcplain.h"

#define KEY_SIZE        10

WordCountPlain::WordCountPlain(char* wrd)
{
    if (wrd) {
        key_.assign(wrd);
        count_ = 1;
    } else
        count_ = 0;
}

WordCountPlain::~WordCountPlain()
{
}

const std::string& WordCountPlain::key() const
{
    return key_;
}

const uint64_t WordCountPlain::count() const
{
    return count_;
}

size_t WordCountPlain::create(Token* t, PartialAgg** p)
{
    WordCountPlain* new_pao;
    if (t == NULL)
        new_pao = new WordCountPlain(NULL);
    else    
        new_pao = new WordCountPlain((char*)t->tokens[0]);
    p[0] = new_pao; 
    return 1;
}

void WordCountPlain::merge(PartialAgg* add_agg)
{
    WordCountPlain* wp = (WordCountPlain*)add_agg;
    count_ += wp->count_;
}

void WordCountPlain::serialize(std::ofstream* output) const
{
    (*output) << (key_ + " ");
    (*output) << count_;
    (*output) << endl;
}

bool WordCountPlain::deserialize(std::ifstream* input)
{
    (*input) >> key_;
    (*input) >> count_;
}

REGISTER_PAO(WordCountPlain);
