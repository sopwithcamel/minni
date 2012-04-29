#include "digplain.h"

#define KEY_SIZE        10

DigPlain::DigPlain(char* wrd, char* next)
{
    if (wrd) {
        key_.assign(wrd);
        key_.append("-");
        key_.append(next);
        count_ = 1;
    } else
        count_ = 0;
}

DigPlain::~DigPlain()
{
}

const std::string& DigPlain::key() const
{
    return key_;
}

const uint64_t DigPlain::count() const
{
    return count_;
}

size_t DigPlain::create(Token* t, PartialAgg** p)
{
    DigPlain* new_pao;
    if (t == NULL)
        new_pao = new DigPlain(NULL, NULL);
    else    
        new_pao = new DigPlain((char*)(t->tokens[0]), (char*)(t->tokens[1]));
    p[0] = new_pao; 
    return 1;
}

void DigPlain::merge(PartialAgg* add_agg)
{
    DigPlain* wp = (DigPlain*)add_agg;
    count_ += wp->count_;
}

void DigPlain::serialize(boost::archive::binary_oarchive* output) const
{
    (*output) << key_;
    (*output) << count_;
}

bool DigPlain::deserialize(boost::archive::binary_iarchive* input)
{
    try {
        (*input) >> key_;
        (*input) >> count_;
        return true;
    } catch (...) {
        return false;
    }
}

REGISTER_PAO(DigPlain);
