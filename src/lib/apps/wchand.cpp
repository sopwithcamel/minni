#include "wchand.h"

#define KEY_SIZE        10

WordCountHand::WordCountHand(char* wrd)
{
    if (wrd) {
        key_.assign(wrd);
        count_ = 1;
    } else
        count_ = 0;
}

WordCountHand::~WordCountHand()
{
}

const std::string& WordCountHand::key() const
{
    return key_;
}

const uint64_t WordCountHand::count() const
{
    return count_;
}

size_t WordCountHand::create(Token* t, PartialAgg** p)
{
    WordCountHand* new_pao;
    if (t == NULL)
        new_pao = new WordCountHand(NULL);
    else    
        new_pao = new WordCountHand((char*)t->tokens[0]);
    p[0] = new_pao; 
    return 1;
}

void WordCountHand::merge(PartialAgg* add_agg)
{
    WordCountHand* wp = (WordCountHand*)add_agg;
    count_ += wp->count_;
}

void WordCountHand::serialize(std::ofstream* output) const
{
    (*output) << key_ << "\t";
    (*output) << count_ << "\n";
}

bool WordCountHand::deserialize(std::ifstream* input)
{
    if ((*input) >> key_) {
        (*input) >> count_;
        return true;
    } else {
        return false;
    }
}

REGISTER_PAO(WordCountHand);
