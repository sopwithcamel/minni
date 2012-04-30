#include "semantic_plain.h"

SemanticPlain::SemanticPlain(char* wrd, char* book)
{
	if (wrd) {
		key_.assign(wrd);
        books.push_back(book);
	}
}

SemanticPlain::~SemanticPlain()
{
}


size_t SemanticPlain::create(Token* t, PartialAgg** p)
{
	SemanticPlain* new_pao;
	if (t == NULL)
		new_pao = new SemanticPlain(NULL, NULL);
	else {
/*
        SynsetPtr sptr, pt;
        char* syn = "uncl.";
        wninit();
        sptr = findtheinfo_ds((char*)t->tokens[0], NOUN, HYPERPTR, ALLSENSES);
        if (sptr) {
            if (sptr->ptrcount > 0) {
                pt = read_synset(sptr->ppos[0], sptr->ptroff[0], "");
//                printf("Syn: %s\n", pt->words[0]);
                syn = pt->words[0];
            }
        }
		new_pao = new SemanticPlain(syn, (char*)t->tokens[1]);
        if (sptr) {
            free_syns(sptr);
            free_syns(pt);
        }
*/
		new_pao = new SemanticPlain((char*)(t->tokens[0]), (char*)(t->tokens[1]));
    }
	p[0] = new_pao;	
	return 1;
}

void SemanticPlain::merge(PartialAgg* add_agg)
{
	SemanticPlain* spao = (SemanticPlain*)add_agg;
    // Merge the lists
    size_t n = books.size();
    books.insert(books.end(), spao->books.begin(), spao->books.end());
    std::inplace_merge(books.begin(), books.begin() + n, books.end());

    // Remove duplicates
    uint32_t rem = unique(books.begin(), books.end()) - books.begin();

    // Truncate duplicates
    books.resize(rem);
}

inline void SemanticPlain::serialize(boost::archive::binary_oarchive*
        output) const
{
    (*output) << key_;
    (*output) << books;
}

bool SemanticPlain::deserialize(boost::archive::binary_iarchive* input)
{
    try {
        (*input) >> key_;
        (*input) >> books;
        return true;
    } catch (...) {
        return false;
    }
}

REGISTER_PAO(SemanticPlain);
