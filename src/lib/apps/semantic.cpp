#include "semantic.h"

using namespace google::protobuf::io;

SemanticPartialAgg::SemanticPartialAgg(char* wrd, char* book)
{
	if (wrd) {
		pb.set_key(wrd);
        pb.add_books(book);
	}
}

SemanticPartialAgg::~SemanticPartialAgg()
{
}


size_t SemanticPartialAgg::create(Token* t, PartialAgg** p)
{
	SemanticPartialAgg* new_pao;
	if (t == NULL)
		new_pao = new SemanticPartialAgg(NULL, NULL);
	else {
        SynsetPtr p, pt;
        char* syn = "uncl.";
        wninit();
        p = findtheinfo_ds((char*)t->tokens[0], NOUN, HYPERPTR, ALLSENSES);
        if (p) {
            if (p->ptrcount > 0) {
                pt = read_synset(p->ppos[0], p->ptroff[0], "");
//                printf("Syn: %s\n", pt->words[0]);
                syn = pt->words[0];
            }
        }
		new_pao = new SemanticPartialAgg(syn, (char*)t->tokens[1]);
    }
	p[0] = new_pao;	
	return 1;
}

void SemanticPartialAgg::merge(PartialAgg* add_agg)
{
	SemanticPartialAgg* spao = (SemanticPartialAgg*)add_agg;
    // Merge the lists
    pb.mutable_books()->MergeFrom(spao->pb.books());
    uint32_t siz = pb.books_size();

    // Sort the merged list
    sort(pb.mutable_books()->begin(), pb.mutable_books()->end());

    // Remove duplicates
    uint32_t rem = unique(pb.mutable_books()->begin(),
            pb.mutable_books()->end()) - pb.mutable_books()->begin();

    // Truncate duplicates
    for (int i=0; i<siz-rem; i++)
        pb.mutable_books()->RemoveLast();
}

inline void SemanticPartialAgg::serialize(
        CodedOutputStream* output) const
{
    output->WriteVarint32(pb.ByteSize());
    pb.SerializeToCodedStream(output);
}

inline uint32_t SemanticPartialAgg::serializedSize() const
{
    return pb.ByteSize();
}


inline void SemanticPartialAgg::serialize(std::string* output) const
{
    pb.SerializeToString(output);
}

inline void SemanticPartialAgg::serialize(char* output, size_t size)
{
    memset((void*)output, 0, size);
	pb.SerializeToArray(output, size);
}

inline bool SemanticPartialAgg::deserialize(CodedInputStream* input)
{
    uint32_t bytes;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

inline bool SemanticPartialAgg::deserialize(const std::string& input)
{
	return pb.ParseFromString(input);
}

inline bool SemanticPartialAgg::deserialize(const char* input, size_t size)
{
	return pb.ParseFromArray(input, size);
}

REGISTER_PAO(SemanticPartialAgg);
