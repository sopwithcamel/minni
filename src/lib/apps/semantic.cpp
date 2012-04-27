#include "semantic.h"

using namespace google::protobuf::io;

SemanticPartialAgg::SemanticPartialAgg(char* wrd, char* book)
{
	if (wrd) {
        SynsetPtr p, pt;
        wninit();
        p = findtheinfo_ds(wrd, NOUN, HYPERPTR, ALLSENSES);
        if (p) {
            if (p->ptrcount > 0) {
                pt = read_synset(p->ppos[0], p->ptroff[0], "");
                printf("Syn: %s\n", pt->words[0]);
            }
        }
		pb.set_key(wrd);
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
	else	
		new_pao = new SemanticPartialAgg((char*)t->tokens[0], (char*)t->tokens[1]);
	p[0] = new_pao;	
	return 1;
}

void SemanticPartialAgg::merge(PartialAgg* add_agg)
{
	SemanticPartialAgg* wp = (SemanticPartialAgg*)add_agg;
//	pb.set_count(count() + wp->count());
}

inline void SemanticPartialAgg::serialize(
        CodedOutputStream* output) const
{
    output->WriteVarint32(pb.ByteSize());
//    pb.SerializeToCodedStream(output);
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
