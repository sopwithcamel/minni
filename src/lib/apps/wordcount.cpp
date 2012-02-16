#include "wordcount.h"

using namespace google::protobuf::io;

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
}


size_t WordCountPartialAgg::create(Token* t, PartialAgg** p)
{
	WordCountPartialAgg* new_pao;
	if (t == NULL)
		new_pao = new WordCountPartialAgg(NULL);
	else	
		new_pao = new WordCountPartialAgg((char*)t->tokens[0]);
	p[0] = new_pao;	
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

inline void WordCountPartialAgg::serialize(
        CodedOutputStream* output) const
{
    output->WriteVarint32(pb.ByteSize());
    assert(pb.SerializeToCodedStream(output));
}

inline void WordCountPartialAgg::serialize(std::string* output) const
{
    pb.SerializeToString(output);
}

inline bool WordCountPartialAgg::deserialize(CodedInputStream* input)
{
    uint32_t bytes;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    assert(pb.ParseFromCodedStream(input));
    input->PopLimit(msgLimit);
}

inline bool WordCountPartialAgg::deserialize(const std::string& input)
{
	assert(pb.ParseFromString(input));
    return true;
}

inline bool WordCountPartialAgg::deserialize(const char* input, size_t size)
{
	assert(pb.ParseFromArray(input, size));
    return true;
}

REGISTER_PAO(WordCountPartialAgg);
