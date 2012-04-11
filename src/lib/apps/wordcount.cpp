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

void WordCountPartialAgg::merge(PartialAgg* add_agg)
{
	WordCountPartialAgg* wp = (WordCountPartialAgg*)add_agg;
	pb.set_count(count() + wp->count());
}

inline void WordCountPartialAgg::serialize(
        CodedOutputStream* output) const
{
    output->WriteVarint32(pb.ByteSize());
    pb.SerializeToCodedStream(output);
}

inline uint32_t WordCountPartialAgg::serializedSize() const
{
    return pb.ByteSize();
}


inline void WordCountPartialAgg::serialize(std::string* output) const
{
    pb.SerializeToString(output);
}

inline void WordCountPartialAgg::serialize(char* output, size_t size)
{
    memset((void*)output, 0, size);
	pb.SerializeToArray(output, size);
}

inline bool WordCountPartialAgg::deserialize(CodedInputStream* input)
{
    uint32_t bytes;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

inline bool WordCountPartialAgg::deserialize(const std::string& input)
{
	return pb.ParseFromString(input);
}

inline bool WordCountPartialAgg::deserialize(const char* input, size_t size)
{
	return pb.ParseFromArray(input, size);
}

REGISTER_PAO(WordCountPartialAgg);
