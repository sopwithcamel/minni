#include "wcdigram.h"
#include <sstream>
#include <string>
#include <stdlib.h>

using namespace google::protobuf::io;

#define KEY_SIZE        10

WCDigram::WCDigram(char* one, char* two)
{
	if (one) {
		pb.set_key(one);
        pb.mutable_key()->append("-");
        pb.mutable_key()->append(two);
        pb.set_count(1);
	} else
        pb.set_count(0);
}

WCDigram::~WCDigram()
{
}


size_t WCDigram::create(Token* t, PartialAgg** p)
{
	WCDigram* new_pao;
	if (t == NULL)
		new_pao = new WCDigram(NULL, NULL);
	else	
		new_pao = new WCDigram((char*)(t->tokens[0]), (char*)(t->tokens[1]));
	p[0] = new_pao;	
	return 1;
}

void WCDigram::merge(PartialAgg* add_agg)
{
	WCDigram* wp = (WCDigram*)add_agg;
	pb.set_count(count() + wp->count());
}

inline void WCDigram::serialize(
        CodedOutputStream* output) const
{
    output->WriteVarint32(pb.ByteSize());
    pb.SerializeToCodedStream(output);
}

inline uint32_t WCDigram::serializedSize() const
{
    return pb.ByteSize();
}


inline void WCDigram::serialize(std::string* output) const
{
    pb.SerializeToString(output);
}

inline void WCDigram::serialize(char* output, size_t size)
{
    memset((void*)output, 0, size);
	pb.SerializeToArray(output, size);
}

inline bool WCDigram::deserialize(CodedInputStream* input)
{
    uint32_t bytes;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

inline bool WCDigram::deserialize(const std::string& input)
{
	return pb.ParseFromString(input);
}

inline bool WCDigram::deserialize(const char* input, size_t size)
{
	return pb.ParseFromArray(input, size);
}

REGISTER_PAO(WCDigram);
