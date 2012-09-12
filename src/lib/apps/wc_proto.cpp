#include "wc_proto.h"

using namespace google::protobuf::io;

#define KEY_SIZE        10

WCProtoPAO::WCProtoPAO(char* wrd)
{
    if (wrd) {
        pb.set_key(wrd);
        pb.set_count(1);
    } else
        pb.set_count(0);
}

WCProtoPAO::~WCProtoPAO()
{
}

const char* WCProtoOperations::getKey(PartialAgg* p) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    return wp->pb.key().c_str();
}

bool WCProtoOperations::setKey(PartialAgg* p, char* k) const
{
	WCProtoPAO* wp = (WCProtoPAO*)p;
    wp->pb.set_key(k);
}

bool WCProtoOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    WCProtoPAO* wp1 = (WCProtoPAO*)p1;
    WCProtoPAO* wp2 = (WCProtoPAO*)p2;
    return (!wp1->pb.key().compare(wp2->pb.key()));
}

size_t WCProtoOperations::createPAO(Token* t, PartialAgg** p) const
{
    WCProtoPAO* new_pao;
    if (t == NULL)
        new_pao = new WCProtoPAO(NULL);
    else    
        new_pao = new WCProtoPAO((char*)t->tokens[0]);
    p[0] = new_pao; 
    return 1;
}

size_t WCProtoOperations::dividePAO(const PartialAgg& p,
        PartialAgg** p_list) const
{
    WCProtoPAO* new_pao;
	const WCProtoPAO* wp = (const WCProtoPAO*)(&p);
    new_pao = new WCProtoPAO((char*)(wp->pb.key().c_str()));
    p_list[0] = new_pao; 
    return 1;
}

bool WCProtoOperations::destroyPAO(PartialAgg* p) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    delete wp;
}

bool WCProtoOperations::merge(PartialAgg* p, PartialAgg* mg) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    WCProtoPAO* wmp = (WCProtoPAO*)mg;
    wp->pb.set_count(wp->pb.count() + wmp->pb.count());
}

inline uint32_t WCProtoOperations::getSerializedSize(PartialAgg* p) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    return wp->pb.ByteSize();
}


bool WCProtoOperations::serialize(PartialAgg* p,
        std::string* output) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    wp->pb.SerializeToString(output);
}

bool WCProtoOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    memset((void*)output, 0, size);
    wp->pb.SerializeToArray(output, size);
}

inline bool WCProtoOperations::deserialize(PartialAgg* p,
        const std::string& input) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    return wp->pb.ParseFromString(input);
}

inline bool WCProtoOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    return wp->pb.ParseFromArray(input, size);
}

bool WCProtoOperations::serialize(PartialAgg* p,
        CodedOutputStream* output) const
{
    WCProtoPAO* wp = (WCProtoPAO*)p;
    output->WriteVarint32(wp->pb.ByteSize());
    wp->pb.SerializeToCodedStream(output);
}

bool WCProtoOperations::deserialize(PartialAgg* p,
        CodedInputStream* input) const
{
    uint32_t bytes;
    WCProtoPAO* wp = (WCProtoPAO*)p;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = wp->pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

REGISTER(WCProtoOperations);
