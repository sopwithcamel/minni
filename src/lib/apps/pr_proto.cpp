#include "pr_proto.h"
#include <sstream>

using namespace std;
using namespace google::protobuf::io;

#define KEY_SIZE        10

PageRankProtoPAO::PageRankProtoPAO()
{
    pb.set_key("");
    pb.set_rank(0);
    pb.set_outlinks(0);
}

PageRankProtoPAO::~PageRankProtoPAO()
{
}

const char* PageRankProtoOperations::getKey(PartialAgg* p) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    return wp->pb.key().c_str();
}

bool PageRankProtoOperations::setKey(PartialAgg* p, char* k) const
{
	PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    wp->pb.set_key(k);
}

bool PageRankProtoOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    PageRankProtoPAO* wp1 = (PageRankProtoPAO*)p1;
    PageRankProtoPAO* wp2 = (PageRankProtoPAO*)p2;
    return (!wp1->pb.key().compare(wp2->pb.key()));
}

size_t PageRankProtoOperations::createPAO(Token* t, PartialAgg** p) const
{
    PageRankProtoPAO* new_pao;
    new_pao = new PageRankProtoPAO();
    p[0] = new_pao; 
    return 1;
}

size_t PageRankProtoOperations::dividePAO(const PartialAgg& p,
        PartialAgg** p_list) const
{
    PageRankProtoPAO* new_pao;
	const PageRankProtoPAO* wp = static_cast<const PageRankProtoPAO*>(&p);

    // copy the input PAO as the first output PAO
/*
    new_pao = new PageRankProtoPAO(*wp);
    p_list[0] = new_pao;
    p_list[0] = (PageRankProtoPAO*)wp;
*/

    uint32_t out_links = wp->pb.outlinks();
    if (out_links) {
        float pr_given = wp->pb.rank() / out_links;

        stringstream ss(wp->pb.links());
        string l;
        for (uint32_t i=0; i<out_links; i++) {
//            new_pao = (PageRankProtoPAO*)(p_list[i]);
            getline(ss, l, ' ');
            new_pao = new PageRankProtoPAO();
            new_pao->pb.set_key(l);
            new_pao->pb.set_rank(pr_given);
            p_list[i] = new_pao;
        }
    }
    return out_links;
}

bool PageRankProtoOperations::destroyPAO(PartialAgg* p) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    delete wp;
}

bool PageRankProtoOperations::merge(PartialAgg* p, PartialAgg* mg) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    PageRankProtoPAO* wmp = (PageRankProtoPAO*)mg;
    wp->pb.set_rank(wp->pb.rank() + wmp->pb.rank());
    if (wp->pb.outlinks() == 0 && wmp->pb.outlinks() > 0) {
        wp->pb.set_outlinks(wmp->pb.outlinks());
        wp->pb.set_links(wmp->pb.links());
    }
}

inline uint32_t PageRankProtoOperations::getSerializedSize(PartialAgg* p) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    return wp->pb.ByteSize();
}


bool PageRankProtoOperations::serialize(PartialAgg* p,
        string* output) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    wp->pb.SerializeToString(output);
}

bool PageRankProtoOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    memset((void*)output, 0, size);
    wp->pb.SerializeToArray(output, size);
}

inline bool PageRankProtoOperations::deserialize(PartialAgg* p,
        const string& input) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    return wp->pb.ParseFromString(input);
}

inline bool PageRankProtoOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    return wp->pb.ParseFromArray(input, size);
}

bool PageRankProtoOperations::serialize(PartialAgg* p,
        CodedOutputStream* output) const
{
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    output->WriteVarint32(wp->pb.ByteSize());
    wp->pb.SerializeToCodedStream(output);
}

bool PageRankProtoOperations::deserialize(PartialAgg* p,
        CodedInputStream* input) const
{
    uint32_t bytes;
    PageRankProtoPAO* wp = (PageRankProtoPAO*)p;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = wp->pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

REGISTER(PageRankProtoOperations);
