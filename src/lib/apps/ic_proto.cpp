#include "ic_proto.h"

using namespace google::protobuf::io;
using namespace std;

ImageClusterPAO::ImageClusterPAO()
{
}

ImageClusterPAO::~ImageClusterPAO()
{
}

const char* ICProtoOperations::getKey(PartialAgg* p) const
{
    ImageClusterPAO* wp = (ImageClusterPAO*)p;
    return wp->pb.key().c_str();
}

bool ICProtoOperations::setKey(PartialAgg* p, char* k) const
{
	ImageClusterPAO* wp = (ImageClusterPAO*)p;
    wp->pb.set_key(k);
}

bool ICProtoOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    ImageClusterPAO* wp1 = (ImageClusterPAO*)p1;
    ImageClusterPAO* wp2 = (ImageClusterPAO*)p2;
    return (!wp1->pb.key().compare(wp2->pb.key()));
}


size_t ICProtoOperations::createPAO(Token* t, PartialAgg** p) const
{
    int num_rotations = 4;
    if (t == NULL) {
        p[0] = new ImageClusterPAO();
    } else {
        int l = strlen((char*)(t->tokens[1]));
        int prefix_len = (int)(l * 0.5);
        int step = l / num_rotations;
        string ph((char*)(t->tokens[1]));
        for (int i=0; i<num_rotations; i++) {
//            ImageClusterPAO* img = (ImageClusterPAO*)(p[i]);
            ImageClusterPAO* img = new ImageClusterPAO();
            rotate(ph.begin(), ph.begin()+step, ph.end());
            img->pb.set_key(ph.substr(0, prefix_len));
            img->pb.set_key(ph);
            img->pb.clear_neighbors();
            img->pb.add_neighbors((char*)(t->tokens[0]));
            p[i] = img;
        }
    }
	return num_rotations;
}

size_t ICProtoOperations::dividePAO(const PartialAgg& p,
        PartialAgg** p_list) const
{
    return 1;
}

bool ICProtoOperations::destroyPAO(PartialAgg* p) const
{
    ImageClusterPAO* img = (ImageClusterPAO*)p;
    delete img;
}


bool ICProtoOperations::merge(PartialAgg* p, PartialAgg* mg) const
{
	ImageClusterPAO* img = (ImageClusterPAO*)p;
	ImageClusterPAO* mg_img = (ImageClusterPAO*)mg;
    img->pb.mutable_neighbors()->MergeFrom(mg_img->pb.neighbors());
    return true;
}

inline uint32_t ICProtoOperations::getSerializedSize(PartialAgg* p) const
{
	ImageClusterPAO* img = (ImageClusterPAO*)p;
    return img->pb.ByteSize();
}

bool ICProtoOperations::serialize(PartialAgg* p,
        string* output) const
{
    ImageClusterPAO* wp = (ImageClusterPAO*)p;
    wp->pb.SerializeToString(output);
}

bool ICProtoOperations::serialize(PartialAgg* p,
        char* output, size_t size) const
{
    ImageClusterPAO* wp = (ImageClusterPAO*)p;
    memset((void*)output, 0, size);
    wp->pb.SerializeToArray(output, size);
}

inline bool ICProtoOperations::deserialize(PartialAgg* p,
        const string& input) const
{
    ImageClusterPAO* wp = (ImageClusterPAO*)p;
    return wp->pb.ParseFromString(input);
}

inline bool ICProtoOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
    ImageClusterPAO* wp = (ImageClusterPAO*)p;
    return wp->pb.ParseFromArray(input, size);
}

bool ICProtoOperations::serialize(PartialAgg* p,
        CodedOutputStream* output) const
{
    ImageClusterPAO* wp = (ImageClusterPAO*)p;
    output->WriteVarint32(wp->pb.ByteSize());
    wp->pb.SerializeToCodedStream(output);
}

bool ICProtoOperations::deserialize(PartialAgg* p,
        CodedInputStream* input) const
{
    uint32_t bytes;
    ImageClusterPAO* wp = (ImageClusterPAO*)p;
    input->ReadVarint32(&bytes);
    CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = wp->pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

REGISTER(ICProtoOperations);
