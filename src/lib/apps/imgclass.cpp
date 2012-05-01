#include "imgclass.h"

#define KEYSIZE		64

ImageClassPAO::ImageClassPAO(Token* token)
{
	if (token != NULL)
    {
        // Set key equal to the query file name
//        fprintf(stderr, "Key: %s\t", (char*)(token->tokens[0]));
//        fprintf(stderr, "Hash: %s\n", (char*)(token->tokens[1]));
        pb.set_key((char*)(token->tokens[1]));
        imgclasspao::Neighbor* n = pb.add_neighbors();
        n->set_key((char*)(token->tokens[0]));
    }
}

ImageClassPAO::~ImageClassPAO()
{
}

size_t ImageClassPAO::create(Token* t, PartialAgg** p)
{
	ImageClassPAO* img;
	img = new ImageClassPAO(t);
	p[0] = img;
	return 1;
}

void ImageClassPAO::merge(PartialAgg* add_agg)
{
	ImageClassPAO* mg_pao = (ImageClassPAO*)add_agg;
    pb.mutable_neighbors()->MergeFrom(mg_pao->pb.neighbors());
}

inline uint32_t ImageClassPAO::serializedSize() const
{
    return pb.ByteSize();
}

inline void ImageClassPAO::serialize(google::protobuf::io::CodedOutputStream* output) const
{
    output->WriteVarint32(pb.ByteSize());
    pb.SerializeToCodedStream(output);
}

inline void ImageClassPAO::serialize(std::string* output) const
{
    pb.SerializeToString(output);
}

inline void ImageClassPAO::serialize(char* output, size_t size)
{
    memset((void*)output, 0, size);
	pb.SerializeToArray(output, size);
}

inline bool ImageClassPAO::deserialize(google::protobuf::io::CodedInputStream* input)
{
    uint32_t bytes;
    input->ReadVarint32(&bytes);
    google::protobuf::io::CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

inline bool ImageClassPAO::deserialize(const std::string& input)
{
	return pb.ParseFromString(input);
}

inline bool ImageClassPAO::deserialize(const char* input, size_t size)
{
	return pb.ParseFromArray(input, size);
}

REGISTER_PAO(ImageClassPAO);
