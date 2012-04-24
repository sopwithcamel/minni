#include "img.h"

#define KEYSIZE		64
#define IMG_SIZE	500000
#define NEAREST		5

/**
 * Creates a PAO that represents the relationship between a query image and a c
 * reference image. The pHash values for both images are calculated and the
 * absolute value of the difference is used as the distance. Note that the
 * PAO doesn't store the image data.
 *
 * Arguments:
 *  - tokens[0] to have the query image name
 *  - tokens[1] to have the query image data
 *  - tokens[2] to have neighbor name
 *  - tokens[3] to have neighbor image data
 *  - token_sizes to have the respective buffer lengths
 */
ImagePAO::ImagePAO(Token* token)
{
	if (token == NULL) {
		pb.set_key(NULL);
        /* nada; leave at size 0 pb.add_neighbors(); */
		return;
	}

	// Set key equal to the query file name
	pb.set_key((char*) (token->tokens[2]));

    //	fprintf(stderr, "Key: %s\t", key);
	// Calculate image hash
	CImg<unsigned char> img;
	img.load_jpeg_buffer((JOCTET*)(token->tokens[3]), token->token_sizes[3]);
	pHash(img, hash);

	uint64_t ne_hash;
	fprintf(stderr, "Neigh: %s\n", (char*)token->tokens[0]);
    imagepao::Neighbor* n = pb.add_neighbors();
	n->set_key((char*)(token->tokens[0]));

	CImg<unsigned char> ne_img;
	ne_img.load_jpeg_buffer((JOCTET*)(token->tokens[1]),
			token->token_sizes[1]);
	pHash(ne_img, ne_hash);
	n->set_distance(abs((long)(hash - ne_hash)));

//	fprintf(stderr, "Found neighbor: %s, %ld\n", n->key, n->distance);
}

ImagePAO::~ImagePAO()
{
}

size_t ImagePAO::create(Token* t, PartialAgg** p)
{
	ImagePAO* img;
	img = new ImagePAO(t);
	p[0] = img;
	return 1;
}

/**
 * Adds the serialized images and distances to the current PAO, sorts the list
 * and keeps the K-nearest neighbors.
 *
 * Arguments:
 *  - val is expected to be a mutable array
 */ 
//void ImagePAO::add(void* val)
//{	
//	char* spl;
//	char* read_buf = (char*)val;
//	Neighbor* n;
//	std::list<Neighbor>::iterator it; 
//	// read key
//	spl = strtok(read_buf, " ");
//	while (true) {
//		/* read in neighbor key */
//		spl = strtok(NULL, " ");
//		if (spl == NULL)
//			break;
//		/* create new neighbor */
//		n = new Neighbor(spl, strlen(spl)+1);
//		/* read in neighbor distance */
//		spl = strtok(NULL, " ");
//		n->distance = atof(spl);	
//		neigh_list->push_back(*n);
//	}
//	neigh_list->sort(Neighbor::comp);
//	if (neigh_list->size() > NEAREST) {
//		it = neigh_list->begin();
//		advance(it, NEAREST);	
//		neigh_list->erase(it, neigh_list->end());
//	}
//}

void ImagePAO::merge(PartialAgg* add_agg)
{
	ImagePAO* mg_pao = (ImagePAO*)add_agg;
    mutable_neighbors()->MergeFrom(mg_pao->neighbors());

    sort(mutable_neighbors()->begin(),
         mutable_neighbors()->end(),
         comparator);

	if (neighbors_size() > NEAREST) {
        mutable_neighbors()->RemoveLast();
	}
}

CImg<float>* ImagePAO::ph_dct_matrix(const int N)
{
	CImg<float> *ptr_matrix = new CImg<float>(N,N,1,1,1/sqrt((float)N));
	const float c1 = sqrt(2.0/N);
	for (int x=0;x<N;x++){
		for (int y=1;y<N;y++){
			*ptr_matrix->data(x,y) = c1*cos((cimg::PI/2/N)*y*(2*x+1));
		}
	}
	return ptr_matrix;
}

void ImagePAO::pHash(CImg<unsigned char> src, uint64_t& hash)
{
	CImg<float> meanfilter(7,7,1,1,1);
	CImg<float> img;
	if (src.spectrum() == 3){
		img = src.RGBtoYCbCr().channel(0).get_convolve(meanfilter);
	} else if (src.spectrum() == 4){
		int width = img.width();
		int height = img.height();
		int depth = img.depth();
		img = src.crop(0,0,0,0,width-1,height-1, depth-1,
				2).RGBtoYCbCr().channel(0).get_convolve(meanfilter);
	} else {
		img = src.channel(0).get_convolve(meanfilter);
	}

	img.resize(32,32);
	CImg<float> *C  = ph_dct_matrix(32);
	CImg<float> Ctransp = C->get_transpose();

	CImg<float> dctImage = (*C)*img*Ctransp;

	CImg<float> subsec = dctImage.crop(1,1,8,8).unroll('x');;

	float median = subsec.median();
	long one = 0x0000000000000001;
	hash = 0x0000000000000000;
	for (int i=0;i< 64;i++){
		float current = subsec(i);
		if (current > median)
			hash |= one;
		one = one << 1;
	}
}

inline void ImagePAO::serialize(google::protobuf::io::CodedOutputStream* output) const
{
    output->WriteVarint32(pb.ByteSize());
    pb.SerializeToCodedStream(output);
}

inline void ImagePAO::serialize(std::string* output) const
{
    pb.SerializeToString(output);
}

inline bool ImagePAO::deserialize(google::protobuf::io::CodedInputStream* input)
{
    uint32_t bytes;
    input->ReadVarint32(&bytes);
    google::protobuf::io::CodedInputStream::Limit msgLimit = input->PushLimit(bytes);
    bool ret = pb.ParseFromCodedStream(input);
    input->PopLimit(msgLimit);
    return ret;
}

inline bool ImagePAO::deserialize(const std::string& input)
{
	return pb.ParseFromString(input);
}

inline bool ImagePAO::deserialize(const char* input, size_t size)
{
	return pb.ParseFromArray(input, size);
}

REGISTER_PAO(ImagePAO);
