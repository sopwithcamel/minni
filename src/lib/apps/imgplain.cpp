#include "imgplain.h"

#define KEYSIZE		64
#define IMG_SIZE	500000
#define NEAREST		5

/**
 * Creates a PAO that represents the relationship between a query image and a 
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
ImagePlain::ImagePlain(Token* token)
{
	if (token == NULL) {
		key_ = "";
		neigh_list = NULL;
		return;
	}

	// Set key equal to the query file name
	key_ = string((const char*)(token->tokens[2]));

	// Calculate image hash
	CImg<unsigned char> img;
	img.load_jpeg_buffer((JOCTET*)(token->tokens[3]), token->token_sizes[3]);
	pHash(img, hash);

	uint64_t ne_hash;
	Neighbor* n = new Neighbor((char*)(token->tokens[0]),
			token->token_sizes[0]);
	CImg<unsigned char> ne_img;
	ne_img.load_jpeg_buffer((JOCTET*)(token->tokens[1]),
			token->token_sizes[1]);
	pHash(ne_img, ne_hash);
	n->distance = abs((long)(hash - ne_hash));

	neigh_list = new std::list<Neighbor>();
	neigh_list->push_back(*n);
}

ImagePlain::~ImagePlain()
{
    /* key cleanup is handled (no new) */
	if (neigh_list)
		neigh_list->clear();
}

size_t ImagePlain::create(Token* t, PartialAgg** p)
{
	ImagePlain* img;
	img = new ImagePlain(t);
	p[0] = img;
	return 1;
}

void ImagePlain::merge(PartialAgg* add_agg)
{
	ImagePlain* mg_pao = (ImagePlain*)add_agg;
	std::list<Neighbor>* mg_list = mg_pao->neigh_list;
	Neighbor* new_neighbor;
	std::list<Neighbor>::iterator it; 
	for (it=mg_list->begin(); it != mg_list->end(); ++it) {
		new_neighbor = new Neighbor(*it);
		neigh_list->push_back(*new_neighbor);
	}
	neigh_list->sort(Neighbor::comp);
	if (neigh_list->size() > NEAREST) {
		it = neigh_list->begin();
		advance(it, NEAREST);	
		neigh_list->erase(it, neigh_list->end());
	}
}

inline const std::string& ImagePlain::key() const
{
    return key_;
}

void ImagePlain::serialize(boost::archive::binary_oarchive* output) const
{
    size_t size;
    (*output) << hash;
    (*output) << key_;
    size = neigh_list->size();
    (*output) << size;

	std::list<Neighbor>::iterator it; 
	for (it=neigh_list->begin(); it != neigh_list->end(); ++it) {
        string key__ = (*it).key;
        (*output) << key__;
        (*output) << (*it).distance;
	}
}

bool ImagePlain::deserialize(boost::archive::binary_iarchive* input)
{
    size_t size;
    try {
        (*input) >> hash;
        (*input) >> key_;
        (*input) >> size;
        neigh_list = new std::list<Neighbor>();
        for (size_t i = 0; i < size; i++)
        {
            string key_n;
            uint64_t distance_n;
            (*input) >> key_n;
            (*input) >> distance_n;
            Neighbor* new_neighbor = new Neighbor(key_n.c_str(), key_n.length(), distance_n);
            neigh_list->push_back(*new_neighbor);
        }
        return true;
    } catch (...) {
        return false;
    }
}

CImg<float>* ImagePlain::ph_dct_matrix(const int N)
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


void ImagePlain::pHash(CImg<unsigned char> src, uint64_t& hash)
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

REGISTER_PAO(ImagePlain);
