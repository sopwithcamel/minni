#include "img.h"

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
ImagePAO::ImagePAO(Token* token)
{
	if (token == NULL) {
		key = NULL;
		neigh_list = NULL;
		return;
	}

	// Set key equal to the query file name
	key = (char*)malloc(token->token_sizes[2]);
	strcpy(key, (char*)(token->tokens[2]));

//	fprintf(stderr, "Key: %s\t", key);
	// Calculate image hash
	CImg<unsigned char> img;
	img.load_jpeg_buffer((JOCTET*)(token->tokens[3]), token->token_sizes[3]);
	pHash(img, hash);

	uint64_t ne_hash;
	fprintf(stderr, "Neigh: %s\n", (char*)token->tokens[0]);
	Neighbor* n = new Neighbor((char*)(token->tokens[0]),
			token->token_sizes[0]);
	CImg<unsigned char> ne_img;
	ne_img.load_jpeg_buffer((JOCTET*)(token->tokens[1]),
			token->token_sizes[1]);
	pHash(ne_img, ne_hash);
	n->distance = abs((long)(hash - ne_hash));

	neigh_list = new std::list<Neighbor>();
	neigh_list->push_back(*n);
//	fprintf(stderr, "Found neighbor: %s, %ld\n", n->key, n->distance);
}

ImagePAO::~ImagePAO()
{
	if (key)
		free(key);
	if (neigh_list)
		neigh_list->clear();
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
void ImagePAO::add(void* val)
{	
	char* spl;
	char* read_buf = (char*)val;
	Neighbor* n;
	std::list<Neighbor>::iterator it; 
	// read key
	spl = strtok(read_buf, " ");
	while (true) {
		/* read in neighbor key */
		spl = strtok(NULL, " ");
		if (spl == NULL)
			break;
		/* create new neighbor */
		n = new Neighbor(spl, strlen(spl)+1);
		/* read in neighbor distance */
		spl = strtok(NULL, " ");
		n->distance = atof(spl);	
		neigh_list->push_back(*n);
	}
	neigh_list->sort(Neighbor::comp);
	if (neigh_list->size() > NEAREST) {
		it = neigh_list->begin();
		advance(it, NEAREST);	
		neigh_list->erase(it, neigh_list->end());
	}
}

void ImagePAO::merge(PartialAgg* add_agg)
{
	ImagePAO* mg_pao = (ImagePAO*)add_agg;
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

void ImagePAO::serialize(FILE* f, void* buf, size_t buf_size)
{
	serialize(buf);
	char* wr_buf = (char*)buf;
	assert(NULL != f);
	size_t l = strlen(wr_buf);
	assert(fwrite(wr_buf, sizeof(char), l, f) == l);
}

void ImagePAO::serialize(void* buf)
{
	char* wr_buf = (char*)buf;
	char dist[20];
	std::list<Neighbor>::iterator it; 
	strcpy(wr_buf, key);
	for (it = neigh_list->begin(); it != neigh_list->end(); ++it) {
		strcat(wr_buf, " ");
		strcat(wr_buf, (*it).key);
		strcat(wr_buf, " ");
		sprintf(dist, "%lu", (*it).distance);
		strcat(wr_buf, dist);
	}	
	strcat(wr_buf, "\n");
}

bool ImagePAO::deserialize(FILE* f, void* buf, size_t buf_size)
{
	char *read_buf = (char*)buf;
	if (feof(f)) {
		return false;
	}
	if (fgets(read_buf, buf_size, f) == NULL)
		return false;
	return deserialize(read_buf);
}

bool ImagePAO::deserialize(void* buf)
{
	char* spl;
	char* read_buf = (char*)buf;
	Neighbor* n;
	neigh_list = new std::list<Neighbor>();
	spl = strtok(read_buf, " ");
	if (spl == NULL)
		return false;
	key = (char*)malloc(strlen(spl) + 1);
	strcpy(key, spl);
	while (true) {
		/* read in neighbor key */
		spl = strtok(NULL, " ");
		if (spl == NULL)
			break;
		/* create new neighbor */
		n = new Neighbor(spl, KEYSIZE);
		/* read in neighbor distance */
		spl = strtok(NULL, " ");
		n->distance = atof(spl);	
		neigh_list->push_back(*n);
	}
	return true;
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


void ImagePAO::pHash(CImg<unsigned char> src, unsigned long& hash)
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

REGISTER_PAO(ImagePAO);
