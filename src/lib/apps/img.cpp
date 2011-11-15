#include "img.h"
#include "Neighbor.h"
#include "Defs.h"
#include <math.h>
#include <list>

#define KEYSIZE		64
#define IMG_SIZE	500000

ImagePAO::ImagePAO(const char** const tokens)
{
	if (tokens == NULL) {
		key = NULL;
		value = NULL;
		return;
	}
	// Set key equal to the query file name
	key = (char*)malloc(FILENAME_LENGTH);
	strcpy(key, tokens[0]);

	// Calculate image hash
	CImg<unsigned char> img;
	img.load_jpeg_buffer((JOCTET*)tokens[1], IMG_SIZE);
	pHash(img, hash);

	uint64_t ne_hash;
	Neighbor* n = new Neighbor(tokens[2], FILENAME_LENGTH);
	CImg<unsigned char> ne_img;
	ne_img.load_jpeg_buffer((JOCTET*)tokens[3], IMG_SIZE);
	pHash(ne_img, ne_hash);
	n->distance = abs((long)(hash - ne_hash));
	list<Neighbor> n_list;
	n_list.push_back(*n);
	value = (std::list<Neighbor>*)(&n_list);
}

ImagePAO::~ImagePAO()
{
	free(key);
	std::list<Neighbor>* v = (std::list<Neighbor>*)value;
	for (list<Neighbor>::iterator it = v->begin(); 
			it != v->end(); ++it)
		free(it->key);
}

void ImagePAO::add(void* neighbor_key)
{
}

void ImagePAO::merge(PartialAgg* add_agg)
{
	std::list<Neighbor>* this_list = (std::list<Neighbor>*)value;
	std::list<Neighbor>* mg_list = (std::list<Neighbor>*)(add_agg->value);
	this_list->merge(*mg_list, Neighbor::comp);
}

void ImagePAO::serialize(FILE* f, void* buf, size_t buf_size)
{
	char* wr_buf = (char*)buf;
	char dist[20];
	list<Neighbor>* this_list = (list<Neighbor>*)value;
	strcpy(wr_buf, key);
	for (list<Neighbor>::iterator it = this_list->begin(); 
			it != this_list->end(); ++it) {
		strcat(wr_buf, " ");
		strcat(wr_buf, (*it).key);
		strcat(wr_buf, " ");
		sprintf(dist, "%f", (*it).distance);
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
	list<Neighbor>* this_list = (list<Neighbor>*)value;
	spl = strtok(read_buf, " ");
	if (spl == NULL)
		return false;
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
		this_list->push_back(*n);
	}
	return true;
}

bool ImagePAO::tokenize(void* buf, void* prog, void* tot, char** toks)
{
	char** file_list = (char**)buf;
	uint64_t* tok_index = (uint64_t*)prog;
	uint64_t* tottok = (uint64_t*)tot;
	if (*tok_index == *tottok)
		return false;
	toks[0] = file_list[*tok_index];
	(*tok_index)++;
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
