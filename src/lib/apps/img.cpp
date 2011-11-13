#include "img.h"

#define KEYSIZE		64

ImagePAO::ImagePAO(const char** const tokens)
{
	if (tokens == NULL) {
		key = NULL;
		value = NULL;
		return;
	}	
	CImg<unsigned char> img;
	unsigned long hash;
	img.load_jpeg_buffer((JOCTET*)tokens[0], strlen(tokens[0]));
	pHash(img, hash);
	key = (char*)malloc(KEYSIZE);
	// make phash value the key of the PAO
	sprintf(key, "%lu", hash);
	value = (uint64_t*)malloc(sizeof(uint64_t));
	*((uint64_t*)value) = 1;
}

ImagePAO::~ImagePAO()
{
	free(key);
	free(value);
}

void ImagePAO::add(void* neighbor_key)
{
}

void ImagePAO::merge(PartialAgg* add_agg)
{
	*((uint64_t*)value) += *(uint64_t*)(add_agg->value);
}

void ImagePAO::serialize(FILE* f, void* buf, size_t buf_size)
{
	uint64_t* _value = (uint64_t*)value;
	char* wr_buf = (char*)buf;
	strcpy(wr_buf, key);
	strcat(wr_buf, " ");
	sprintf(wr_buf + strlen(wr_buf), "%lu", *_value);
	strcat(wr_buf, "\n");
	assert(NULL != f);
	size_t l = strlen(wr_buf);
	assert(fwrite(wr_buf, sizeof(char), l, f) == l);
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
	char *spl;
	char *read_buf = (char*)buf;
	spl = strtok(read_buf, " \n\r");
	if (spl == NULL)
		return false;
	key = (char*)malloc(KEYSIZE);
	strcpy(key, spl);
	spl = strtok(NULL, " \n\r");
	if (spl == NULL)
		return false;
	*((uint64_t*)value) = atol(spl);
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
