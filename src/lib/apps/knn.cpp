#include "knn.h"

kNNPAO::kNNPAO(char** tokens)
{
	key = (char*)malloc(KEYSIZE);
	strcpy(key, tokens[0]);
	list<Neighbor> n_list;
	value = (list<Neighbor>*)(&n_list); 
}

kNNPAO::~kNNPAO()
{
	free(key);
	list<Neighbor>* n_list = (list<Neighbor>*)value;
	n_list->clear();
}

void kNNPAO::add(void* neighbor_key)
{
	char* k = (char*)neighbor_key;
	float d = calculate_distance(k);
	check_insert(k, d);
}

void kNNPAO::merge(PartialAgg* add_agg)
{
	list<Neighbor>* this_list = (list<Neighbor>*)value;
	list<Neighbor>* mg_list = (list<Neighbor>*)(add_agg->value);
	this_list->merge(*mg_list, Neighbor::comp);
	if (this_list->size() > K) {
		list<Neighbor>::iterator it1, it2;
		it1 = this_list->begin();
		advance(it1, K);
		it2 = this_list->end();
		this_list->erase(it1, it2);
	}
}

void kNNPAO::serialize(FILE* f, void* buf, size_t buf_size)
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

bool kNNPAO::deserialize(FILE* f, void* buf, size_t buf_size)
{
	char* read_buf = (char*)buf;

	if (feof(f)) {
		return false;
	}
	if (fgets(read_buf, buf_size, f) == NULL)
		return false;
	deserialize(read_buf);
}

bool kNNPAO::deserialize(void* buf)
{
	char* spl;
	char* read_buf = (char*)buf;
	Neighbor* n;
	list<Neighbor>* this_list = (list<Neighbor>*)value;
	spl = strtok(read_buf, " ");
	if (spl == NULL)
		return false;
	strcpy(key, spl);
	for (int i=0; i<K; i++) {
		/* read in neighbor key */
		spl = strtok(NULL, " ");
		if (spl == NULL)
			return false;
		/* create new neighbor */
		n = new Neighbor(spl, KEYSIZE);
		/* read in neighbor distance */
		spl = strtok(NULL, " ");
		if (spl == NULL)
			return false;
		n->distance = atof(spl);	
		this_list->push_back(*n);
	}
	return true;
}

bool tokenize(void* buf, void* prog, void* tot, char** toks)
{
}

float kNNPAO::calculate_distance(const char* key)
{
	return 1;
}

void kNNPAO::check_insert(const char* key, float distance)
{
	list<Neighbor>* this_list = (list<Neighbor>*)value;
	for (list<Neighbor>::iterator it = this_list->begin(); 
			it != this_list->end(); ++it) {
		if (distance < (*it).distance) {
			Neighbor* n = new Neighbor(key, KEYSIZE, distance);
			this_list->insert(it, *n);
		}
	}
	/* remove one element at the end of the list */
	if (this_list->size() > K)
		this_list->pop_back();
}

REGISTER_PAO(kNNPAO);
