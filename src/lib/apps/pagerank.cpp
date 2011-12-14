#include "pagerank.h"
#include "Neighbor.h"
#include <math.h>
#include <list>

PageRankPAO::PageRankPAO(char* name, double pr)
{
	if (name == NULL) {
		key = NULL;
		pagerank = 0;
		return;
	}
	key = (char*)malloc(strlen(name) + 1);
	strcpy(key, name);
	pagerank = pr;
}

PageRankPAO::~PageRankPAO()
{
	free(key);
}

size_t PageRankPAO::create(Token* t, PartialAgg** p)
{
	PageRankPAO* new_pao;
	size_t p_ctr = 0;
	double pgrank;
	if (t == NULL) {
		new_pao = new PageRankPAO(NULL, 0);
		p[0] = new_pao;
		return 1;
	}
	size_t num_links = (t->tokens.size() - 1);
	if (t->objs.size() == 0)
		pgrank = 1;
	else {
		PageRankPAO* ref_page = (PageRankPAO*)(t->objs[0]);
		pgrank = ref_page->pagerank / num_links;
	}
	for (int i=1; i<t->tokens.size(); i++) {
		new_pao = new PageRankPAO((char*)t->tokens[i], pgrank);
		p[p_ctr++] = new_pao;
	}
	return num_links;
}

void PageRankPAO::add(void* val)
{
	double v = atof((char*)val);
	pagerank += v;
}

void PageRankPAO::merge(PartialAgg* add_agg)
{
	PageRankPAO* np = (PageRankPAO*)add_agg;
	pagerank += np->pagerank;
}

void PageRankPAO::serialize(void* buf)
{
	char* wr_buf = (char*)buf;
	strcpy(wr_buf, key);
	strcat(wr_buf, " ");
	sprintf(wr_buf + strlen(wr_buf), "%lf", pagerank);
	strcat(wr_buf, "\n");
}

void PageRankPAO::serialize(FILE* f, void* buf, size_t buf_size)
{
	serialize(buf);
	char* wr_buf = (char*)buf;
	assert(NULL != f);
	size_t l = strlen(wr_buf);
	assert(fwrite(wr_buf, sizeof(char), l, f) == l);
}

bool PageRankPAO::deserialize(FILE* f, void* buf, size_t buf_size)
{
	char *read_buf = (char*)buf;
	if (feof(f)) {
		return false;
	}
	if (fgets(read_buf, buf_size, f) == NULL)
		return false;
	return deserialize(read_buf);
}

bool PageRankPAO::deserialize(void* buf)
{
	char *str1, *spl;
	char* read_buf = (char*)buf;
	spl = strtok(str1, " ");
	key = (char*)malloc(strlen(spl) + 1);
	strcpy(key, spl);
	spl = strtok(str1, " ");
	pagerank = atof(spl);
}

REGISTER_PAO(PageRankPAO);
