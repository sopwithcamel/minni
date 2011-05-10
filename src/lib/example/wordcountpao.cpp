#include "wordcountpao.h"

void WordCountPartialAgg::add (string v) {
	int val;
	stringstream ss1(v);
	ss1 >> val;
	int curr_val;
	stringstream ss2(PartialAgg::value);
	ss2 >> curr_val;
	curr_val += val;
	stringstream ss3;
	ss3 << curr_val;
	value = ss3.str();
}

void WordCountPartialAgg::merge (PartialAgg* add_agg) {
	int val;
	stringstream ss1(add_agg->value);
	ss1 >> val;

	int curr_val;
	stringstream ss2(PartialAgg::value);
	ss2 >> curr_val;

	curr_val += val;

	stringstream ss3;
	ss3 << curr_val;

	value = ss3.str();
}

REGISTER_PAO(WordCountPartialAgg);
