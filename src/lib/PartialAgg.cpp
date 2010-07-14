#include "config.h"
#include "PartialAgg.h"

PartialAgg::PartialAgg() {
	value = "0"; 
	
}

PartialAgg::PartialAgg (string v) {
	value = v;
}

PartialAgg::~PartialAgg() { }

void PartialAgg::add (string v) {
	char* tot = (char*)malloc(16);
	int val = atoi(v.c_str());
	int curr_val = atoi(value.c_str());
	curr_val += val;
	sprintf(tot, "%d", curr_val);
	value.assign(tot);
	free(tot);
}

void PartialAgg::merge (PartialAgg* add_agg) {
	char* tot = (char*)malloc(10);
	int val = atoi(add_agg->value);
	int curr_val = atoi(value);
	curr_val += val;
	sprintf(tot, "%d", curr_val);
	value.assign(tot);
	free(tot);
}



string PartialAgg::get_value() {
	return value;
}


void PartialAgg::set_val(string v) {
	value = v;
}












