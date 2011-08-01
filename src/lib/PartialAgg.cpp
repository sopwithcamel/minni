#include "config.h"
#include "PartialAgg.h"
#include <iostream>

using namespace std;

PartialAgg::PartialAgg(char *k) {
        key = k;
	strcpy(value,"0"); 	
}

PartialAgg::PartialAgg (char* k, char *v) {
	key = k;
	value = v;
}

PartialAgg::~PartialAgg() { }

// sprintf writes a trailing null character.
void PartialAgg::add(const char *v) {
	int val = atoi(v);
	int curr_val = atoi(value);
	curr_val += val;
	sprintf(value, "%d", curr_val);
}


void PartialAgg::merge (PartialAgg* add_agg) {
	int val = atoi(add_agg->value);
	int curr_val = atoi(value);
	curr_val += val;
	sprintf(value, "%d", curr_val);
}


char* PartialAgg::get_value() {
	return value;
}


void PartialAgg::set_val(char* v) {
	strcpy(value, v);
}
