#include "PartialAgg.h"
#include <iostream>

PartialAgg::PartialAgg() {
	key = "\0";
	value = "0"; 
	
}

PartialAgg::PartialAgg (string k, string v) {
	key = k;
	value = v;
}

PartialAgg::~PartialAgg() { }

void PartialAgg::add (string v) {
	int val;
	stringstream ss1(v);
	ss1 >> val;

	int curr_val;
	stringstream ss2(value);
	ss2 >> curr_val;
	
	curr_val += val;
	
	stringstream ss3;
	ss3 << curr_val;

	value = ss3.str();
}

void PartialAgg::merge (PartialAgg* add_agg) {
	int val;
	stringstream ss1(add_agg->value);
	ss1 >> val;

	int curr_val;
	stringstream ss2(value);
	ss2 >> curr_val;

	curr_val += val;

	stringstream ss3;
	ss3 << curr_val;

	value = ss3.str();

}

string PartialAgg::get_key() {
	return key;
}

string PartialAgg::get_value() {
	return value;
}

void PartialAgg::set_key (string k) {
	key = k;
}

void PartialAgg::set_val(string v) {
	value = v;
}



