#pragma once

#include <iostream>
#include <sstream>
#include "daemon_types.h"

#define PARAM_M = 20;
#define PARAM_R = 10;

using namespace std;
using namespace workdaemon;

string local_filename(JobID jid, KeyID kid);
void generate_random_keyvalue_pairs(string filename, int num);
unsigned int paritition(unsigned int i);
