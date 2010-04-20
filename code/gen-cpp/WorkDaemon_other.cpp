#include "WorkDaemon_other.h"
#include <fstream>

using namespace std;

string local_filename(JobID jid, KeyID kid){
  stringstream ss;
  ss << "local_" << kid << "_" << jid;
  return ss.str();
}

void generate_random_keyvalue_pairs(string filename, int num){
  ofstream file;
  file.open(filename.c_str());
  for(unsigned int i = 0; i < num; i++){
    file << i << "; word_" << i << endl; 
  }
  file.close();
}

unsigned int paritition(unsigned int i){
  return i % PARAM_R;
}
