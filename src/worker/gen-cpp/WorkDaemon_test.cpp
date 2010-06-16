#include "common.h"
#include <iostream>
#include <sstream>
#include "WorkDaemon_tasks.h"
#include "WorkDaemon_file.h"


int main(int argc, char **argv) {
  LocalFileRegistry reg;
  reg.recordComplete(1,1,"moose");
  reg.recordComplete(2,1,"badger");
  cout << reg.toString() << endl;

  stringstream ss;
  string output;
  cout << "Blocks: " << reg.blocks(1) << endl;
  for(int i = 0; i < 5; i++){
    reg.bufferData(output, 1, i);
    ss << output;
    cout << i << ": " << output << endl; 
  }
  cout << "Full: " << endl << ss.str();
}
