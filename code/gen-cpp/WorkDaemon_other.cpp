#include "WorkDaemon_other.h"
#include <fstream>
#include <sstream>
#include <iostream>

using namespace std;

// Misc. Functions

string local_filename(JobID jid, PartitionID pid){
  stringstream ss;
  ss << "local_" << pid << "_" << jid;
  return ss.str();
}

void generate_keyvalue_pairs(string filename, int num){
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


// File Registry implementation
FileRegistry::FileRegistry(JobStatusMap * _smap): status_map(_smap){}

void FileRegistry::get_data(PartitionID pid, vector<vector<string> > &_return){
  
}

Status FileRegistry::get_status(PartitionID pid){
  
}

void FileRegistry::find_new(PartitionID pid){
  typedef map<JobID,Status> InnerMap;

  // 0) Get the parition's key-value pair
  // Key = PartitionID, Value = InnerMap
  FileStatusMap::accessor part_kvp;
  bool found = this->registry.find(part_kvp, pid);
  if(!found){
    // 0a) First insert; initialize
    this->registry.insert(part_kvp,pid);
    part_kvp->second = InnerMap();
  }

  // 1) Go through all the completed jobs in the status map  
  for(JobStatusMap::iterator it = this->status_map->begin();
      it != this->status_map->end(); it++){
    if(it->second == JobStatus::DONE){
      // 1a) There is a completed job, does it have a file?
      JobID jid = it->first;     
      if((part_kvp->second).count(jid) == 0 
	 || (part_kvp->second[jid] == FileStatus::DNE)){
	// New file, create
	part_kvp->second[jid] = FileStatus::READY;
      }      
    }
  }
}

string FileRegistry::to_string(){
  stringstream ss;
  typedef map<JobID, Status> InnerMap;
  for(FileStatusMap::iterator outer = registry.begin(); 
      outer != registry.end(); outer++){
    ss << outer->first << ":" <<  endl;
    for(InnerMap::iterator inner = outer->second.begin();
	inner != outer->second.end(); inner++){
      ss << "\t-> (" << inner->first << ", " 
	   << inner->second << ")" << endl;
    }
  }
  return ss.str();
}

