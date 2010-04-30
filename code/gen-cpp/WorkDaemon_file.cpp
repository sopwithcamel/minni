#include "WorkDaemon_file.h"
#include <iostream>
#include <sstream>
#include <cmath>

// File

File::File(JobID j, PartID p, string n, Length l): 
  jid(j), pid(p), name(n), length(l){}

string File::toString() const{
  stringstream ss;
  ss << "File[j:" << jid << ",p:" << pid << ",n:" << name << ",l:" << length << "]";
  return ss.str();
}

Count File::blocks(const Length block_size) const{
  return (Count) ceil((float)this->length / (float)block_size);
}

// File Registry implementation
LocalFileRegistry::LocalFileRegistry(Length _bsize) : block_size(_bsize){}

void LocalFileRegistry::bufferData(const PartID pid, const BlockID bid, vector<vector<string> > &_return){
  
}

// Checks the status of the local partition
Count LocalFileRegistry::blocks(const PartID p) const{
  FileMap::accessor acc_part;
  bool found = file_map.find(acc_part ,p);
  if(!found){
    return 0;
  }
  
  Count sum = 0;
  for(vector<File>::const_iterator it = acc_part->second.begin();
      it != acc_part->second.end(); it++){
    sum += it->blocks(this->block_size);
  }
  return sum;
}

void LocalFileRegistry::recordComplete(const JobID j, const PartID p, const string n, const Length l){
  FileMap::accessor acc_part;
  this->file_map.insert(acc_part, p);
  acc_part->second.push_back(File(j,p,n,l));
}


void LocalFileRegistry::recordComplete(const vector<File> &files){
  for(vector<File>::const_iterator it = files.begin(); it != files.end(); it++){
    this->recordComplete(it->jid, it->pid, it->name, it->length);
  }
}

string LocalFileRegistry::toString() const{
  stringstream ss;
  FileMap::const_range_type R = this->file_map.range();
  ss << "[" << endl;
  for(FileMap::const_iterator it = R.begin(); it != R.end(); it++){
    ss << "\t" << it->first << " -> [" << endl;
    for(vector<File>::const_iterator inner_it = it->second.begin();
	inner_it != it->second.end(); inner_it++){
      ss << "\t\t" << inner_it->toString() << endl;
      }
    ss << "\t]" << endl;
  }

  ss << "]" << endl;

  return ss.str();
}
