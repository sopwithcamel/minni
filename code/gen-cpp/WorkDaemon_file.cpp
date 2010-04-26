// File Registry implementation
LocalFileRegistry::LocalFileRegistry(JobStatusMap * _smap): status_map(_smap){}

void LocalFileRegistry::bufferData(PartitionID pid, vector<vector<string> > &_return){
  
}

// Checks the status of the local files:
// a) The partition is READY if at least one file is READY 
// b) The partition is BLOCKED if it's not READY and at least one file is BLOCKED
// c) The partition is DONE if all files are DONE
Status LocalFileRegistry::status(PartitionID pid){
  typedef map<JobID,Status> InnerMap;

  FileStatusMap::accesor part_kvp;
  bool done = true;
  this->files.insert(part_kvp, pid);
  for(InnerMap::iterator it = part_kvp->second.begin();
      it != part_kvp->second.end(); it++){
    if(it->second == FileStatus::BLOCKED){
      done = false;
    }
    if(it->second == FileStatus::READY){
      return FileStatus::READY;
    }
  }
  if(done){
    return FileStatus::DONE;
  }
  else{
    return FileStatus::BLOCKED;
  }
}

void LocalFileRegistry::findNew(PartitionID pid){
  typedef map<JobID,Status> InnerMap;


  // 0) Get the parition's key-value pair
  // Key = PartitionID, Value = InnerMap
  FileStatusMap::accessor part_kvp;
  this->files.insert(part_kvp, pid);

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

string FileRegistry::toString(){
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
