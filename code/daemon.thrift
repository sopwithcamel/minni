namespace cpp workdaemon

typedef i16 Status
typedef i64 JobID
typedef i64 PartID  // Parition ID
typedef i64 ChunkID // This is for external data
typedef i32 BlockID // This is for internal data
typedef i32 Count
typedef string URL

service WorkDaemon {
	oneway void bark(1:string s), // Debug
	string stateString(),         // Debug
	oneway void kill();	      // Debug
	map<JobID, Status> listStatus(),   // Are you alive
	oneway void startMapper(1:JobID jid, 2:string inFile, 3:ChunkID cid),
	oneway void startReducer(1:JobID jid, 2:PartID pid, 3:string outFile),
	string sendData(1:PartID kid, 2:BlockID bid),
	Status partitionStatus(1:PartID pid),
	Count blockCount(1:PartID pid),
	oneway void reportCompletedJobs(1:list<URL> done)
}