namespace cpp workdaemon

typedef i16 Status
typedef i64 JobID
typedef i64 PartID  // Parition ID
typedef i64 ChunkID // This is for external data
typedef i32 BlockID // This is for internal data
typedef i32 Count

service WorkDaemon {
	oneway void bark(1:string s), // Debug
	map<JobID, Status> listStatus(),   // Are you alive
	oneway void startMapper(1:JobID jid, 2:ChunkID cid),
	oneway void startReducer(1:JobID jid, 2:PartID kid, 3:string outFile),
	string sendData(1:PartID kid, 2:BlockID sid),
	Status dataStatus(1:PartID pid, 2:BlockID sid),
	Count blockCount(1:PartID pid),
	oneway void kill(1:JobID jid)
}