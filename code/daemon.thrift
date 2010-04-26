namespace cpp workdaemon

typedef i16 Status
typedef i64 JobID
typedef i64 PartitionID
typedef i64 ChunkID
typedef i32 SeriesID

service WorkDaemon {
	oneway void bark(1:string s), // Debug
	map<JobID, Status> listStatus(),   // Are you alive
	oneway void startMapper(1:JobID jid, 2:ChunkID cid),
	oneway void startReducer(1:JobID jid, 2:PartitionID kid, 3:string outFile),
	list<list<string>> sendData(1:PartitionID kid, 2:SeriesID sid),
	Status dataStatus(1:PartitionID kid),
	oneway void kill(1:JobID jid)
}