namespace cpp workdaemon

typedef i16 Status
typedef i64 JobID
typedef i64 PartID  // Parition ID
typedef i64 ChunkID // This is for external data
typedef i32 BlockID // This is for internal data
typedef i32 Count
typedef string URL
typedef map<string, string> Properties

service WorkDaemon {
	oneway void bark(1:string s), // Debug
	string stateString(),         // Debug
	oneway void kill();	      // Debug
	map<JobID, Status> listStatus(),   // Are you alive
	oneway void startMapper(1:JobID jid, 2:Properties prop),
	oneway void startReducer(1:JobID jid, 2:Properties prop),
	binary sendData(1:PartID kid, 2:BlockID bid),
	Status mapperStatus(),
	Count blockCount(1:PartID pid),
	oneway void reportCompletedJobs(1:list<URL> done),
	oneway void allMapsDone()
}