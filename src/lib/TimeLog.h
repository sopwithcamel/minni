#ifndef TimeStamp_H
#define TimeStamp_H
#include <iostream>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>

using namespace std;

class TimeLog {
	class TimeStamp {
	public:
		string tag;
		time_t tim;
		TimeStamp(string t, time_t s) {
			tag = t;
			tim = s;
		}
		~TimeStamp() {};
	};
	vector<TimeStamp> timelog;
public:
	TimeLog() {};
	~TimeLog() {};
	void addTimeStamp(string str)
	{
		TimeStamp* ts;
		time_t ltime = time(NULL);
		ts = new TimeStamp(str, ltime);
		timelog.push_back(*ts);		
	}
	void dumpLog() {
		ofstream of;
		of.open("/localfs/hamur/timelog");
		vector<TimeStamp>::iterator it;
		for (it = timelog.begin(); it != timelog.end(); it++)
			of << it->tag << ": " << it->tim << endl;
		of.close();
	}
};

#endif
