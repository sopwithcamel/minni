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
	class Other {
	public:
		string tag;
		uint64_t value;
		Other(string t, uint64_t val) {
			tag = t;
			value = val;
		}
		~Other() {};
	};
	vector<TimeStamp> timelog;
	vector<Other> otherlog;
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
	void addLogValue(string str, uint64_t val)
	{
		Other* ot;
		ot = new Other(str, val);
		otherlog.push_back(*ot);
	}
	void dumpLog() {
		ofstream of;
		of.open("/localfs/hamur/timelog");
		vector<TimeStamp>::iterator it;
		vector<Other>::iterator otit;
		for (it = timelog.begin(); it != timelog.end(); it++)
			of << it->tag << ": " << it->tim << endl;
		for (otit = otherlog.begin(); otit != otherlog.end(); otit++)
			of << otit->tag << ": " << otit->value << endl;
		of.close();
	}
};

#endif
