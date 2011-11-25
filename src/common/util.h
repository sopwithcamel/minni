#ifndef LIB_COMMONUTIL_H
#define LIB_COMMONUTIL_H

#include <iostream>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <libconfig.h++>

using std::string;
using std::vector;
using std::ofstream;
using namespace libconfig;

extern Setting& readConfigFile(const Config &cfg, const char* set_name);
extern bool openConfigFile(Config &cfg);
extern bool configExists(const Config &cfg, const char* set_name);

class TimeStamp {
public:
	std::string tag;
	time_t tim;
	TimeStamp(std::string t, time_t s) {
		tag = t;
		tim = s;
	}
	~TimeStamp() {};
};

class TimeLog {
private:
	TimeLog() {};
	~TimeLog() {};
public:
	static std::vector<TimeStamp> timelog;
	static void addTimeStamp(std::string str)
	{
		TimeStamp* ts;
		time_t ltime = time(NULL);
		ts = new TimeStamp(str, ltime);
		timelog.push_back(*ts);		
	}
		
	static void dumpLog() {
		std::ofstream of;
		of.open("/localfs/hamur/timelog", ofstream::app);
		for (vector<TimeStamp>::iterator it = timelog.begin(); 
				it != timelog.end(); it++) {
			of << it->tag << ": " << it->tim << std::endl;
		}
		timelog.clear();		
		of.close();
	}
};

#endif
