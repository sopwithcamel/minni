#ifndef LIB_COMMONUTIL_H
#define LIB_COMMONUTIL_H

#include <iostream>
#include <fstream>
#include <tbb/mutex.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <libconfig.h++>

extern libconfig::Setting& readConfigFile(const libconfig::Config &cfg, const char* set_name);
extern bool openConfigFile(libconfig::Config &cfg);
extern bool configExists(const libconfig::Config &cfg, const char* set_name);

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
    static tbb::mutex mutex_;
	static void addTimeStamp(int jobid, std::string str)
	{
        mutex_.lock();
        TimeStamp* ts;
        time_t ltime = time(NULL);
        std::stringstream write_str;
        write_str << jobid << ": " << str;
        ts = new TimeStamp(write_str.str(), ltime);
        timelog.push_back(*ts);		
        mutex_.unlock();
	}
		
	static void dumpLog() {
		std::ofstream of;
		of.open("/localfs/hamur/timelog", std::ofstream::app);
		for (std::vector<TimeStamp>::iterator it = timelog.begin(); 
				it != timelog.end(); it++) {
			of << it->tag << ": " << it->tim << std::endl;
		}
		timelog.clear();		
		of.close();
	}
};

#endif
