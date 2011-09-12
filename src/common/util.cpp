#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include "util.h"
#include "Defs.h"

#define CONFIG_FILE	"/home/hamur/code/minni/sample.cfg"

Setting& readConfigFile(const Config &cfg, const char* set_name)
{
	try {
		if (cfg.exists(set_name))
			return cfg.lookup(set_name);
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
		exit(1);
	}
}

bool openConfigFile(Config &cfg)
{
	try {
		cfg.readFile(CONFIG_FILE);
	}
	catch (FileIOException e) {
		fprintf(stderr, "Error reading config file \n");
		exit(1);
	}	
	catch (ParseException e) {
		fprintf(stderr, "Error reading config file: %s at %s:%d\n", e.getError(), e.what(), e.getLine());
		exit(1);
	}
	return true;
}