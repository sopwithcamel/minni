#ifndef LIB_COMMONUTIL_H
#define LIB_COMMONUTIL_H

#include <libconfig.h++>

using namespace libconfig;

extern Setting& readConfigFile(const Config &cfg, const char* set_name);
extern bool openConfigFile(Config &cfg);

#endif
