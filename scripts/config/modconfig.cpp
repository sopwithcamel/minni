#include <assert.h>
#include <iostream>
#include <iomanip>
#include <cstdlib>
#include <stdint.h>
#include <libconfig.h++>
#include <gflags/gflags.h>

using namespace std;
using namespace libconfig;

DEFINE_uint64(minni__tbb__token_size, 0, "TBB token size");
DEFINE_uint64(minni__tbb__max_keys_per_token, 0, "TBB max keys per token");


bool openConfigFile(Config &cfg, const char* file_name)
{
	try {
		cfg.readFile(file_name);
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

bool writeNewConfigFile(Config &cfg, const char* file_name)
{
    // Write out the updated configuration.
    try
    {
        cfg.writeFile(file_name);
        cerr << "Updated configuration successfully written to: " << file_name
            << endl;

    }
    catch(const FileIOException &fioex)
    {
        cerr << "I/O error while writing file: " << file_name << endl;
        return(EXIT_FAILURE);
    }
	return true;
}

int main(int argc, char **argv)
{
    // Define gflags options
    google::ParseCommandLineFlags(&argc, &argv, true);

    // libconfig++ stuff
    Config cfg;
    assert(openConfigFile(cfg, "../../default.cfg"));

    /* Setting values in config file depending on command line flags */
    if (FLAGS_minni__tbb__token_size) {
        Setting& c_token_size = cfg.lookup("minni.tbb.token_size");
        c_token_size = (int)FLAGS_minni__tbb__token_size;
    }

    if (FLAGS_minni__tbb__max_keys_per_token) {
        Setting& c_max_keys = cfg.lookup("minni.tbb.max_keys_per_token");
        c_max_keys = (int)FLAGS_minni__tbb__max_keys_per_token;
    }

    static const char *output_file = "../../sample.cfg";
    assert(writeNewConfigFile(cfg, output_file));
}
