#include <iostream>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <vector>
#include "tbb/task.h"
#include "tbb/tbb_thread.h"
#include <unistd.h>
#include <sstream>
#include <fstream>
#include "uthash.h"

using namespace std;

/* Configuration */
long UNIQ_KEYS;
long LEN;
long REPEAT_FACTOR;
/* Config ends */

#define CONV_26(x)      x * log(2)/log(26)

char** fillers;
long NUM_FILLERS = 1000000;

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

int all_zero(int arr[])
{
        int ret = 0;
        for (int i = 0; i < LEN; i++)
                ret = ret | arr[i];
        return ret;
}

void gen_fillers(int fillen)
{
    int i;
    fillers = (char**)malloc(sizeof(char*) * NUM_FILLERS);
    for (i=0; i<NUM_FILLERS; i++) {
        fillers[i] = (char*)malloc(fillen + 1);
        for (int j=0; j<fillen; j++)
            fillers[i][j] = 97 + rand() % 26;
    }
}

int main(int argc, char* argv[])
{
        if (argc < 4) {
                cout << "Insufficient arguments. Exiting..." << endl;
                exit(1);
        }

        UNIQ_KEYS = atoi(argv[1]);
        REPEAT_FACTOR = atoi(argv[2]);
        LEN = atoi(argv[3]);

        FILE* f = fopen("/mnt/hamur/randinput.txt", "w");
        int i = 0, k = 0;
        char* word = (char*)malloc(LEN + 1);
        char* filler = (char*)malloc(LEN + 1);

        int* loop_vars = (int*)malloc(sizeof(int) * LEN);

        if (pow(26, LEN) < UNIQ_KEYS) {
                cout << "Increase length to " << ceil(CONV_26(log2(UNIQ_KEYS))) << endl;
                exit(1);
        }

        int num_full_loops = (int)floor(CONV_26(log2(UNIQ_KEYS)));
        int part_loop = (int)ceil(UNIQ_KEYS / pow(26, num_full_loops));
        cout << num_full_loops << ", " << part_loop << endl;

        gen_fillers(LEN - num_full_loops);

        i=0;
        long bkt;
        uint64_t hashv;

        word[LEN] = '\n';
        while (i < REPEAT_FACTOR) {
                for (k = 0; k < LEN; k++)
                        loop_vars[k] = 0;
                do {
                        for (k = num_full_loops; k >= 0; k--) {
                                int lim;
                                if (k < num_full_loops)
                                        lim = 26;
                                else
                                        lim = part_loop;
                                if (++loop_vars[k] == lim)
                                        loop_vars[k] = 0;
                                else
                                        break;
                        }
                        for (int j=0; j < num_full_loops; j++)
                                word[j] = 97 + rand() % 26;
                        word[num_full_loops] = 97 + rand() % part_loop;
                        word[num_full_loops + 1] = '\0';

                        Hash(word, strlen(word), UNIQ_KEYS, hashv, bkt);
                        uint64_t filler_number = hashv % NUM_FILLERS;

                        for (int j=num_full_loops+1; j < LEN; j++)
                                word[j] = fillers[filler_number][j-num_full_loops-1];
                        fwrite(word, sizeof(char), LEN + 1, f);
                } while (all_zero(loop_vars));
                i++;
        }
        fclose(f);
        free(word);
        free(loop_vars);
}
                 
