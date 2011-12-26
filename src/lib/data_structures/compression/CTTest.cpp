#include <stdlib.h>
#include <string.h>

#include "CompressTree.h"

int main()
{
    compresstree::CompressTree* ct = new compresstree::CompressTree(2, 4);
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (int i=0; i<1000; i++) {
        ct->insert(i, buf, strlen(buf));
    }
}
