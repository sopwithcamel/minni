#ifndef LIB_RESOURCE_H
#define LIB_RESOURCE_H

/* Abstract class for functions related to scaling resources. Any data
 * structure that claims to be scaling-enabled should inherit this */

class ElasticHashtable {
    /* Call these to change the memory consumption of the object dynamically.
     * These return false if the respective lower and upper limits are hit 
     *  and true otherwise.  
     */
    virtual bool increaseSize() = 0;
    virtual bool reduceSize() = 0;
};

#endif
