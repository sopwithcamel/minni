#ifndef LIB_RESOURCE_H
#define LIB_RESOURCE_H

/* Abstract class for functions related to scaling resources. Any data
 * structure that claims to be scaling-enabled should inherit this */

class ElasticObject {
    /* Call these to change the memory consumption of the object dynamically.
     * These return false if the respective lower and upper limits are hit 
     *  and true otherwise.  
     */
    virtual bool increaseMemory() = 0;
    virtual bool reduceMemory() = 0;
};

#endif
