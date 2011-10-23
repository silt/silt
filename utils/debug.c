/*
 * Rather boring.  Define the debugging stuff.
 */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
//for time
#include <time.h>
#include <sys/time.h>

#include "debug.h"


unsigned int debug = 0;

/* We could autogenerate this if we felt like it */

struct debug_def {
    int debug_val;
    char *debug_def;
};

static
struct debug_def debugs[] = {
#include "debug-text.h"
    { 0, NULL } /* End of list marker */
};

int set_debug(char *arg)
{
    int i;
    if (!arg || arg[0] == '\0') {
	return -1;
    }

    if (arg[0] == '?' || !strcmp(arg, "list")) {
	fprintf(stderr,
		"Debug values and definitions\n"
		"----------------------------\n");
	for (i = 0;  debugs[i].debug_def != NULL; i++) {
	    fprintf(stderr, "%5d  %s\n", debugs[i].debug_val,
		    debugs[i].debug_def);
	}
        fprintf(stderr, "\n\'all\' will enable all debug flags.\n");
	return -1;
    }
    if (!strcmp(arg, "all")) {
        debug = 0xffffffff;
        return 0;
    }

    if (isdigit(arg[0])) {
	debug |= atoi(arg);
    }
    return 0;
}

