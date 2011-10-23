#ifndef _DEBUG_H_
#define _DEBUG_H_

#include <stdio.h>  /* for perror */
#define eprintf(fmt, args...) fprintf(stderr, fmt, ##args)

#ifndef DEBUG
//#define DEBUG
#endif

#ifdef DEBUG
//extern unsigned int debug;

#define debug_level (1 | 2)

#define DPRINTF(level, fmt, args...) \
        do { if (debug_level & (level)) fprintf(stderr, fmt , ##args ); } while(0)
#define DEBUG_PERROR(errmsg) \
        do { if (debug_level & DEBUG_ERRS) perror(errmsg); } while(0)
#else
#define DPRINTF(args...)
#define DEBUG_PERROR(args...)
#endif

/*
 * The format of this should be obvious.  Please add some explanatory
 * text if you add a debugging value.  This text will show up in
 * -d list
 */
#define DEBUG_NONE      0x00	// DBTEXT:  No debugging
#define DEBUG_ERRS      0x01	// DBTEXT:  Verbose error reporting
#define DEBUG_FLOW      0x02	// DBTEXT:  Messages to understand flow
#define DEBUG_SOCKETS   0x04    // DBTEXT:  Debug socket operations
#define DEBUG_INPUT     0x08    // DBTEXT:  Debug client input
#define DEBUG_CLIENTS   0x10    // DBTEXT:  Debug client arrival/depart
#define DEBUG_COMMANDS  0x20    // DBTEXT:  Debug client commands
#define DEBUG_CHANNELS  0x40    // DBTEXT:  Debug channel operations
#define DEBUG_STATUS    0x80    // DBTEXT:  Debug status messages

#define DEBUG_ALL  0xffffffff

//int set_debug(char *arg);  /* Returns 0 on success, -1 on failure */


#endif /* _DEBUG_H_ */
