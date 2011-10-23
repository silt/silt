#ifndef _PREPROCESSTRACE_H_
#define _PREPROCESSTRACE_H_

#define MAXLEN 1024
enum QUERY_TYPE{
    PUT=0,
    GET,
    DELETE,
};

struct Query{
    char hashed_key[20];
    char tp;
} __attribute__((__packed__)) ;

#endif /* _PREPROCESSTRACE_H_ */
