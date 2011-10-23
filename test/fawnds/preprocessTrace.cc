#include <cstdlib>
#include <iostream>
#include <openssl/sha.h>
#include <cstdio>
#include <cstring>

#include "preprocessTrace.h"

using namespace std;



static void sha1(char hash[20], const char* buf, size_t count)
{
	SHA1(reinterpret_cast<const unsigned char*>(buf), count, reinterpret_cast<unsigned char*>(hash));
}


int main(int argc, char **argv) {

    if (argc <= 2) {
        cout << "usage: preprocess fieldlength output_filename < input_filename" << endl;
        exit (1);
    }

    //size_t val_len = static_cast<size_t>(-1);
    size_t val_len = atoi(argv[1]);
    FILE *fp;
    fp = fopen(argv[2], "w");

    fwrite(&val_len, sizeof(size_t), 1, fp);

    const size_t tmp_size = 1048576;
	char* tmp = new char[tmp_size];
	
    while (fgets(tmp, tmp_size, stdin)) {
        char key[MAXLEN];
        //char val[MAXLEN]; 
        struct Query q;

        /*
        if (val_len == static_cast<size_t>(-1)) {
            if (sscanf(tmp, "\"fieldlength\"=\"%zu\"", &val_len)) {
                fwrite(&val_len, sizeof(size_t), 1, fp);
            }
        }
        */

        if (sscanf(tmp, "INSERT usertable %s [ field", key)){
            q.tp = PUT;
            sha1(q.hashed_key, key, strlen(key));
            //const char *pos = index(tmp, '=');
            //strncpy(val, pos + 1, val_len);
            //val[val_len] = 0;
        } else if (sscanf(tmp, "UPDATE usertable %s [ field", key)) { 
            q.tp = PUT;
            sha1(q.hashed_key, key, strlen(key));
            //const char *pos = index(tmp, '=');
            //strncpy(val, pos + 1, val_len);
            //val[val_len] = 0;
        } 
        else if (sscanf(tmp, "READ usertable %s [", key)) {
            q.tp = GET;
            sha1(q.hashed_key, key, strlen(key));
        } else 
            continue;

        if (q.tp == PUT) {
            fwrite(&q, sizeof(Query), 1, fp);
            //fwrite(val, sizeof(char), val_len, fp); 
        } else if (q.tp == GET) {
            fwrite(&q, sizeof(Query), 1, fp);
        }


    }
    delete tmp;
    fclose(fp);
}
