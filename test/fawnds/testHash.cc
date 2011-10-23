#include <cstring>
#include <iostream>
#include "fawnds.h"
#include "hash_functions.h"




using namespace std;
using namespace fawn;

int main() {
  const char* dbname = "fawn_db";
  u_int64_t num_records = 10;
  double max_deleted_ratio = .9;
  double max_load_factor = .8;
  
  FawnDS *h;

  cout << "Creating FawnDS" << endl;
  h = fawn::FawnDS::Create_FawnDS(dbname, num_records, 
				  max_deleted_ratio, 
				  max_load_factor);

  cout << "Opening FawnDS" << endl;
  h = fawn::FawnDS::Open_FawnDS(dbname);


  const char* key = "key0";
  const char* data = "value0";
  bool irv;

  cout << "Inserting " << key << "," << data << " into " << dbname << endl;

  irv = h->Insert(key, data, 8);

  char** data_returned = (char**) malloc(sizeof(char*));
  u_int64_t* length = (u_int64_t*) malloc(sizeof(u_int64_t));
  bool grv;
     
  printf("Get value from database\n");
  grv = h->Get(key, data_returned, length);

  if (key != NULL && data != NULL && length != NULL) {
    cout << "Key: " << key << " data: " << *data_returned 
	 << " length: " << *length << endl;
  }


  return 0;
}
