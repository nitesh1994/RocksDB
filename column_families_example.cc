// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <cstdio>
#include <string>
#include <vector>

#include <iostream>
#include <time.h>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

using namespace rocksdb;
using namespace std;

std::string kDBPath1 = "/tmp/rocksdb_column_families_example1";
std::string kDBPath = "/tmp/rocksdb_column_families_example";
std::string kDBPath11 = "/tmp/rocksdb_column_families_example11";
std::string kDBPath2 = "/tmp/rocksdb_column_families_example2";
std::string kDBPath22 = "/tmp/rocksdb_column_families_example22";
std::string kDBPath3 = "/tmp/rocksdb_column_families_example3";
std::string kDBPath33 = "/tmp/rocksdb_column_families_example33";

int main() {
  // open DB
  Options options;
  options.create_if_missing = true;
  DB* db;
  DB* db11;
  DB* db2;
  DB* db22;
  DB* db3;
  DB* db33;
  DB* db1;
  Status s = DB::Open(options, kDBPath1, &db1);
  assert(s.ok());

  //Status s1 = DB::Open(options, kDBPath11, &db11);
  //assert(s1.ok());
  options.manual_wal_flush = true;
  //options.no_block_cache = true;
  options.use_direct_reads = true;
  //options.prefix_extractor.reset(NewFixedPrefixTransform(56));
  // create column family
  ColumnFamilyHandle* cf;
  ColumnFamilyOptions column_family_options;
  column_family_options.write_buffer_size=1;
  //options.writable_file_max_buffer_size   /// Important
  s = db1->CreateColumnFamily(ColumnFamilyOptions(column_family_options), "new_cf", &cf);
  assert(s.ok());

  // close DB
  delete cf;
  delete db1;
  //delete db11;

  // This part not required
  BlockBasedTableOptions bbt_opts;
  bbt_opts.block_size = 32 * 1024;
  bbt_opts.block_cache= NewLRUCache(1 * 1024 * 1024 * 1024LL);
  bbt_opts.no_block_cache = true;
  // open DB with two column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(ColumnFamilyDescriptor(
      kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new one, too
  column_families.push_back(ColumnFamilyDescriptor(
      "new_cf", ColumnFamilyOptions(column_family_options)));
  column_families[1].options.table_factory.reset(NewBlockBasedTableFactory(bbt_opts));
  column_families[0].options.table_factory.reset(NewBlockBasedTableFactory(bbt_opts));
  std::vector<ColumnFamilyHandle*> handles;

  s = DB::Open(Options(options, column_families[0].options), kDBPath1, column_families, &handles, &db1);
  //s = DB::Open(Options(options), kDBPath1, column_families, &handles, &db1);
  assert(s.ok());

  //s = DB::Open(Options(options), kDBPath11, column_families, &handles, &db11);
  //assert(s.ok());
  // put and get from non-default column family
  s = db1->Put(WriteOptions(), handles[1], Slice("key"), Slice("value"));
  assert(s.ok());
  //options.prefix_extractor.reset(NewFixedPrefixTransform(10));
   int i=4;
  /*while(i<1000)
  {
  std::string sr="key";
  sr.append(std::to_string(i));
  std::string vr="value";
  vr.append(std::to_string(i));
  s = db->Put(WriteOptions(), handles[1], Slice(sr), Slice(vr));
  assert(s.ok());
  ++i; 
  }*/
  /*std::string sr="IDX_REC:*-LINEITEM:ORDERKEY-PARTKEY-SUPPKEY- LINENUMBER:00000000000000000035-0000000450-0000002951-00000000000000000001";
  sr.append(std::to_string(i));
  std::string sr4;
  while(i<100000)
  {
	  std:: string sr2="IDX_REC:*-LINEITEM:ORDERKEY-PARTKEY-SUPPKEY- LINENUMBER:00000000000000000035-0000000450-0000002951-00000000000000000001";
  	sr2.append(std::to_string(i));
	if(i==60000)
		sr4=sr2;
  	std::string vr="idx_rec_entry.fb_num=1; idx_rec_entry.row_num=3; idx_rec_entry.rid=39";
  	vr.append(std::to_string(i));
  	s = db1->Put(WriteOptions(), handles[1], Slice(sr2), Slice(vr));
  	assert(s.ok());
  	++i; 
  }*/
  
  std::string sr="IDX001:00000000000000000035-0000000450-0000002951-00000000000000000001";
  std::string sr4;
  sr.append(std::to_string(i));
  while(i<100000)
  {
	  std:: string sr2="IDX001:00000000000000000035-0000000450-0000002951-00000000000000000001";
  	sr2.append(std::to_string(i));
	if(i==60000)
		sr4=sr2;
  	std::string vr="idx_rec_entry.fb_num=1; idx_rec_entry.row_num=3; idx_rec_entry.rid=39";
  	vr.append(std::to_string(i));
  	s = db1->Put(WriteOptions(), handles[1], Slice(sr2), Slice(vr));
  	assert(s.ok());
  	++i; 
  }
  /*auto iter = db->NewIterator(ReadOptions());
  int o=1;
  //for (iter->Seek(key); iter->Valid(); iter->Next())
  for(iter->Seek("IDX_REC:*-LINEITEM:ORDERKEY-PARTKEY-SUPPKEY- LINENUMBER:00000000000000000035-0000000450-0000002951-00000000000000000001");iter->Valid(); iter->Next()) // Seek inside prefix "foo"
  {
	  //iter->Next(); // Find next key-value pair inside prefix "foo"
  	cout<<iter->value();
	++o;
	if(o==3)
		break;
  }*/

  cout<<"Size is"<<sizeof(sr);
  db1->Flush(FlushOptions(), handles[1]);
  std::string str;

  //Uncomment later
  for(int j=0; j <5; ++j)
  {
	  std::string value;
  const clock_t begin = clock(); 
  s = db1->Get(ReadOptions(), handles[1], Slice(sr4), &value);
  assert(s.ok());

  //Uncomment later
  cout<< "Time taken is:"<<float(clock() - begin)  / CLOCKS_PER_SEC<<'\n';
  cout<<"Value found is"<<value<<'\n';
  cout<<"Key was"<<sr<<'\n';
  }
  // atomic write
  /*WriteBatch batch;
  batch.Put(handles[0], Slice("key2"), Slice("value2"));
  batch.Put(handles[1], Slice("key3"), Slice("value3"));
  batch.Delete(handles[0], Slice("key"));
  s = db->Write(WriteOptions(), &batch);
  assert(s.ok());*/

  // drop column family
  s = db1->DropColumnFamily(handles[1]);
  assert(s.ok());

  // close db
  for (auto handle : handles) {
    delete handle;
  }
  delete db1;

  return 0;
}
