// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// test functions -------------------------------------
#include <x86intrin.h>
#include <stdio.h>
#include <stdlib.h>

#define TEST
#define TEST_NUM 20
typedef unsigned long long cycles_t;

unsigned int call_count[TEST_NUM] = {0};
cycles_t time_count[TEST_NUM] = {0};
cycles_t max_count[TEST_NUM] = {0};
cycles_t min_count[TEST_NUM] = {0};
FILE *out;

inline cycles_t currentcycles()
{
  return _rdtsc();
}

inline void adjustTime(int funId, cycles_t t1)
{
  cycles_t t2 = currentcycles();
  cycles_t t = t2 - t1;
  if (time_count[funId] == 0)
  {
    time_count[funId] = t;
    max_count[funId] = t;
    min_count[funId] = t;
  }
  else
  {
    time_count[funId] = 1 / (call_count[funId] + 1.0) * t + call_count[funId] / (call_count[funId] + 1.0) * time_count[funId];
    max_count[funId] = max_count[funId] > t ? max_count[funId] : t;
    min_count[funId] = min_count[funId] < t ? min_count[funId] : t;
  }
  call_count[funId]++;
  out = fopen("test.log", "a");
  fprintf(out, "record%d: count: %u, max: %llu, min: %llu, mean: %llu \n",
          funId, call_count[funId], max_count[funId], min_count[funId], time_count[funId]);
  fclose(out);
}

extent_client::extent_client()
{ 
  //cycles_t t1 = currentcycles();
  es = new extent_server();
  //adjustTime(1,t1);
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  //cycles_t t1 = currentcycles();
  extent_protocol::status ret = extent_protocol::OK;
  ret = es->create(type, id);
  //adjustTime(2,t1);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  //cycles_t t1 = currentcycles();
  extent_protocol::status ret = extent_protocol::OK;
  //attr_cache.erase(eid);
  std::map<extent_protocol::extentid_t,std::string>::iterator iter = buf_cache.find(eid);
  if(iter != buf_cache.end())
  {
    buf = iter->second;
  }
  else{
    ret = es->get(eid, buf);
    buf_cache[eid] = buf;
  }
  //adjustTime(3,t1);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  //cycles_t t1 = currentcycles();

  extent_protocol::status ret = extent_protocol::OK;
  std::map<extent_protocol::extentid_t,extent_protocol::attr>::iterator iter = attr_cache.find(eid);
  if(iter != attr_cache.end())
  {
    attr = iter->second;
  }
  else{
    ret = es->getattr(eid, attr);
    attr_cache[eid] = attr;
  }

  // extent_protocol::status ret = extent_protocol::OK;
  // ret = es->getattr(eid, attr);

  //adjustTime(4,t1);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  //cycles_t t1 = currentcycles();
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  attr_cache.erase(eid);
  buf_cache[eid] = buf;
  ret = es->put(eid, buf, r);
  //adjustTime(5,t1);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  //cycles_t t1 = currentcycles();
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  attr_cache.erase(eid);
  buf_cache.erase(eid);
  ret = es->remove(eid, r);
  //adjustTime(6,t1);
  return ret;
}


