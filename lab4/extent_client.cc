// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

#ifndef USE_EXTENT_CACHE  

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0)
  {
    //printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::create, type, id);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::get, eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  ret = cl->call(extent_protocol::put, eid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
}

#else

int extent_client::last_port = 1;

extent_client::extent_client(std::string dst)
{
  //bind
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() < 0)
  {
    //printf("lock_client: call bind\n");
  }
  
  srand(time(NULL)^last_port);
  rextent_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rextent_port;
  id = host.str();
  last_port = rextent_port;
  rpcs *resrpc = new rpcs(rextent_port);
  resrpc->reg(extent_protocol::pull, this, &extent_client::pull_handler);
  resrpc->reg(extent_protocol::push, this, &extent_client::push_handler);

  // root
	filemap[1] = new fileinfo();
	filemap[1]->attr.type = extent_protocol::T_DIR;
}

extent_client::~extent_client()
{
	for(std::map<extent_protocol::extentid_t, fileinfo *>::iterator it = filemap.begin(); it != filemap.end(); it ++)
		delete it->second;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &eid)
{
  //printf("extent_client:create\n");
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::create, type, id, eid);
	filemap[eid] = new fileinfo();
	filemap[eid]->attr.type = type;
	filemap[eid]->attr_valid = true;
	filemap[eid]->buf_valid = true;

  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  //printf("extent_client:get\n");
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  if(filemap[eid] == NULL){
    filemap[eid] = new fileinfo();
  }

  fileinfo *info = filemap[eid];
  if(info->buf_valid){
    buf.assign(info->buf);
  }
  else{
    ret = cl->call(extent_protocol::get, eid, id, buf);
		info->buf.assign(buf);
		info->buf_valid = true;
  }

  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr)
{
  //printf("extent_client:getattr\n");
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here

  if (filemap[eid] == NULL)
    {filemap[eid] = new fileinfo();}  
    fileinfo *info = filemap[eid];
  if(info->attr_valid){
    attr = info->attr;
  }
  else{
    ret = cl->call(extent_protocol::getattr, eid, id, attr);
		info->attr = attr;
		info->attr_valid = true;
  }
  
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  //printf("extent_client:put %llu\n",eid);
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  if(filemap[eid] == NULL){
    filemap[eid] = new fileinfo();
  }
  //Check
  fileinfo *info = filemap[eid];
  if (!info->writable)
  {
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
    info->writable = true;
  }
  info->attr.atime = time(NULL);
  info->attr.mtime = time(NULL);
  info->attr.ctime = time(NULL);
  info->attr.size = buf.size();
  info->buf.assign(buf);
  info->attr_valid = true;
  info->buf_valid = true;

  //printf("extent_client:put end\n");
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  //printf("extent_client:remove\n");
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  ret = cl->call(extent_protocol::remove, eid, id, r);
  filemap.erase(filemap.find(eid));
  return ret;
}

extent_protocol::status extent_client::pull_handler(extent_protocol::extentid_t eid, int &)
{
  extent_protocol::status ret = extent_protocol::OK;
  filemap[eid]->attr_valid = false;
	filemap[eid]->buf_valid = false;
	filemap[eid]->writable = false;
  return ret;
}

extent_protocol::status extent_client::push_handler(extent_protocol::extentid_t eid, int &)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;

	ret = cl->call(extent_protocol::put, eid, id, filemap[eid]->buf, r);
  
	if(filemap[eid]){
		filemap[eid]->attr.atime = time(NULL);
		filemap[eid]->attr.mtime = time(NULL);
		filemap[eid]->attr.ctime = time(NULL);
		filemap[eid]->attr_valid = true;
		filemap[eid]->buf_valid = true;
	}
  
  return ret;
}

#endif